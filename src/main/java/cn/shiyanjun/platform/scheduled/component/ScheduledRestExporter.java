package cn.shiyanjun.platform.scheduled.component;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.api.utils.Pair;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.JobQueueingService;
import cn.shiyanjun.platform.scheduled.api.RestExporter;
import cn.shiyanjun.platform.scheduled.component.QueueingManagerImpl.QueueingContext;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;

public class ScheduledRestExporter implements RestExporter {

	private final ComponentManager componentManager;
	
	public ScheduledRestExporter(ComponentManager componentManager) {
		super();
		this.componentManager = componentManager;
	}
	
	@Override
	public Set<String> queueingNames() {
		return componentManager.getQueueingManager().queueNames();
	}
	
	private JobQueueingService getQueueingService(String queue) {
		QueueingContext qc = componentManager.getQueueingManager().getQueueingContext(queue);
		JobQueueingService queueingService = qc.getJobQueueingService();
		return queueingService;
	}

	@Override
	public Collection<String> getWaitingJobs(String queue, String jobId) {
		JobQueueingService queueingService = getQueueingService(queue);
		Set<String> jobs = queueingService.getWaitingJobsBefore(jobId);
		Iterator<String> iter = jobs.iterator();
		// remove non-QUEUEING statuses' jobs
		while(iter.hasNext()) {
			JSONObject job = JSONObject.parseObject(iter.next());
			String jobStatus = job.getString(ScheduledConstants.JOB_STATUS);
			if(!jobStatus.equals(JobStatus.QUEUEING.toString())) {
				iter.remove();
			}
		}
		return jobs == null ? Sets.newHashSet() : jobs ;
	}

	@Override
	public void prioritize(String queue, int jobId) {
		JobQueueingService queueingService = getQueueingService(queue);
		queueingService.prioritize(jobId);
	}

	@Override
	public Map<Integer, JSONObject> getQueuedJobStatuses(String queue) {
		 Map<Integer, JSONObject> statuses = Maps.newHashMap();
		JobQueueingService queueingService = getQueueingService(queue);
		Set<String> jobs = queueingService.getJobs();
		jobs.forEach(job -> {
			JSONObject detail = JSONObject.parseObject(job);
			statuses.put(detail.getIntValue(ScheduledConstants.JOB_ID), detail);
		});
		return statuses;
	}

	@Override
	public Map<String, JSONObject> getQueueStatuses() {
		Map<String, JSONObject> statuses = Maps.newHashMap();
		Set<String> queues = componentManager.getQueueingManager().queueNames();
		queues.forEach(queue -> {
			Map<Integer, JSONObject> jobs = getQueuedJobStatuses(queue);
			JSONObject status = new JSONObject();
			status.put(queue, jobs.size());
			statuses.put(queue, status);
		});
		return statuses;
	}
	
	@Override
	public boolean cancelJob(int jobId) {
		try {
			return componentManager.cancelJob(jobId);
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public void updateResourceAmount(String queue, TaskType taskType, int amount) {
		componentManager.getResourceManager().updateResourceAmount(queue, taskType, amount);		
	}

	@Override
	public void setSchedulingOpened(boolean isSchedulingOpened) {
		componentManager.setSchedulingOpened(isSchedulingOpened);;
	}

	@Override
	public boolean isSchedulingOpened() {
		return componentManager.isSchedulingOpened();
	}

	@Override
	public Pair<String, String> queryMaintenanceTimeSegment() {
		return componentManager.getJobFetcher().getMaintenanceTimeSegment();
	}

	@Override
	public void updateMaintenanceTimeSegment(String startTime, String endTime) {
		componentManager.getJobFetcher().updateMaintenanceTimeSegment(startTime, endTime);		
	}

}
