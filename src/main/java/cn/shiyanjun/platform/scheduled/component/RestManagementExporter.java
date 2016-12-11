package cn.shiyanjun.platform.scheduled.component;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.scheduled.common.JobQueueingService;
import cn.shiyanjun.platform.scheduled.common.GlobalResourceManager;
import cn.shiyanjun.platform.scheduled.common.RestManageable;
import cn.shiyanjun.platform.scheduled.component.DefaultQueueingManager.QueueingContext;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;

public class RestManagementExporter implements RestManageable {

	private final GlobalResourceManager manager;
	
	public RestManagementExporter(GlobalResourceManager manager) {
		super();
		this.manager = manager;
	}
	
	private JobQueueingService getQueueingService(String queue) {
		QueueingContext qc = manager.getQueueingManager().getQueueingContext(queue);
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
		for(String job : jobs) {
			JSONObject detail = JSONObject.parseObject(job);
			statuses.put(detail.getIntValue(ScheduledConstants.JOB_ID), detail);
		}
		return statuses;
	}

	@Override
	public Map<String, JSONObject> getQueueStatuses() {
		Map<String, JSONObject> statuses = Maps.newHashMap();
		Set<String> queues = manager.getQueueingManager().queueNames();
		for(String queue : queues) {
			Map<Integer, JSONObject> jobs = getQueuedJobStatuses(queue);
			JSONObject status = new JSONObject();
			status.put(queue, jobs.size());
			statuses.put(queue, status);
		}
		return statuses;
	}

}
