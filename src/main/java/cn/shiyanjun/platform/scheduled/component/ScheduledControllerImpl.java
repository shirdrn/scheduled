package cn.shiyanjun.platform.scheduled.component;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;

import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.ScheduledController;
import cn.shiyanjun.platform.scheduled.api.StateManager;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;

public class ScheduledControllerImpl implements ScheduledController {

	private static final Log LOG = LogFactory.getLog(ScheduledControllerImpl.class);
	private final StateManager stateManager;
	private final Set<Integer> cancellingJobs = Sets.newConcurrentHashSet();
	protected volatile boolean isSchedulingOpened = true;
	
	public ScheduledControllerImpl(ComponentManager componentManager) {
		super();
		this.stateManager = componentManager.getStateManager();
	}
	
	@Override
	public boolean cancelJob(int jobId) {
		LOG.info("Prepare to cancel job: jobId=" + jobId);
		cancellingJobs.add(jobId);
		try {
			stateManager.getRunningJob(jobId).ifPresent(ji -> {
				ji.setJobStatus(JobStatus.CANCELLING);
				ji.setLastUpdatedTime(Time.now());
				LOG.info("In-memory job state changed: job=" + ji);
				
				// update Redis job status to CANCELLING
				String queue = ji.getQueue();
				JSONObject job = stateManager.retrieveQueuedJob(queue, jobId);
				job.put(ScheduledConstants.JOB_STATUS, JobStatus.CANCELLING.toString());
				job.put(ScheduledConstants.LAST_UPDATE_TS, Time.now());
				try {
					stateManager.updateQueuedJob(jobId, queue, job);
				} catch (Exception e) {
					LOG.warn("Failed to update queued job: " + job);
				}
			});
			// update DB
			stateManager.updateJobStatus(jobId, JobStatus.CANCELLING);
		} catch (Exception e) {
			LOG.warn("Fail to cancel job: ", e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean cancelJob(int jobId, Runnable action) {
		LOG.info("Prepare to cancel job: jobId=" + jobId);
		action.run();
		return cancellingJobs.add(jobId);
	}

	@Override
	public boolean shouldCancelJob(int jobId) {
		return cancellingJobs.contains(jobId);
	}

	@Override
	public void jobCancelled(int jobId, Runnable action) {
		action.run();
		cancellingJobs.remove(jobId);
		LOG.info("Job cancelled: jobId=" + jobId);
	}
	
	@Override
	public boolean isSchedulingOpened() {
		return isSchedulingOpened;
	}

	@Override
	public void setSchedulingOpened(boolean isSchedulingOpened) {
		this.isSchedulingOpened = isSchedulingOpened;
	}

}
