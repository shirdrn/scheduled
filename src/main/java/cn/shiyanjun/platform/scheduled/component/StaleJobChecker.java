package cn.shiyanjun.platform.scheduled.component;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.api.StateManager;
import cn.shiyanjun.platform.scheduled.common.JobInfo;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
import cn.shiyanjun.platform.scheduled.dao.entities.Job;
import cn.shiyanjun.platform.scheduled.dao.entities.Task;

/**
 * Check jobs whose status are deem to be not updated for a specified time,
 * and decide to clear the tasks' in-redis or in-memory statuses.
 * 
 * @author yanjun
 */
class StaleJobChecker implements Runnable {
	
	private static final Log LOG = LogFactory.getLog(StaleJobChecker.class);
	private final SchedulingManagerImpl sched;
	private final StateManager stateManager;
	private final int keptTimeoutJobMaxCount;
	private final int staleJobMaxThresholdSecs;
	private final long staleJobMaxThresholdMillis;
	
	public StaleJobChecker(final SchedulingManagerImpl schedulingManager) {
		this.sched = schedulingManager;
		this.stateManager = schedulingManager.getComponentManager().getStateManager();
		keptTimeoutJobMaxCount = schedulingManager.componentManager.getContext().getInt(ConfigKeys.SCHEDULED_KEPT_TIMEOUT_JOB_MAX_COUNT, 100);
		// default 2 hours
		staleJobMaxThresholdSecs = schedulingManager.componentManager.getContext().getInt(ConfigKeys.SCHEDULED_STALE_JOB_MAX_THRESHOLD_SECS, 7200);
		staleJobMaxThresholdMillis = staleJobMaxThresholdSecs * 1000;
	}
	
	@Override
	public void run() {
		LOG.debug("Start to check stale jobs...");
		try {
			// check stale job/tasks in-memory
			checkInMemStaleJobs();
			
			// check stale job/tasks in-db/in-redis
			checkJobStatuses();
		} catch (Exception e) {
			LOG.warn("Fail to check stale jobs: ", e);
		} finally {
			LOG.debug("Complete to check stale jobs.");
		}
	}
	
	void checkJobStatuses() {
		// check stale job/tasks in DB, and update status for these job/tasks
		checkInDBStaleJobs();
		
		// purge stale jobs in Redis queue
		checkInRedisStaleJobs();
	}
	
	private void checkInDBStaleJobs() {
		// check a in-db stale job with status in (FETCHED, SCHEDULED, RUNNING)
		Arrays.asList(new JobStatus[]{JobStatus.FETCHED, JobStatus.SCHEDULED, JobStatus.RUNNING})
			.forEach(status -> processInDBStaleJobs(stateManager.retrieveJobs(status)));
	}

	private void processInDBStaleJobs(List<Job> jobs) {
		jobs.forEach(job -> {
			int jobId = job.getId();
			long now = Time.now();
			// check whether a job is timeout
			if(now - job.getDoneTime().getTime() > staleJobMaxThresholdMillis) {
				stateManager.updateJobStatus(job.getId(), JobStatus.TIMEOUT);
				LOG.info("Stale in-db job: jobId=" + jobId + 
						", lastUpdatedTs=" + job.getDoneTime() 
						+ ", current=" + new Timestamp(now));
				List<Task> tasks = stateManager.retrieveTasks(jobId);
				stateManager.getRunningJob(jobId).ifPresent(jobInfo -> {
					try {
						// in memory job timeout, fail it fast
						jobInfo.lock();
						jobInfo.setJobStatus(JobStatus.TIMEOUT);
					} finally {
						jobInfo.unlock();
					}
					String queue = jobInfo.getQueue();
					tasks.forEach(task -> {
						if(task.getStatus() == TaskStatus.RUNNING.getCode()) {
							stateManager.getRunningTasks(jobId).forEach(ti -> {
								if(ti != null && ti.getTaskStatus() == TaskStatus.RUNNING 
										&& sched.componentManager.getPlatformId().equals(ti.getPlatformId())) {
									sched.releaseResource(queue, ti);
									stateManager.updateTaskStatus(ti.getTaskId(), TaskStatus.TIMEOUT);
									sched.incrementTimeoutTaskCount(queue);
									sched.updateJobStatCounter(queue, JobStatus.TIMEOUT);
								}
							});
						}
					});
					
					stateManager.handleInMemoryCompletedJob(jobId);
					stateManager.removeQueuedJob(queue, jobId);
					stateManager.handleInMemoryTimeoutJob(jobInfo, keptTimeoutJobMaxCount);
				});
			}
		});
	}
	
	private void checkInMemStaleJobs() {
		stateManager.getRunningJobs().forEach(jobInfo -> {
			int jobId = jobInfo.getJobId();
			// check timeout jobs (RUNNING || CANCELLED)
			boolean isTimeout = Time.now() - jobInfo.getLastUpdatedTime() > staleJobMaxThresholdMillis;
			if(jobInfo.getJobStatus() != JobStatus.SUCCEEDED 
					&& jobInfo.getJobStatus() != JobStatus.FAILED && isTimeout) {
				LOG.info("Stale in-mem job: jobInfo=" + jobInfo);
				String queue = jobInfo.getQueue();
				final JobStatus targetJobStatus;
				try {
					jobInfo.lock();
					switch(jobInfo.getJobStatus()) {
						case RUNNING:
							targetJobStatus = JobStatus.TIMEOUT;
							break;
						case CANCELLED:
							targetJobStatus = JobStatus.CANCELLED;
							break;
						default:
							targetJobStatus = JobStatus.FAILED;
					}
				} finally {
					jobInfo.unlock();
				}
				
				stateManager.getRunningTasks(jobId).forEach(ti -> {
					if(ti != null && 
							(ti.getTaskStatus() == TaskStatus.RUNNING || targetJobStatus == JobStatus.CANCELLED)
							&& sched.componentManager.getPlatformId().equals(ti.getPlatformId())) {
						sched.releaseResource(queue, ti);
						stateManager.updateTaskStatus(ti.getTaskId(), TaskStatus.TIMEOUT);
						sched.incrementTimeoutTaskCount(queue);
						// clear cancelled job from memory
						if(targetJobStatus == JobStatus.CANCELLED) {
							sched.getComponentManager().jobCancelled(ti.getJobId(), () -> {
								jobInfo.setLastUpdatedTime(Time.now());;
								LOG.info("Cancelled job timeout: jobId=" + jobId + "jobInfo=" + jobInfo);
							});
						}
					}
				});
				stateManager.updateJobStatus(jobId, targetJobStatus);
				
				// handle in-memory completed job
				stateManager.handleInMemoryCompletedJob(jobId);
				stateManager.removeQueuedJob(queue, jobId);
				sched.updateJobStatCounter(queue, targetJobStatus);
			}
		});
	}
	
	private void checkInRedisStaleJobs() {
		stateManager.queueNames().forEach(queue -> {
			stateManager.retrieveQueuedJobs(queue).forEach(job -> {
				int jobId = job.getIntValue(ScheduledConstants.JOB_ID);
				String jStatus = job.getString(ScheduledConstants.JOB_STATUS);
				JobStatus jobStatus = JobStatus.valueOf(jStatus);
				long lastUpdateTs = job.getLongValue(ScheduledConstants.LAST_UPDATE_TS);
				
				Optional<JobInfo> jobInfo = stateManager.getRunningJob(jobId);
				jobInfo.ifPresent(ji -> {
					// clear job with FAILED status from Redis queue
					if(jobStatus == JobStatus.FAILED) {
						stateManager.removeQueuedJob(queue, jobId);
						LOG.warn("Stale job in Redis purged: queue=" + queue + ", job=" + job);
					}
					
					// clear timeout job with RUNNING status && in Redis queue
					long now = Time.now();
					if((jobStatus == JobStatus.RUNNING || jobStatus == JobStatus.CANCELLING)
							&& now - lastUpdateTs > staleJobMaxThresholdMillis) {
						stateManager.removeQueuedJob(queue, jobId);
						LOG.warn("Stale job in Redis purged: queue=" + queue + ", job=" + job);
					}
				});
			});
		});
	}
	
}
