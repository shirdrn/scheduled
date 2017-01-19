package cn.shiyanjun.platform.scheduled.component;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;

import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.component.SchedulingManagerImpl.JobInfo;
import cn.shiyanjun.platform.scheduled.component.SchedulingManagerImpl.TaskID;
import cn.shiyanjun.platform.scheduled.component.SchedulingManagerImpl.TaskInfo;
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
	private final ConcurrentMap<Integer, JobInfo> timeoutJobIdToInfos = Maps.newConcurrentMap();
	private final int keptTimeoutJobMaxCount;
	private final int staleJobMaxThresholdSecs;
	private final long staleJobMaxThresholdMillis;
	
	public StaleJobChecker(final SchedulingManagerImpl schedulingManager) {
		this.sched = schedulingManager;
		keptTimeoutJobMaxCount = schedulingManager.manager.getContext().getInt(ConfigKeys.SCHEDULED_KEPT_TIMEOUT_JOB_MAX_COUNT, 100);
		// default 2 hours
		staleJobMaxThresholdSecs = schedulingManager.manager.getContext().getInt(ConfigKeys.SCHEDULED_STALE_JOB_MAX_THRESHOLD_SECS, 7200);
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
			.forEach(status -> {
				List<Job> jobs = sched.getJobByState(status);
				processInDBStaleJobs(jobs);
		});
	}

	private void processInDBStaleJobs(List<Job> jobs) {
		jobs.forEach(job -> {
			int jobId = job.getId();
			long now = Time.now();
			// check whether a job is timeout
			if(now - job.getDoneTime().getTime() > staleJobMaxThresholdMillis) {
				LOG.info("Stale in-db job: jobId=" + jobId);
				List<Task> tasks = sched.getTasksFor(job.getId());
				JobInfo jobInfo = sched.runningJobIdToInfos.get(jobId);
				if(jobInfo == null) {
					sched.updateJobInfo(job.getId(), JobStatus.TIMEOUT);
				} else {
					String queue = jobInfo.queue;
					tasks.forEach(task -> {
						if(task.getStatus() == TaskStatus.RUNNING.getCode()) {
							LinkedList<TaskID> inMemTasks = sched.runningJobToTaskList.get(jobId);
							if(inMemTasks != null) {
								inMemTasks.forEach(id -> {
									TaskInfo ti = sched.runningTaskIdToInfos.get(id);
									if(ti != null && ti.taskStatus == TaskStatus.RUNNING 
											&& sched.manager.getPlatformId().equals(ti.platformId)) {
										sched.releaseResource(queue, ti.jobId, ti.taskId, ti.taskType);
										sched.updateTaskInfo(ti.id, TaskStatus.TIMEOUT);
										sched.incrementTimeoutTaskCount(queue);
										sched.updateJobStatCounter(queue, JobStatus.TIMEOUT);
									}
								});
							}
						}
					});
					try {
						jobInfo.inMemJobUpdateLock.lock();
						jobInfo.jobStatus = JobStatus.TIMEOUT;
					} finally {
						jobInfo.inMemJobUpdateLock.unlock();
					}
					sched.handleInMemoryCompletedTask(jobId);
					sched.removeRedisJob(queue, jobId);
					handleTimeoutInMemTask(jobInfo, now);
				}
				
				// check running tasks
				tasks.forEach(task -> {
					// task with RUNNING status
					if(task.getStatus() == TaskStatus.RUNNING.getCode()) {
						sched.updateTaskInfo(job.getId(), task.getId(), TaskStatus.FAILED);
					}
				});
			}
		});
	}
	
	private void checkInMemStaleJobs() {
		sched.runningJobIdToInfos.keySet().forEach(jobId -> {
			JobInfo jobInfo = sched.runningJobIdToInfos.get(jobId);
			
			// check timeout jobs (RUNNING || CANCELLED)
			boolean isTimeout = Time.now() - jobInfo.lastUpdatedTime > staleJobMaxThresholdMillis;
			if(jobInfo.jobStatus != JobStatus.SUCCEEDED && jobInfo.jobStatus != JobStatus.FAILED && isTimeout) {
				LOG.info("Stale in-mem job: jobInfo=" + jobInfo);
				String queue = jobInfo.queue;
				final JobStatus targetJobStatus;
				try {
					jobInfo.inMemJobUpdateLock.lock();
					switch(jobInfo.jobStatus) {
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
					jobInfo.inMemJobUpdateLock.unlock();
				}
				
				LinkedList<TaskID> inMemTasks = sched.runningJobToTaskList.get(jobId);
				if(inMemTasks != null) {
					inMemTasks.forEach(id -> {
						TaskInfo ti = sched.runningTaskIdToInfos.get(id);
						if(ti != null && (ti.taskStatus == TaskStatus.RUNNING || targetJobStatus == JobStatus.CANCELLED)
								&& sched.manager.getPlatformId().equals(ti.platformId)) {
							sched.releaseResource(queue, ti.jobId, ti.taskId, ti.taskType);
							sched.updateTaskInfo(ti.id, TaskStatus.TIMEOUT);
							sched.incrementTimeoutTaskCount(queue);
							// clear cancelled job from memory
							if(targetJobStatus == JobStatus.CANCELLED) {
								sched.jobCancelled(jobId, () -> {
									jobInfo.lastUpdatedTime = Time.now();
									LOG.info("Cancelled job timeout: jobId=" + jobId + "jobInfo=" + jobInfo);
								});
							}
						}
					});
				}
				sched.updateJobInfo(jobId, targetJobStatus);
				
				// handle in-memory completed job
				sched.handleInMemoryCompletedTask(jobId);
				sched.removeRedisJob(queue, jobId);
				sched.updateJobStatCounter(queue, targetJobStatus);
			}
		});
	}
	
	private void checkInRedisStaleJobs() {
		sched.queueingManager.queueNames().forEach(queue -> {
			Set<String> jobs = sched.getJobs(queue);
			jobs.forEach(jstrJob -> {
				JSONObject job = JSONObject.parseObject(jstrJob);
				int jobId = job.getIntValue(ScheduledConstants.JOB_ID);
				String jStatus = job.getString(ScheduledConstants.JOB_STATUS);
				JobStatus jobStatus = JobStatus.valueOf(jStatus);
				long lastUpdateTs = job.getLongValue(ScheduledConstants.LAST_UPDATE_TS);
				
				JobInfo jobInfo = sched.runningJobIdToInfos.get(jobId);
				if(jobInfo == null) {
					// clear job with FAILED status from Redis queue
					if(jobStatus == JobStatus.FAILED) {
						sched.removeRedisJob(queue, jobId);
						LOG.warn("Stale job in Redis purged: queue=" + queue + ", job=" + job);
					}
					
					// clear timeout job with RUNNING status && in Redis queue
					long now = Time.now();
					if((jobStatus == JobStatus.RUNNING || jobStatus == JobStatus.CANCELLING)
							&& now - lastUpdateTs > staleJobMaxThresholdMillis) {
						sched.removeRedisJob(queue, jobId);
						LOG.warn("Stale job in Redis purged: queue=" + queue + ", job=" + job);
					}
				}
			});
		});
	}
	
	private void handleTimeoutInMemTask(JobInfo jobInfo, long now) {
		int jobId = jobInfo.jobId;
		jobInfo.lastUpdatedTime = now;
		timeoutJobIdToInfos.putIfAbsent(jobId, jobInfo);
		jobInfo.jobStatus = JobStatus.TIMEOUT;
		if(timeoutJobIdToInfos.size() > 2 * keptTimeoutJobMaxCount) {
			timeoutJobIdToInfos.values().stream()
			.sorted((x, y) -> x.lastUpdatedTime - y.lastUpdatedTime < 0 ? -1 : 1)
			.limit(keptTimeoutJobMaxCount)
			.forEach(ji -> {
				timeoutJobIdToInfos.remove(ji.jobId);
			});
		}
	}

}
