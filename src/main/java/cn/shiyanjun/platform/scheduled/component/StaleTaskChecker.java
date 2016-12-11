package cn.shiyanjun.platform.scheduled.component;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.component.DefaultSchedulingManager.JobInfo;
import cn.shiyanjun.platform.scheduled.component.DefaultSchedulingManager.TaskID;
import cn.shiyanjun.platform.scheduled.component.DefaultSchedulingManager.TaskInfo;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
import cn.shiyanjun.platform.scheduled.dao.entities.Job;
import cn.shiyanjun.platform.scheduled.dao.entities.Task;

/**
 * Check tasks whose status are deem to be not updated for a specified time,
 * and decide to clear the tasks' in-redis or in-memory statuses.
 * 
 * @author yanjun
 */
class StaleTaskChecker implements Runnable {
	
	private static final Log LOG = LogFactory.getLog(StaleTaskChecker.class);
	private final DefaultSchedulingManager schedulingManager;
	private final ConcurrentMap<Integer, JobInfo> timeoutJobIdToInfos = Maps.newConcurrentMap();
	private final int keptTimeoutJobMaxCount;
	private final int staleTaskMaxThresholdSecs;
	private final long staleTaskMaxThresholdMillis;
	
	public StaleTaskChecker(final DefaultSchedulingManager schedulingManager) {
		this.schedulingManager = schedulingManager;
		keptTimeoutJobMaxCount = schedulingManager.manager.getContext().getInt(ConfigKeys.SCHEDULED_KEPT_TIMEOUT_JOB_MAX_COUNT, 100);
		// default 2 hours
		staleTaskMaxThresholdSecs = schedulingManager.manager.getContext().getInt(ConfigKeys.SCHEDULED_STALE_TASK_MAX_THRESHOLD_SECS, 7200);
		staleTaskMaxThresholdMillis = staleTaskMaxThresholdSecs * 1000;
	}
	
	void checkJobTaskStatuses() {
		// check stale job/tasks in DB, and update status for these job/tasks
		checkStaleTasksInDB();
		
		// purge stale jobs in Redis queue
		checkStaleTasksInRedisQueue();
	}

	@Override
	public void run() {
		LOG.debug("Start to check stale tasks...");
		try {
			checkJobTaskStatuses();
			
			// purge stale job/tasks in-memory
			purgeStaleTasks();
		} catch (Exception e) {
			LOG.warn("Fail to check stale tasks: ", e);
		} finally {
			LOG.debug("Complete to check stale tasks.");
		}
	}
	
	void checkStaleTasksInDB() {
		// job with RUNNING status
		List<Job> jobs = schedulingManager.getJobByState(JobStatus.RUNNING);
		for(Job job : jobs) {
			long now = Time.now();
			// check a running job
			if(now - job.getDoneTime().getTime() > staleTaskMaxThresholdMillis) {
				List<Task> tasks = schedulingManager.getTasksFor(job.getId());
				// check running tasks
				for(Task task : tasks) {
					// task with RUNNING status
					if(task.getStatus() == TaskStatus.RUNNING.getCode() 
							&& now - task.getStartTime().getTime() > staleTaskMaxThresholdMillis) {
						schedulingManager.updateJobInfo(job.getId(), JobStatus.FAILED);
						schedulingManager.updateTaskInfo(job.getId(), task.getId(), task.getSerialNo(), TaskStatus.FAILED);
						break;
					}
				}
			}
		}
	}
	
	private void purgeStaleTasks() {
		long now = Time.now();
		List<TaskID> staleTasks = Lists.newArrayList();
		
		for(TaskID id : schedulingManager.runningTaskIdToInfos.keySet()) {
			TaskInfo info = schedulingManager.runningTaskIdToInfos.get(id);
			
			// check failed non-last tasks of jobs  
			boolean isTimeout = now - info.lastUpdatedTime > staleTaskMaxThresholdMillis;
			if(info.taskStatus != TaskStatus.SUCCEEDED && isTimeout) {
				staleTasks.add(id);
			}
		}
		
		if(!staleTasks.isEmpty()) {
			LOG.info("Stale task list: " + staleTasks);
			// purge stale task related information
			for(TaskID id : staleTasks) {
				TaskInfo info = schedulingManager.runningTaskIdToInfos.get(id);
				int jobId = info.jobId;
				
				// update job/task status in DB
				schedulingManager.updateJobInfo(jobId, JobStatus.FAILED);
				schedulingManager.updateTaskInfo(jobId, id.taskId, id.serialNo, TaskStatus.FAILED);
				
				// update Redis queue status, next time remove it absolutely
				Optional<String> q = schedulingManager.getJobQueueByJobId(jobId);
				q.ifPresent(queue -> {
					try {
						JSONObject job = schedulingManager.getRedisJob(queue, jobId);
						if(job != null) {
							job.put(ScheduledConstants.JOB_STATUS, JobStatus.FAILED.toString());
							job.put(ScheduledConstants.TASK_STATUS, TaskStatus.FAILED);
							job.put(ScheduledConstants.LAST_UPDATE_TS, Time.now());
							schedulingManager.updateRedisState(jobId, queue, job);
							
							// handle in-memory completed task
							schedulingManager.handleInMemoryCompletedTask(id);
							
							// release resources
							if(schedulingManager.manager.getPlatformId().equals(info.platformId)) {
								schedulingManager.releaseResource(queue, info.taskType);
							}
							schedulingManager.incrementTimeoutTaskCount(queue);
						}
					} catch (Exception e) {
						LOG.warn("Fail to get Redis job: queue=" + queue + ", jobId=" + jobId, e);
					}
				});
			}
		}
	}
	
	void checkStaleTasksInRedisQueue() {
		for(String queue : schedulingManager.queueingManager.queueNames()) {
			Set<String> jobs = schedulingManager.getJobs(queue);
			for(String jstrJob : jobs) {
				JSONObject job = JSONObject.parseObject(jstrJob);
				int jobId = job.getIntValue(ScheduledConstants.JOB_ID);
				String jStatus = job.getString(ScheduledConstants.JOB_STATUS);
				JobStatus jobStatus = JobStatus.valueOf(jStatus);
				long lastUpdateTs = job.getLongValue(ScheduledConstants.LAST_UPDATE_TS);
				
				JobInfo jobInfo = schedulingManager.runningJobIdToInfos.get(jobId);
				if(jobInfo != null) {
					try {
						if(jobInfo.inMemJobUpdateLock.tryLock(500, TimeUnit.MILLISECONDS)) {
							// clear job with FAILED status in Redis queue
							if(jobStatus == JobStatus.FAILED) {
								schedulingManager.removeRedisJob(queue, jobId);
								LOG.warn("Stale job in Redis purged: queue=" + queue + ", job=" + job);
								
								// handle in-memory completed job
								schedulingManager.handleInMemoryCompletedTask(jobId);
							}
							
							// clear timeout job with RUNNING status && in Redis queue
							long now = Time.now();
							if(jobStatus == JobStatus.RUNNING 
									&& now - lastUpdateTs > staleTaskMaxThresholdMillis) {
								schedulingManager.removeRedisJob(queue, jobId);
								LOG.warn("Stale job in Redis purged: queue=" + queue + ", job=" + job);
								
								// move in-memory timeout job/task to timeout queue
								handleTimeoutInMemTask(jobId, jobInfo, now);
							}
						}
					} catch(InterruptedException e) {
						// ignore interrupt exception
						continue;
					} finally {
						jobInfo.inMemJobUpdateLock.unlock();
					}
				} else {
					// clear job with FAILED status in Redis queue
					if(jobStatus == JobStatus.FAILED) {
						schedulingManager.removeRedisJob(queue, jobId);
						LOG.warn("Stale job in Redis purged: queue=" + queue + ", job=" + job);
					}
					
					// clear timeout job with RUNNING status && in Redis queue
					long now = Time.now();
					if(jobStatus == JobStatus.RUNNING 
							&& now - lastUpdateTs > staleTaskMaxThresholdMillis) {
						schedulingManager.removeRedisJob(queue, jobId);
						LOG.warn("Stale job in Redis purged: queue=" + queue + ", job=" + job);
					}
				}
			}
		}
	}
	
	private void handleTimeoutInMemTask(int jobId, JobInfo jobInfo, long now) {
		jobInfo.lastUpdatedTime = now;
		timeoutJobIdToInfos.putIfAbsent(jobId, jobInfo);
		jobInfo.jobStatus = JobStatus.FAILED;
		if(timeoutJobIdToInfos.size() > 2 * keptTimeoutJobMaxCount) {
			timeoutJobIdToInfos.values().stream()
			.sorted((x, y) -> x.lastUpdatedTime - y.lastUpdatedTime < 0 ? -1 : 1)
			.limit(keptTimeoutJobMaxCount)
			.forEach(ji -> {
				timeoutJobIdToInfos.remove(ji.jobId);
			});
		}
		
		// handle in-memory completed job
		schedulingManager.handleInMemoryCompletedTask(jobId);
	}

}
