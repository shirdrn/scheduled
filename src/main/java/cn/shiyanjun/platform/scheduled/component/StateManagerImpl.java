package cn.shiyanjun.platform.scheduled.component;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import cn.shiyanjun.platform.api.common.AbstractComponent;
import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.api.JobPersistenceService;
import cn.shiyanjun.platform.scheduled.api.QueueingManager;
import cn.shiyanjun.platform.scheduled.api.StateManager;
import cn.shiyanjun.platform.scheduled.api.StorageService;
import cn.shiyanjun.platform.scheduled.api.TaskPersistenceService;
import cn.shiyanjun.platform.scheduled.common.JobInfo;
import cn.shiyanjun.platform.scheduled.common.TaskID;
import cn.shiyanjun.platform.scheduled.common.TaskInfo;
import cn.shiyanjun.platform.scheduled.common.TaskOrder;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
import cn.shiyanjun.platform.scheduled.dao.entities.Job;
import cn.shiyanjun.platform.scheduled.dao.entities.Task;

public class StateManagerImpl extends AbstractComponent implements StateManager {

	private static final Log LOG = LogFactory.getLog(StateManagerImpl.class);
	private final JobPersistenceService jobPersistenceService;
	private final TaskPersistenceService taskPersistenceService;
	private QueueingManager queueingManager;
	private final StorageService componentManager;
	private final ConcurrentMap<Integer, JobInfo> runningJobIdToInfos = Maps.newConcurrentMap();
	private final ConcurrentMap<Integer, LinkedList<TaskID>> runningJobToTaskList = Maps.newConcurrentMap();
	private final ConcurrentMap<TaskID, TaskInfo> runningTaskIdToInfos = Maps.newConcurrentMap();
	private final ConcurrentMap<Integer, JobInfo> completedJobIdToInfos = Maps.newConcurrentMap();
	private final ConcurrentMap<Integer, JobInfo> timeoutJobIdToInfos = Maps.newConcurrentMap();
	private final int keptHistoryJobMaxCount;
	
	public StateManagerImpl(StorageService componentManager) {
		super(componentManager.getContext());
		this.componentManager = componentManager;
		jobPersistenceService = componentManager.getJobPersistenceService();
		taskPersistenceService = this.componentManager.getTaskPersistenceService();
		
		keptHistoryJobMaxCount = context.getInt(ConfigKeys.SCHEDULED_KEPT_HISTORY_JOB_MAX_COUNT, 200);
		LOG.info("Configs: keptHistoryJobMaxCount=" + keptHistoryJobMaxCount);
	}
	
	@Override
	public void setQueueingManager(QueueingManager queueingManager) {
		this.queueingManager = queueingManager;		
	}
	
	@Override
	public Optional<JobInfo> getRunningJob(int jobId) {
		return Optional.ofNullable(runningJobIdToInfos.get(jobId));
	}
	
	@Override
	public Collection<JobInfo> getRunningJobs() {
		return runningJobIdToInfos.values();
	}

	@Override
	public void registerRunningJob(TaskOrder taskOrder) {
		int jobId = taskOrder.getTask().getJobId();
		if(!runningJobIdToInfos.containsKey(jobId)) {
			JobInfo jobInfo = new JobInfo(jobId, taskOrder.getQueue(), taskOrder.getTaskCount());
			jobInfo.setJobStatus(JobStatus.QUEUEING);
			runningJobIdToInfos.putIfAbsent(jobId, jobInfo);
		}		
	}
	
	@Override
	public Optional<String> getRunningJobQueueName(int jobId) {
		Optional<JobInfo> jobInfo = getRunningJob(jobId);
		return jobInfo.map(ji -> ji.getQueue());
	}
	
	@Override
	public void handleInMemoryCompletedJob(int jobId) {
		JobInfo current = runningJobIdToInfos.get(jobId);
		JobStatus jobStatus = current.getJobStatus();
		if(current != null && (jobStatus == JobStatus.SUCCEEDED 
				|| jobStatus == JobStatus.FAILED 
				|| jobStatus == JobStatus.TIMEOUT 
				|| jobStatus == JobStatus.CANCELLED)) {
			LOG.info("Moving in-mem job: runningJobIdToInfos -> completedJobIdToInfos");
			runningJobToTaskList.remove(jobId).stream().forEach(id -> {
				TaskInfo ti = runningTaskIdToInfos.remove(id);
				LOG.info("In-mem task removed: " + ti);
			});
			
			JobInfo removedJobInfo = runningJobIdToInfos.remove(jobId);
			// move job to completed queue
			completedJobIdToInfos.putIfAbsent(jobId, removedJobInfo);
			LOG.info("In-mem job moved: " + removedJobInfo);
			
			// clear history job/tasks which exceeded the maximum capacity of completed queue
			if(completedJobIdToInfos.size() > 2 * keptHistoryJobMaxCount) {
				LOG.info("Clear in-mem jobs: queue=completedJobIdToInfos" + ", size=" + completedJobIdToInfos.size() + ", keptHistoryJobCount=" + keptHistoryJobMaxCount);
				completedJobIdToInfos.values().stream()
				.sorted((x, y) -> x.getLastUpdatedTime() - y.getLastUpdatedTime() < 0 ? -1 : 1)
				.limit(keptHistoryJobMaxCount)
				.forEach(ti -> {
					JobInfo ji = completedJobIdToInfos.remove(ti.getJobId());
					LOG.info("In-mem job removed: " + ji);
				});
			}
		}
	}
	
	@Override
	public void handleInMemoryCompletedJob(TaskID id) {
		if(id != null) {
			// remove job/task from running queue
			handleInMemoryCompletedJob(id.getJobId());
		}
	}

	@Override
	public void handleInMemoryTimeoutJob(JobInfo jobInfo, int keptTimeoutJobMaxCount) {
		jobInfo.setLastUpdatedTime(Time.now());
		timeoutJobIdToInfos.putIfAbsent(jobInfo.getJobId(), jobInfo);
		if(timeoutJobIdToInfos.size() > 2 * keptTimeoutJobMaxCount) {
			timeoutJobIdToInfos.values().stream()
			.sorted((x, y) -> x.getLastUpdatedTime() - y.getLastUpdatedTime() < 0 ? -1 : 1)
			.limit(keptTimeoutJobMaxCount)
			.forEach(ji -> {
				timeoutJobIdToInfos.remove(ji.getJobId());
			});
		}		
	}
	
	
	/////////////////////////////////////////////////////////////

	
	@Override
	public void registerRunningTask(TaskID id, String platformId) {
		int jobId = id.getJobId();
		int taskId = id.getTaskId();
		int seqNo = id.getSeqNo();
		TaskStatus taskStatus = TaskStatus.SCHEDULED;
		JobStatus jobStatus = JobStatus.SCHEDULED;
		// update status: (job, task) = (SCHEDULED, SCHEDULED)
		// in DB
		if(seqNo == 1) {
			updateJobStatus(jobId, jobStatus, Time.now());
		}
		updateTaskStatusWithoutTime(jobId, taskId, seqNo, taskStatus);
		
		final TaskInfo taskInfo = new TaskInfo(id, platformId);
		runningTaskIdToInfos.putIfAbsent(id, taskInfo);
		
		JobInfo jobInfo = getRunningJob(jobId).get();
		// update running job status
		jobInfo.setJobStatus(JobStatus.SCHEDULED);
		
		if(seqNo == 1) {
			jobInfo.setLastUpdatedTime(Time.now());
			logInMemoryJobStateChanged(jobInfo);
		}
		LinkedList<TaskID> tasks = runningJobToTaskList.get(jobId);
		if(tasks == null) {
			tasks = Lists.newLinkedList();
			runningJobToTaskList.putIfAbsent(jobId, tasks);
		}
		tasks.add(id);
		
		logInMemoryStateChanges(jobInfo, taskInfo);
	}
	
	private void logInMemoryStateChanges(final JobInfo jobInfo, final TaskInfo taskInfo) {
		LOG.info("In-memory state changed: job=" + jobInfo + ", task=" + taskInfo); 
	}
	
	private void logInMemoryJobStateChanged(final JobInfo jobInfo) {
		LOG.info("In-memory job state changed: job=" + jobInfo);
	}
	
	
	/////////////////////////////////////////////////////////////
	
	
	@Override
	public void updateJobStatus(int jobId, JobStatus jobStatus) {
		updateJobStatus(jobId, jobStatus, Time.now());
	}
	
	@Override
	public void updateJobStatus(int jobId, JobStatus jobStatus, long timestamp) {
		Timestamp updateTime = new Timestamp(timestamp);
		Job job = new Job();
		job.setId(jobId);
		job.setStatus(jobStatus.getCode());
		job.setDoneTime(updateTime);
		jobPersistenceService.updateJobByID(job);
		LOG.info("In-db job state changed: jobId=" + jobId + ", updateTime=" + updateTime + ", jobStatus=" + jobStatus);
	}
	
	@Override
	public void updateJobStatus(JobStatus currentStatus, JobStatus targetStatus) {
		List<Job> runnings = jobPersistenceService.getJobByState(currentStatus);
		if(runnings != null) {
			runnings.forEach(job -> {
				job.setStatus(targetStatus.getCode());
				jobPersistenceService.updateJobByID(job);
				LOG.info("Update in-db job: jobId=" + job.getId() + ", currentStatus=" + currentStatus + ", targetStatus=" + targetStatus);
			});
		}		
	}
	
	@Override
	public Optional<Job> retrieveJob(int jobId) {
		return Optional.ofNullable(jobPersistenceService.retrieveJob(jobId));
	}
	
	@Override
	public List<Job> retrieveJobs(JobStatus jobStatus) {
		return jobPersistenceService.getJobByState(jobStatus);
	}
	
	
	/////////////////////////////////////////////////////////////

	
	@Override
	public void updateTaskStatus(int taskId, TaskStatus taskStatus) {
		updateTaskStatus(taskId, taskStatus, Time.now());		
	}

	@Override
	public void updateTaskStatus(int taskId, TaskStatus taskStatus, long timestamp) {
		Timestamp updateTime = new Timestamp(timestamp);
		Task task = new Task();
		task.setId(taskId);
		task.setStatus(taskStatus.getCode());
		task.setDoneTime(updateTime);
		taskPersistenceService.updateTaskByID(task);
		LOG.info("In-db task state changed: taskId=" + taskId + 
				", updateTime=" + updateTime + ", taskStatus=" + taskStatus);		
	}
	
	@Override
	public void updateTaskStatus(int  taskId, TaskStatus taskStatus, Optional<Integer> resultCount) {
		resultCount.ifPresent(count -> {
			Timestamp updateTime = new Timestamp(Time.now());
			Task task = new Task();
			task.setId(taskId);
			task.setStatus(taskStatus.getCode());
			task.setDoneTime(updateTime);
			task.setResultCount(count);
			taskPersistenceService.updateTaskByID(task);
			LOG.info("In-db task state changed: " + 
					", taskId=" + taskId + ", resultCount=" + count + 
					", updateTime=" + updateTime + ", taskStatus=" + taskStatus);
		});
	}
	
	@Override
	public List<Task> retrieveTasks(int jobId) {
		return taskPersistenceService.getTasksFor(jobId);
	}
	
	private void updateTaskStatusWithoutTime(int jobId, int taskId, int seqNo, TaskStatus taskStatus) {
		Task task = new Task();
		task.setId(taskId);
		task.setStatus(taskStatus.getCode());
		taskPersistenceService.updateTaskByID(task);
		LOG.info("In-db task state changed: jobId=" +
				jobId + ", taskId=" + taskId + ", seqNo=" + seqNo + ", taskStatus=" + taskStatus);
	}
	
	@Override
	public void insertTasks(List<Task> tasks) {
		taskPersistenceService.insertTasks(tasks);
	}
	
	@Override
	public void taskPublished(TaskID id) throws Exception {
		int jobId = id.getJobId();
		int taskId = id.getTaskId();
		int seqNo = id.getSeqNo();
		JobInfo jobInfo = runningJobIdToInfos.get(jobId);
		String queue = jobInfo.getQueue();
		// update status: (job, task) = (RUNNING, RUNNING)
		// in Redis
		
		TaskStatus taskStatus = TaskStatus.RUNNING;
		JobStatus jobStatus = JobStatus.RUNNING;
		JSONObject job = retrieveQueuedJob(queue, jobId);
		job.put(ScheduledConstants.JOB_STATUS, jobStatus.toString());
		job.put(ScheduledConstants.TASK_STATUS, taskStatus.toString());
		job.put(ScheduledConstants.LAST_UPDATE_TS, Time.now());
		updateQueuedJob(jobId, queue, job);
		
		// in DB
		if(seqNo == 1) {
			updateJobStatus(jobId, jobStatus);
		}
		
		updateStartedTask(jobId, taskId, seqNo, taskStatus);
		
		// in memory
		jobInfo.setJobStatus(jobStatus);;
		if(seqNo == 1) {
			jobInfo.setLastUpdatedTime(Time.now());
			logInMemoryJobStateChanged(jobInfo);
		}
		
		TaskInfo taskInfo = runningTaskIdToInfos.get(id);
		taskInfo.setTaskStatus(taskStatus);;
		taskInfo.setLastUpdatedTime(Time.now());
		logInMemoryStateChanges(jobInfo, taskInfo);
	}

	private void updateStartedTask(int jobId, int taskId, int seqNo, TaskStatus taskStatus) {
		Timestamp updateTime = new Timestamp(Time.now());
		Task task = new Task();
		task.setId(taskId);
		task.setStartTime(updateTime);
		task.setDoneTime(updateTime);
		task.setStatus(taskStatus.getCode());
		taskPersistenceService.updateTaskByID(task);
		LOG.info("In-db task state changed: jobId=" + jobId + ", taskId=" + taskId + ", seq=" + seqNo + 
				", startTime=" + updateTime + ", updateTime=" + updateTime + ", taskStatus=" + taskStatus);
	}
	
	
	/////////////////////////////////////////////////////////////
	
	
	@Override
	public JSONObject retrieveQueuedJob(String queue, int jobId) {
		return queueingManager
				.getQueueingContext(queue)
				.getJobQueueingService()
				.retrieve(jobId);
	}
	
	@Override
	public Set<JSONObject> retrieveQueuedJobs(String queue) {
		Set<JSONObject> jobs = queueingManager
			.getQueueingContext(queue)
			.getJobQueueingService()
			.getJobs()
			.stream()
			.map(j -> JSONObject.parseObject(j))
			.collect(Collectors.toSet());
		return jobs == null ? Sets.newHashSet() : jobs;
	}
	
	@Override
	public void updateQueuedJob(int jobId, String queue, JSONObject job) throws Exception {
		queueingManager
			.getQueueingContext(queue)
			.getJobQueueingService()
			.updateQueuedJob(jobId, job);
		LOG.info("Redis state changed: queue=" + queue + ", job=" + job);
	}
	
	@Override
	public Set<String> queueNames() {
		return queueingManager.queueNames();
	}

	@Override
	public boolean removeQueuedJob(String queue, int jobId) {
		try {
			queueingManager
				.getQueueingContext(queue)
				.getJobQueueingService()
				.remove(String.valueOf(jobId));
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	@Override
	public Optional<TaskInfo> getRunningTask(TaskID id) {
		return Optional.ofNullable(runningTaskIdToInfos.get(id));
	}

	@Override
	public Collection<TaskInfo> getRunningTasks(int jobId) {
		return runningJobToTaskList.get(jobId)
				.stream()
				.map(id -> runningTaskIdToInfos.get(id))
				.collect(Collectors.toList());
	}
	
	@Override
	public void recoverTaskInMemoryStructures(JSONObject taskResponse) throws Exception {
		int jobId = taskResponse.getIntValue(ScheduledConstants.JOB_ID);
		int taskId = taskResponse.getIntValue(ScheduledConstants.TASK_ID);
		int seqNo = taskResponse.getIntValue(ScheduledConstants.SEQ_NO);
		int taskTypeCode = taskResponse.getIntValue(ScheduledConstants.TASK_TYPE);
		TaskType taskType = TaskType.fromCode(taskTypeCode).get();
		TaskID id = new TaskID(jobId, taskId, seqNo, taskType);
		String oldPlatformId = taskResponse.getString(ScheduledConstants.PLATFORM_ID);
		// check job status in Redis queue
		Job ujob = jobPersistenceService.retrieveJob(jobId);
		if(ujob != null) {
			String queue = queueingManager.getQueueName(ujob.getJobType());
			JSONObject job = retrieveQueuedJob(queue, jobId);
			if(job != null) {
				int taskCount = job.getIntValue(ScheduledConstants.TASK_COUNT);
				String queuedJobStatus = job.getString(ScheduledConstants.JOB_STATUS);
				if(JobStatus.RUNNING == JobStatus.valueOf(queuedJobStatus)) {
					long now = Time.now();
					job.put(ScheduledConstants.LAST_UPDATE_TS, now);
					// update last update timestamp at once to avoid being purged
					updateQueuedJob(jobId, queue, job);
					
					// rebuild job in-memory structure
					JobInfo jobInfo = new JobInfo(jobId, queue, taskCount);
					jobInfo.setJobStatus(JobStatus.RUNNING);
					jobInfo.setLastUpdatedTime(now);
					logInMemoryJobStateChanged(jobInfo);
					runningJobIdToInfos.putIfAbsent(jobId, jobInfo);
					
					final TaskInfo taskInfo = new TaskInfo(id);
					taskInfo.setPlatformId(oldPlatformId);
					taskInfo.setLastUpdatedTime(now);
					taskInfo.setTaskStatus(TaskStatus.RUNNING);
					runningTaskIdToInfos.putIfAbsent(id, taskInfo);
					
					LinkedList<TaskID> tasks = runningJobToTaskList.get(jobId);
					if(tasks == null) {
						tasks = Lists.newLinkedList();
						runningJobToTaskList.putIfAbsent(jobId, tasks);
					}
					tasks.add(id);
					LOG.info("Task in-mem structures recovered: jobId=" + jobId + 
							", taskCount=" + taskCount + 
							", jobStatus=" + jobInfo.getJobStatus() + 
							", taskId=" + taskId + ", seqNo=" + seqNo + 
							", taskStatus=" + taskInfo.getTaskStatus());
				}
			} else {
				LOG.warn("Task timeout: " + taskResponse);
			}
		}		
	}

	@Override
	public boolean isJobCompleted(int jobId) {
		Optional<Job> job = retrieveJob(jobId);
		return job.map(j -> {
			JobStatus currentStatus = JobStatus.valueOf(j.getStatus()).get();
			if(currentStatus == JobStatus.SUCCEEDED
					|| currentStatus == JobStatus.FAILED
					|| currentStatus == JobStatus.CANCELLED) {
				return true;
			} else {
				return false;
			}
		}).orElse(true);
	}

	@Override
	public boolean isInMemJobCompleted(int jobId) {
		JobInfo rJI = runningJobIdToInfos.get(jobId);
		JobInfo cJI = completedJobIdToInfos.get(jobId);
		if(rJI != null) {
			return rJI.getJobStatus() == JobStatus.SUCCEEDED 
					|| rJI.getJobStatus() == JobStatus.FAILED
					|| rJI.getJobStatus() == JobStatus.CANCELLED;
		}
		if(cJI != null) {
			return cJI.getJobStatus() == JobStatus.SUCCEEDED 
					|| cJI.getJobStatus() == JobStatus.FAILED
					|| cJI.getJobStatus() == JobStatus.CANCELLED;
		}
		return false;
	}

}
