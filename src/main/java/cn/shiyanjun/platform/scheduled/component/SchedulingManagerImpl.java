package cn.shiyanjun.platform.scheduled.component;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.constants.JSONKeys;
import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.api.utils.NamedThreadFactory;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.JobPersistenceService;
import cn.shiyanjun.platform.scheduled.api.JobQueueingService;
import cn.shiyanjun.platform.scheduled.api.MQAccessService;
import cn.shiyanjun.platform.scheduled.api.QueueingManager;
import cn.shiyanjun.platform.scheduled.api.ResourceManager;
import cn.shiyanjun.platform.scheduled.api.SchedulingManager;
import cn.shiyanjun.platform.scheduled.api.SchedulingPolicy;
import cn.shiyanjun.platform.scheduled.api.TaskPersistenceService;
import cn.shiyanjun.platform.scheduled.common.AbstractJobController;
import cn.shiyanjun.platform.scheduled.common.AbstractRunnableConsumer;
import cn.shiyanjun.platform.scheduled.common.TaskOrder;
import cn.shiyanjun.platform.scheduled.component.QueueingManagerImpl.QueueingContext;
import cn.shiyanjun.platform.scheduled.component.ResourceManagerImpl.JobStatCounter;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
import cn.shiyanjun.platform.scheduled.dao.entities.Job;
import cn.shiyanjun.platform.scheduled.dao.entities.Task;

/**
 * Manage job/task scheduling, includes:
 * <ol>
 * 	<li>Select a task to be scheduled.</li>
 * 	<li>Consume task result response from RabbitMQ.</li>
 * 	<li>Process task responses to manage job/task statuses.</li>
 * 	<li>Manage to update task resource counters</li>
 * 	<li>Manage job/task in-memory status changes</li>
 * </ol>
 * @author yanjun
 */
public class SchedulingManagerImpl extends AbstractJobController implements SchedulingManager {

	private static final Log LOG = LogFactory.getLog(SchedulingManagerImpl.class);
	protected final ComponentManager manager;
	protected final QueueingManager queueingManager;
	private ExecutorService executorService;
	private ScheduledExecutorService scheduledExecutorService;
	private final TaskPersistenceService taskPersistenceService;
	private final JobPersistenceService jobPersistenceService;
	private final MQAccessService taskMQAccessService;
	private final MQAccessService heartbeatMQAccessService;
	private volatile boolean running = true;
	protected final ConcurrentMap<Integer, JobInfo> runningJobIdToInfos = Maps.newConcurrentMap();
	protected final ConcurrentMap<Integer, LinkedList<TaskID>> runningJobToTaskList = Maps.newConcurrentMap();
	protected final ConcurrentMap<TaskID, TaskInfo> runningTaskIdToInfos = Maps.newConcurrentMap();
	private final ConcurrentMap<Integer, JobInfo> completedJobIdToInfos = Maps.newConcurrentMap();
	private final SchedulingPolicy schedulingPolicy;
	private final ResourceManager resourceMetadataManager;
	private final FreshTasksResponseProcessingManager freshTasksResponseProcessingManager;
	private final BlockingQueue<Heartbeat> rawHeartbeatMessages = Queues.newLinkedBlockingQueue();
	private final StaleJobChecker staleJobChecker;
	
	private final int keptHistoryJobMaxCount;
	private final TasksResponseHandler<Heartbeat> tenuredInflightTasksResponseHandler = new TenuredInflightTaskResponseHandler();
	private final TasksResponseHandler<JSONObject> tenuredInMQTasksResponseHandler = new TenuredInMQTaskResponseHandler();
	private final HeartbeatHandlingController taskResponseHandlingController;

	public SchedulingManagerImpl(ComponentManager manager) {
		super(manager.getContext());
		this.manager = manager;
		queueingManager = this.manager.getQueueingManager();
		taskPersistenceService = this.manager.getTaskPersistenceService();
		taskMQAccessService = this.manager.getTaskMQAccessService();
		heartbeatMQAccessService = this.manager.getHeartbeatMQAccessService();
 		schedulingPolicy = this.manager.getSchedulingPolicy();
		jobPersistenceService = manager.getJobPersistenceService();
		resourceMetadataManager = manager.getResourceManager();
		freshTasksResponseProcessingManager = new FreshTasksResponseProcessingManager();
		staleJobChecker = new StaleJobChecker(this);
		taskResponseHandlingController = new SimpleTaskResponseHandlingController(context);
		
		keptHistoryJobMaxCount = context.getInt(ConfigKeys.SCHEDULED_KEPT_HISTORY_JOB_MAX_COUNT, 200);
		LOG.info("Configs: keptHistoryJobMaxCount=" + keptHistoryJobMaxCount);
	}
	
	@Override
	public void start() {
		// recover task responses
		taskResponseHandlingController.recoverTasks();
		
		heartbeatMQAccessService.start();
		
		// start services
		executorService = Executors.newCachedThreadPool(new NamedThreadFactory("SCHEDULED"));
		executorService.execute(new SchedulingThread());
		executorService.execute(new RawHeartbeatMessageDispatcher());
		executorService.execute(new HeartbeatConsumer(heartbeatMQAccessService.getQueueName(), heartbeatMQAccessService.getChannel()));
		executorService.execute(freshTasksResponseProcessingManager);
		
		scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("STALE-CHECKER"));
		// default 15 minutes
		final int staleTaskCheckIntervalSecs = context.getInt(ConfigKeys.SCHEDULED_STALE_TASK_CHECK_INTERVAL_SECS, 900);
		scheduledExecutorService.scheduleAtFixedRate(
				staleJobChecker, staleTaskCheckIntervalSecs, staleTaskCheckIntervalSecs, TimeUnit.SECONDS);
	}

	@Override
	public void stop() {
		executorService.shutdown();
		scheduledExecutorService.shutdown();
		heartbeatMQAccessService.stop();
		running = false;
	}
	
	@Override
	public boolean cancelJob(int jobId) {
		super.cancelJob(jobId);
		try {
			updateJobInfo(jobId, JobStatus.CANCELLING);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	/**
	 * Schedules a prepared task, and publish it to the MQ
	 * 
	 * @author yanjun
	 */
	private class SchedulingThread extends Thread {
		
		private final int scheduleTaskIntervalMillis = 5000;
		
		@Override
		public void run() {
			while(running) {
				try {
					// for each job in Redis queue
					queueingManager.queueNames().forEach(queue -> {
						resourceMetadataManager.taskTypes(queue).forEach(taskType -> {
							LOG.debug("Loop for: queue=" + queue + ", taskType=" + taskType);
							Optional<TaskOrder> offeredTask = schedulingPolicy.offerTask(queue, taskType);
							offeredTask.ifPresent(task -> {
								int jobId = task.getTask().getJobId();
								if(shouldCancelJob(jobId)) {
									// cancel a running job
									jobCancelled(jobId, () -> {
										try {
											updateJobInfo(jobId, JobStatus.CANCELLED);
											removeRedisJob(queue, jobId);
										} finally {
											releaseResource(queue, taskType);
										}
									});
								} else {
									if(!runningJobIdToInfos.containsKey(jobId)) {
										JobInfo jobInfo = new JobInfo(jobId, queue, task.getTaskCount());
										jobInfo.jobStatus = JobStatus.QUEUEING;
										runningJobIdToInfos.putIfAbsent(jobId, jobInfo);
									}
									LOG.info("Prepare to schedule: queue=" + queue + ", taskType=" + taskType + ", " + task);
									scheduleTask(queue, taskType, task, runningJobIdToInfos.get(task.getTask().getJobId()));
								}
							});
						});
					});
					Thread.sleep(scheduleTaskIntervalMillis);
				} catch (Exception e) {
					LOG.warn("Fail to schedule task: ", e);
				}
			}
		}
		
		private void scheduleTask(String queue, TaskType taskType, TaskOrder scheduledTask, JobInfo jobInfo) {
			int jobId = scheduledTask.getTask().getJobId();
			int taskId = scheduledTask.getTask().getId();
			int serialNo = scheduledTask.getTask().getSerialNo();
			int taskCount = scheduledTask.getTaskCount();
			boolean alreadyPublished = false;
			TaskID id = null;
			try {
				JSONObject job = getRedisJob(queue, jobId);
				
				JobStatus jobStatus = JobStatus.valueOf(job.getString(ScheduledConstants.JOB_STATUS));
				TaskStatus taskStatus = TaskStatus.valueOf(job.getString(ScheduledConstants.TASK_STATUS));
				
				assert jobStatus == JobStatus.SCHEDULED;
				assert taskStatus == TaskStatus.SCHEDULED;
				
				// update status: (job, task) = (SCHEDULED, SCHEDULED)
				// in DB
				if(serialNo == 1) {
					updateJobInfo(jobId, jobStatus);
				}
				updateTaskStatusWithoutTime(jobId, taskId, serialNo, taskStatus);
				// in memory
				id = new TaskID(jobId, taskId, serialNo, taskType);
				final TaskInfo taskInfo = new TaskInfo(id);
				taskInfo.platformId = manager.getPlatformId();
				taskInfo.scheduledTime = Time.now();
				taskInfo.lastUpdatedTime = taskInfo.scheduledTime;
				taskInfo.taskStatus = taskStatus;
				runningTaskIdToInfos.putIfAbsent(id, taskInfo);
				jobInfo.jobStatus = jobStatus;
				if(serialNo == 1) {
					jobInfo.lastUpdatedTime = Time.now();
					logInMemoryJobStateChanged(jobInfo);
				}
				LinkedList<TaskID> tasks = runningJobToTaskList.get(jobId);
				if(tasks == null) {
					tasks = Lists.newLinkedList();
					runningJobToTaskList.putIfAbsent(id.jobId, tasks);
				}
				tasks.add(id);
				logInMemoryStateChanges(jobInfo, taskInfo);
				
				try {
					// here we should update some job/task statuses after message is published to rabbit MQ,
					// and lock the jobInfo to assure the time of these updates are ahead of returned task result processing
					jobInfo.inMemJobUpdateLock.lock();
					
					// publish to the MQ
					JSONObject taskMessage = buildTaskMessage(manager.getPlatformId(), scheduledTask.getTask(), taskCount);
					alreadyPublished = taskMQAccessService.produceMessage(taskMessage.toJSONString());
					
					jobStatus = JobStatus.RUNNING;
					taskStatus = TaskStatus.RUNNING;
					
					assert jobStatus == JobStatus.RUNNING;
					assert taskStatus == TaskStatus.RUNNING;
					
					// update status: (job, task) = (RUNNING, RUNNING)
					// in Redis
					job.put(ScheduledConstants.JOB_STATUS, jobStatus.toString());
					job.put(ScheduledConstants.TASK_STATUS, taskStatus.toString());
					job.put(ScheduledConstants.LAST_UPDATE_TS, Time.now());
					updateRedisState(jobId, queue, job);
					
					// in DB
					if(serialNo == 1) {
						updateJobInfo(jobId, jobStatus);
					}
					
					updateStartedTask(jobId, taskId, serialNo, taskStatus);
					
					// in memory
					jobInfo.jobStatus = jobStatus;
					if(serialNo == 1) {
						jobInfo.lastUpdatedTime = Time.now();
						logInMemoryJobStateChanged(jobInfo);
					}
					taskInfo.taskStatus = taskStatus;
					taskInfo.lastUpdatedTime = Time.now();
					logInMemoryStateChanges(jobInfo, taskInfo);
					
					logTaskCounterChanged(queue);
				} finally {
					jobInfo.inMemJobUpdateLock.unlock();
				}
			} catch(Exception e) {
				LOG.error("Fail to schedule task: jobId=" + jobId + ", taskId=" + taskId + ", serialNo=" + serialNo, e);
				if(!alreadyPublished) {
					releaseResource(queue, taskType);
				}
				
				// update job status
				updateJobInfo(jobId, JobStatus.FAILED);
				
				// handle in-memory completed task
				handleInMemoryCompletedTask(id);
			}
		}
		
	}
	
	/**
	 * Dispatches heartbeat messages, pulled from RabbitMQ. 
	 * At present just includes task response messages.
	 * 
	 * @author yanjun
	 */
	private final class RawHeartbeatMessageDispatcher implements Runnable {
		
		@Override
		public void run() {
			while(running) {
				try {
					Heartbeat rawTasksResponse = rawHeartbeatMessages.take();
					if(taskResponseHandlingController.isTenuredTasksResponse(rawTasksResponse.heartbeatMessage)) {
						LOG.info("Dispatching tenured tasks response: " + rawTasksResponse);
						taskResponseHandlingController.processTenuredTasksResponse(rawTasksResponse);
					} else {
						LOG.info("Dispatching fresh tasks response: " + rawTasksResponse);
						taskResponseHandlingController.processFreshTasksResponse(rawTasksResponse);
					}
				} catch (Exception e) {
					LOG.warn("Fail to process heartbeat: ", e);
				}
			}
		}
	}
	
	/**
	 * We have defined 1 type of heartbeat messages from <code>Job Running Platform</code>:
	 * <ul>
	 * 	<li>Task progress heartbeat message</li>
	 * 		<pre>
	 * 		{"type":"taskProgress","tasks":[{"result":{"resultCount":6910757,"status":5},"jobId":1328,"taskType":2,"taskId":3404,"serialNo":3,"status":"SUCCEEDED"}]}</br>
	 * 		</pre>
	 * </ul>
	 * 
	 * @author yanjun
	 */
	private final class FreshTasksResponseProcessingManager implements Runnable {
		
		private final BlockingQueue<Heartbeat> tasksResponseMessageQueue = Queues.newLinkedBlockingQueue();
		private final TasksResponseHandler<Heartbeat> handler = new FreshHeartbeatHandler();
		
		@Override
		public void run() {
			while(running) {
				try {
					Heartbeat tasksResponse = tasksResponseMessageQueue.take();
					LOG.info("Takes a fresh tasks response: " + tasksResponse.heartbeatMessage);
					handler.handle(tasksResponse);
				} catch (Exception e) {
					LOG.warn("Fail to process heartbeat: ", e);
				}
			}
		}
		
	}
	
	Set<String> getJobs(String queue) {
		JobQueueingService qs = getJobQueueingService(queue);
		Set<String> jobs = qs.getJobs();
		return jobs == null ? Sets.newHashSet() : jobs;
	}
	
	JSONObject getRedisJob(String queue, int jobId) throws Exception {
		JobQueueingService qs = getJobQueueingService(queue);
		return qs.retrieve(jobId);
	}
	
	boolean removeRedisJob(String queue, int jobId) {
		JobQueueingService qs = getJobQueueingService(queue);
		try {
			qs.remove(String.valueOf(jobId));
		} catch (Exception e) {
			LOG.warn("Fail to remove job from Redis: ", e);
			return false;
		}
		return true;
	}
	
	interface HeartbeatHandlingController {
		
		void recoverTasks();
		
		boolean isTenuredTasksResponse(JSONObject tasksResponse);
		boolean isValid(JSONObject tasksResponse);
		void processTenuredTasksResponse(Heartbeat hb);
		void processFreshTasksResponse(Heartbeat hb);
		void recoverTaskInMemoryStructures(JSONObject taskResponse) throws Exception;
		
		void releaseResource(final String queue, TaskType taskType);
		void releaseResource(final String queue, TaskType taskType, boolean isTenuredTaskResponse);
		
		void incrementSucceededTaskCount(String queue, JSONObject taskResponse);
		void incrementFailedTaskCount(String queue, JSONObject taskResponse);
		
	}
	
	private final class SimpleTaskResponseHandlingController implements HeartbeatHandlingController {
		
		private final boolean isRecoveryFeatureEnabled;
		
		public SimpleTaskResponseHandlingController(Context ctx) {
			isRecoveryFeatureEnabled = ctx.getBoolean(ConfigKeys.SCHEDULED_RECOVERY_FEATURE_ENABLED, false);
			LOG.info("Configs: isRecoveryFeatureEnabled=" + isRecoveryFeatureEnabled);
		}
		
		@Override
		public void incrementSucceededTaskCount(String queue, JSONObject taskResponse) {
			if(!isTenuredTasksResponse(taskResponse)) {
				resourceMetadataManager.getTaskStatCounter(queue).incrementSucceededTaskCount();
			}
		}
		
		@Override
		public void incrementFailedTaskCount(String queue, JSONObject taskResponse) {
			if(!isTenuredTasksResponse(taskResponse)) {
				resourceMetadataManager.getTaskStatCounter(queue).incrementFailedTaskCount();
			}
		}
		
		@Override
		public void recoverTasks() {
			if(isRecoveryFeatureEnabled) {
				Map<Integer, JSONObject> jobs = manager.getRecoveryManager().getPendingTaskResponses();
				jobs.keySet().forEach(jobId -> {
					LOG.info("Start to recover task...");
					JSONObject taskResponse = null;
					try {
						taskResponse = jobs.get(jobId);
						tenuredInMQTasksResponseHandler.handle(taskResponse);
						LOG.info("Task recovered: jobId=" + jobId + ", task=" + taskResponse);
					} catch (Exception e) {
						LOG.warn("Fail to recover task: " + taskResponse, e);
					}
					LOG.info("Complete to recover tasks.");
				});
			} else {
				Map<Integer, JSONObject> jobs = manager.getRecoveryManager().getPendingTaskResponses();
				jobs.keySet().forEach(jobId -> {
					JSONObject taskResponse = null;
					try {
						taskResponse = jobs.get(jobId);
						if(!isJobTaskCompleted(taskResponse)) {
							updateJobInfo(jobId, JobStatus.FAILED);
						}
					} catch (Exception e) {
						LOG.warn("Fail to recover task: " + taskResponse, e);
					}
				});
				
				List<Job> runnings = jobPersistenceService.getJobByState(JobStatus.RUNNING);
				if(runnings != null) {
					runnings.forEach(job -> {
						job.setStatus(JobStatus.FAILED.getCode());
						jobPersistenceService.updateJobByID(job);
						LOG.info("Update in-db job to FAILED: jobId=" + job.getId());
					});
				}
			}
		}
		
		private boolean isJobTaskCompleted(JSONObject taskResponse) {
			int jobId = taskResponse.getIntValue(ScheduledConstants.JOB_ID);
			// for tenured task response
			if(isTenuredTasksResponse(taskResponse)) {
				Job job = jobPersistenceService.retrieveJob(jobId);
				if(job != null) {
					if(job.getStatus() == JobStatus.SUCCEEDED.getCode() || job.getStatus() == JobStatus.FAILED.getCode()) {
						return true;
					}
				}
			} else {
				JobInfo rJI = runningJobIdToInfos.get(jobId);
				JobInfo cJI = completedJobIdToInfos.get(jobId);
				if(rJI != null) {
					return rJI.jobStatus == JobStatus.SUCCEEDED || rJI.jobStatus == JobStatus.FAILED;
				}
				if(cJI != null) {
					return cJI.jobStatus == JobStatus.SUCCEEDED || cJI.jobStatus == JobStatus.FAILED;
				}
			}
			return false;
		}
		
		@Override
		public boolean isTenuredTasksResponse(JSONObject tasksResponse) {
			if(isTenuredInflightTasksResponse(tasksResponse) || isNeedRecoveryTasksResponse(tasksResponse)) {
				return true;
			}
			return false;
		}
		
		@Override
		public boolean isValid(JSONObject tasksResponse) {
			String platformId = tasksResponse.getString(ScheduledConstants.PLATFORM_ID);
			if(Strings.isNullOrEmpty(platformId)) {
				return false;
			}
			return true;
		}
		
		@Override
		public void processTenuredTasksResponse(Heartbeat hb) {
			JSONObject tasksResponse = hb.heartbeatMessage;
			JSONArray tasks = hb.heartbeatMessage.getJSONArray(ScheduledConstants.TASKS);
			if(tasks != null) {
				if(isRecoveryFeatureEnabled) {
					if(isTenuredInflightTasksResponse(tasksResponse)) {
						tenuredInflightTasksResponseHandler.handle(hb);
					} else {
						// tenured in MQ tasks response
						tenuredInMQTasksResponseHandler.handle(tasksResponse);
					}
				} else {
					// if recovery feature wasn't enabled, just to update uncompleted job status to FAILED
					for (int i = 0; i < tasks.size(); i++) {
						JSONObject taskResponse = tasks.getJSONObject(i);
						int jobId = taskResponse.getIntValue(ScheduledConstants.JOB_ID);
						if(!isJobTaskCompleted(tasksResponse)) {
							updateJobInfo(jobId, JobStatus.FAILED);
						}
					}
				}
			}
		}
		
		private boolean isTenuredInflightTasksResponse(JSONObject tasksResponse) {
			// when scheduling platform is started, the platform ID is renewed, 
			// but some tasks' execution results don't return to be kept in RabbitMQ,
			// for this case, we shouldn't operate resource counter after parsing the tasks' results.
			// here, we just check whether task response was an inflight result 
			String platformId = tasksResponse.getString(ScheduledConstants.PLATFORM_ID);
			LOG.debug("protocol.getPlatformId() = " + manager.getPlatformId());
			LOG.debug("tasksResponse.getPlatformId() = " + platformId);
			return !manager.getPlatformId().equals(platformId);
		}
		
		private boolean isNeedRecoveryTasksResponse(JSONObject tasksResponse) {
			return tasksResponse.containsKey(ScheduledConstants.NEED_RECOVERING);
		}
		
		@Override
		public void processFreshTasksResponse(Heartbeat hb) {
			freshTasksResponseProcessingManager.tasksResponseMessageQueue.add(hb);
		}
		
		// after reboot scheduling platform, maybe some tasks result from running platform are not processed in time.
		// some required in-memory structures should be rebuilt to support task status management.
		@Override
		public void recoverTaskInMemoryStructures(JSONObject taskResponse) throws Exception {
			if(isTenuredTasksResponse(taskResponse)) {
				LOG.info("Recovering task in-mem structures...");
				int jobId = taskResponse.getIntValue(ScheduledConstants.JOB_ID);
				int taskId = taskResponse.getIntValue(ScheduledConstants.TASK_ID);
				int serialNo = taskResponse.getIntValue(ScheduledConstants.SERIAL_NO);
				int taskTypeCode = taskResponse.getIntValue(ScheduledConstants.TASK_TYPE);
				TaskType taskType = TaskType.fromCode(taskTypeCode).get();
				TaskID id = new TaskID(jobId, taskId, serialNo, taskType);
				String oldPlatformId = taskResponse.getString(ScheduledConstants.PLATFORM_ID);
				// check job status in Redis queue
				Job ujob = jobPersistenceService.retrieveJob(jobId);
				if(ujob != null) {
					String queue = queueingManager.getQueueName(ujob.getJobType());
					JSONObject job = getRedisJob(queue, jobId);
					if(job != null) {
						int taskCount = job.getIntValue(ScheduledConstants.TASK_COUNT);
						String queuedJobStatus = job.getString(ScheduledConstants.JOB_STATUS);
						if(JobStatus.RUNNING == JobStatus.valueOf(queuedJobStatus)) {
							job.put(ScheduledConstants.LAST_UPDATE_TS, Time.now());
							// update last update timestamp at once to avoid being purged
							updateRedisState(jobId, queue, job);
							
							// rebuild job in-memory structure
							JobInfo jobInfo = new JobInfo(jobId, queue, taskCount);
							jobInfo.jobStatus = JobStatus.RUNNING;
							jobInfo.lastUpdatedTime = Time.now();
							logInMemoryJobStateChanged(jobInfo);
							runningJobIdToInfos.putIfAbsent(jobId, jobInfo);
							
							final TaskInfo taskInfo = new TaskInfo(id);
							taskInfo.platformId = oldPlatformId;
							taskInfo.lastUpdatedTime = Time.now();
							taskInfo.taskStatus = TaskStatus.RUNNING;
							runningTaskIdToInfos.putIfAbsent(id, taskInfo);
							
							LinkedList<TaskID> tasks = runningJobToTaskList.get(jobId);
							if(tasks == null) {
								tasks = Lists.newLinkedList();
								runningJobToTaskList.putIfAbsent(id.jobId, tasks);
							}
							tasks.add(id);
							LOG.info("Task in-mem structures recovered: jobId=" + jobId + 
									", taskCount=" + taskCount + ", jobStatus=" + jobInfo.jobStatus + 
									", taskId=" + id.taskId + ", serialNo=" + id.serialNo + ", taskStatus=" + taskInfo.taskStatus);
						}
					} else {
						LOG.warn("Task timeout: " + taskResponse);
					}
				}
			}
		}
		
		@Override
		public void releaseResource(final String queue, TaskType taskType, boolean isTenuredTaskResponse) {
			// if a fresh task response, operate resource counter
			if(!isTenuredTaskResponse) {
				releaseResource(queue, taskType);
			}
		}
		
		@Override
		public void releaseResource(final String queue, TaskType taskType) {
			resourceMetadataManager.releaseResource(queue, taskType);
			resourceMetadataManager.currentResourceStatuses();
			logTaskCounterChanged(queue);
		}

	}
	
	private JSONObject buildTaskMessage(String platformId, Task task, int taskCount) {
		JSONObject taskMessage = new JSONObject(true);
		taskMessage.put(ScheduledConstants.JOB_ID, task.getJobId());
    	taskMessage.put(ScheduledConstants.PLATFORM_ID, platformId);
    	taskMessage.put(ScheduledConstants.TASK_ID, task.getId());
    	taskMessage.put(ScheduledConstants.ROLE, task.getTaskType());
        taskMessage.put(ScheduledConstants.SERIAL_NO, task.getSerialNo());
        taskMessage.put(ScheduledConstants.TASK_COUNT, taskCount);
        taskMessage.put(ScheduledConstants.PARAMS, task.getParams());
        return taskMessage;
    }
	
	interface TasksResponseHandler<T> {
		void handle(final T tasksResponse);
	}
	
	private final class FreshHeartbeatHandler implements TasksResponseHandler<Heartbeat> {

		@Override
		public void handle(final Heartbeat hb) {
			LOG.info("Prepare to handle heartbeat: " + hb);
			JSONArray tasks = hb.heartbeatMessage.getJSONArray(ScheduledConstants.TASKS);
			String platformId = hb.heartbeatMessage.getString(ScheduledConstants.PLATFORM_ID);
			boolean isTenuredTasksResponse = taskResponseHandlingController.isTenuredTasksResponse(hb.heartbeatMessage);
			final Channel channel = hb.channel;
			long deliveryTag = hb.deliveryTag;
			for (int i = 0; i < tasks.size(); i++) {
				JSONObject taskResponse = (JSONObject) tasks.get(i);
				// read heartbeat message contents
				try {
					handleSingleTaskResponse(taskResponse, platformId, isTenuredTasksResponse, channel, deliveryTag);
				} catch (Exception e) {
					LOG.warn("Fail to handle single task response: ", e);
				}
			}			
		}

		private void handleSingleTaskResponse(JSONObject taskResponse, String platformId,
				boolean isTenuredTasksResponse, final Channel channel, long deliveryTag) throws Exception {
			int jobId = taskResponse.getIntValue(ScheduledConstants.JOB_ID);
			int serialNo = taskResponse.getIntValue(ScheduledConstants.SERIAL_NO);
			int taskTypeCode = taskResponse.getIntValue(ScheduledConstants.TASK_TYPE);
			int taskId = taskResponse.getIntValue(ScheduledConstants.TASK_ID);
			String status = taskResponse.getString(ScheduledConstants.STATUS);
			TaskStatus newTaskStatus = TaskStatus.valueOf(status);
			TaskType taskType = TaskType.fromCode(taskTypeCode).get();
			final TaskID id = new TaskID(jobId, taskId, serialNo, taskType);
			taskResponse.put(ScheduledConstants.PLATFORM_ID, platformId);
			LOG.info("Process task response: " + taskResponse);
			
			// recover memory structures for the tenured task, if necessary
			taskResponseHandlingController.recoverTaskInMemoryStructures(taskResponse);
			
			TaskInfo taskInfo = runningTaskIdToInfos.get(id);
			JobInfo jobInfo = runningJobIdToInfos.get(jobId);
			LOG.info("Get in-memory infos: taskInfo=" + taskInfo + ", jobInfo=" + jobInfo);
			
			if(jobInfo != null) {
				try {
					jobInfo.inMemJobUpdateLock.lock();
					
					// lock obtained, should check in-mem job status, because maybe
					// stale task checker has already update this job to timeout(actually FAILED)
					if(jobInfo.jobStatus == JobStatus.RUNNING) {
						if(newTaskStatus == taskInfo.taskStatus) {
							long now = Time.now();
							jobInfo.lastUpdatedTime = now;
							taskInfo.lastUpdatedTime = now;
						} else {
							final Optional<String> q = getJobQueueByJobId(jobId);
							q.ifPresent(queue -> {
								LOG.debug("START to handle task result: " + taskResponse);
								
								for (int i = 0; i < 3; i++) {
									try {
										processTaskResponse(jobInfo, id, newTaskStatus, taskResponse, isTenuredTasksResponse);
										break;
									} catch (Exception e) {
										LOG.warn("Fail to process task result: ", e);
										try {
											Thread.sleep((i+1) * 1000);
										} catch (Exception e1) {}
										LOG.warn("Retry to process task result: int i=" + (i+1) + ", taskResponse=" + taskResponse);
										continue;
									}
								}
							});
						}
						
						// Ack this message, remove heartbeat message from MQ
						sendAck(channel, deliveryTag);
					}
				} finally {
					jobInfo.inMemJobUpdateLock.unlock();
				}
			}
		}
		
		private void processTaskResponse(JobInfo jobInfo, final TaskID id, TaskStatus taskStatus, JSONObject taskResponse, boolean isTenuredTasksResponse) throws Exception {
			LOG.debug("Process task result: " + taskResponse);
			final String queue = jobInfo.queue;
			switch(taskStatus) {
				case SUCCEEDED:
					LOG.info("Task succeeded: " + taskResponse);
					taskResponseHandlingController.releaseResource(queue, id.taskType, isTenuredTasksResponse);
					updateTaskInfo(id, taskStatus, taskResponse);
					updateJobStatus(jobInfo, id, taskStatus);
					
					taskResponseHandlingController.incrementSucceededTaskCount(queue, taskResponse);
					logTaskCounterChanged(queue);
					break;
				case FAILED:
					LOG.info("Task failed: " + taskResponse);
					taskResponseHandlingController.releaseResource(queue, id.taskType, isTenuredTasksResponse);
					updateTaskInfo(id, taskStatus, taskResponse);
					updateJobStatus(jobInfo, id, taskStatus);
					
					taskResponseHandlingController.incrementFailedTaskCount(queue, taskResponse);
					logTaskCounterChanged(queue);
					break;
				case RUNNING:
					LOG.info("Task running: " + taskResponse);
					TaskInfo taskInfo = runningTaskIdToInfos.get(id);
					taskInfo.lastUpdatedTime = Time.now();
					logInMemoryStateChanges(jobInfo, taskInfo);
					break;
				default:
					LOG.warn("Unknown state for task: state=" + taskStatus + ", task=" + id);						
			}
		}
		
	}
	
	private void logTaskCounterChanged(final String queue) {
		JSONObject stat = resourceMetadataManager.getTaskStatCounter(queue).toJSONObject();
		stat.put("runningTaskCount", resourceMetadataManager.getRunningTaskCount(queue));
		LOG.info("Task counter changed: queue=" + queue + ", counter=" + stat.toJSONString());
	}
	
	/**
	 * Handle task responses with a valid MQ channel. 
	 * In this case scheduling platform is started, but the tenured task responses are not published
	 * to MQ by running platform.
	 * 
	 * @author yanjun
	 */
	private final class TenuredInflightTaskResponseHandler implements TasksResponseHandler<Heartbeat> {

		@Override
		public void handle(Heartbeat taskResponse) {
			freshTasksResponseProcessingManager.handler.handle(taskResponse);
		}
		
	}

	/**
	 * Handle task responses with an invalid MQ channel.
	 * After scheduling platform is started, tenured task responses in MQ should be recovered.
	 * 
	 * @author yanjun
	 */
	private final class TenuredInMQTaskResponseHandler implements TasksResponseHandler<JSONObject> {

		@Override
		public void handle(JSONObject tasksResponse) {
			JSONObject response = new JSONObject();
			response.put(JSONKeys.TYPE, ScheduledConstants.HEARTBEAT_TYPE_TASK_PROGRESS);
			JSONArray tasks = new JSONArray();
			tasks.add(tasksResponse);
			response.put(ScheduledConstants.TASKS, tasks);
			Heartbeat hb = new Heartbeat(response);
			freshTasksResponseProcessingManager.handler.handle(hb);
		}
		
	}
	
	private JobQueueingService getJobQueueingService(String queue) {
		QueueingContext qc = queueingManager.getQueueingContext(queue);
		return qc.getJobQueueingService();
	}
	
	Optional<String> getJobQueueByJobId(int jobId) {
		JobInfo ji = runningJobIdToInfos.get(jobId);
		if(ji == null) {
			return Optional.empty();
		}
		return Optional.of(ji.queue);
	}
	
	private void updateTaskInfo(TaskID id, TaskStatus taskStatus, JSONObject taskResponse) {
		Integer resultCount = taskResponse.getInteger(ScheduledConstants.RESULT_COUNT);;
		if(resultCount != null) {
			Timestamp updateTime = new Timestamp(Time.now());
			Task task = new Task();
			task.setId(id.taskId);
			task.setStatus(taskStatus.getCode());
			task.setDoneTime(updateTime);
			if(resultCount != null) {
				task.setResultCount(resultCount);
			}
			taskPersistenceService.updateTaskByID(task);
			LOG.info("In-db task state changed: jobId=" + id.jobId + ", taskId=" + id.taskId + ", serialNo=" + id.serialNo + 
					", resultCount=" + resultCount + ", updateTime=" + updateTime + ", taskStatus=" + taskStatus);
		}
	}
	
	void updateTaskInfo(int jobId, int taskId, TaskStatus taskStatus) {
		Timestamp updateTime = new Timestamp(Time.now());
		Task userTask = new Task();
		userTask.setId(taskId);
		userTask.setStatus(taskStatus.getCode());
		userTask.setDoneTime(updateTime);
		taskPersistenceService.updateTaskByID(userTask);
		LOG.info("In-db task state changed: jobId=" + jobId + ", taskId=" + taskId + 
				", updateTime=" + updateTime + ", taskStatus=" + taskStatus);
	}

	void updateTaskInfo(TaskID id, TaskStatus taskStatus) {
		updateTaskInfo(id.jobId, id.taskId, taskStatus);
	}
	
	void updateTaskInfo(int jobId, int taskId, int serialNo, TaskStatus taskStatus) {
		Timestamp updateTime = new Timestamp(Time.now());
		Task task = new Task();
		task.setId(taskId);
		task.setStatus(taskStatus.getCode());
		task.setDoneTime(updateTime);
		taskPersistenceService.updateTaskByID(task);
		LOG.info("In-db task state changed: jobId=" + jobId + ", taskId=" + taskId + ", serialNo=" + serialNo + 
				", updateTime=" + updateTime + ", taskStatus=" + taskStatus);
	}
	
	private void updateTaskStatusWithoutTime(int jobId, int taskId, int serialNo, TaskStatus taskStatus) {
		Task task = new Task();
		task.setId(taskId);
		task.setStatus(taskStatus.getCode());
		taskPersistenceService.updateTaskByID(task);
		LOG.info("In-db task state changed: jobId=" +
				jobId + ", taskId=" + taskId + ", serialNo=" + serialNo + ", taskStatus=" + taskStatus);
	}
	
	private void updateStartedTask(int jobId, int taskId, int serialNo, TaskStatus taskStatus) {
		Timestamp updateTime = new Timestamp(Time.now());
		Task task = new Task();
		task.setId(taskId);
		task.setStartTime(updateTime);
		task.setDoneTime(updateTime);
		task.setStatus(taskStatus.getCode());
		taskPersistenceService.updateTaskByID(task);
		LOG.info("In-db task state changed: jobId=" + jobId + ", taskId=" + taskId + ", serialNo=" + serialNo + 
				", startTime=" + updateTime + ", updateTime=" + updateTime + ", taskStatus=" + taskStatus);
	}
	
	void updateJobInfo(int jobId, JobStatus jobStatus) {
		Timestamp updateTime = new Timestamp(Time.now());
		Job job = new Job();
		job.setId(jobId);
		job.setStatus(jobStatus.getCode());
		job.setDoneTime(updateTime);
		jobPersistenceService.updateJobByID(job);
		LOG.info("In-db job state changed: jobId=" + jobId + ", updateTime=" + updateTime + ", jobStatus=" + jobStatus);
	}
	
	private void updateJobInfo(int jobId, long lastUpdatedTime) {
		Timestamp updateTime = new Timestamp(lastUpdatedTime);
		Job job = new Job();
		job.setId(jobId);
		job.setDoneTime(updateTime);
		jobPersistenceService.updateJobByID(job);
		LOG.info("In-db job state changed: jobId=" + jobId + ", updateTime=" + updateTime);
	}
	
	private void updateJobStatus(JobInfo jobInfo, TaskID id, TaskStatus taskStatus) throws Exception {
		int jobId = jobInfo.jobId;
		String queue = jobInfo.queue;
		int taskCount = jobInfo.taskCount;
		LinkedList<TaskID> tasks = runningJobToTaskList.get(jobId);
		
		TaskStatus status = taskStatus;
		JobStatus jobStatus = jobInfo.jobStatus;
		if(shouldCancelJob(jobId)) {
			status = TaskStatus.CANCELLED;
			jobStatus = JobStatus.CANCELLED;
		}
		
		// the last task was returned back
		if(taskCount == tasks.size()) {
			boolean isSuccess = false;
			if(status == TaskStatus.SUCCEEDED) {
				isSuccess = true;
			}
			jobStatus = isSuccess ? JobStatus.SUCCEEDED : jobStatus;
			removeRedisJob(queue, jobId);
			
			updateJobInfo(jobId, jobStatus);
			
			jobInfo.lastUpdatedTime = Time.now();
			jobInfo.jobStatus = jobStatus;
			logInMemoryJobStateChanged(jobInfo);
		} else {
			// non-last task
			JSONObject queuedJob = getRedisJob(queue, jobId);
			TaskInfo taskInfo = runningTaskIdToInfos.get(id);
			taskInfo.lastUpdatedTime = Time.now();
			if(status == TaskStatus.SUCCEEDED) {
				updateJobInfo(jobId, taskInfo.lastUpdatedTime);
				
				// task was succeeded, update job status to QUEUEING in Redis queue to make next task be scheduled
				queuedJob.put(ScheduledConstants.JOB_STATUS, JobStatus.QUEUEING.toString());
				queuedJob.put(ScheduledConstants.TASK_STATUS, TaskStatus.SUCCEEDED.toString());
				queuedJob.put(ScheduledConstants.LAST_UPDATE_TS, Time.now());
				updateRedisState(jobId, queue, queuedJob);
				
				// update in-memory task status
				taskInfo.taskStatus = status;
				logInMemoryStateChanges(jobInfo, taskInfo);
			} else {
				// task was failed/cancelled, remove from Redis queue, and update job status to FAILED in job database.
				// TODO later maybe we should give it a flexible policy, such as giving a chance to be rescheduled.
				removeRedisJob(queue, jobId);
				
				updateJobInfo(jobId, jobStatus);
				
				// update in-memroy job/task status
				jobInfo.jobStatus = jobStatus;
				taskInfo.taskStatus = status;
				logInMemoryStateChanges(jobInfo, taskInfo);
			}
		}
		
		if(shouldCancelJob(jobId)) {
			jobCancelled(jobId, () -> {
				// do nothing
			}); 
		}
		
		// handle in-memory completed task
		handleInMemoryCompletedTask(id);
		
		updateJobStatCounter(queue, jobStatus);
	}
	
	void updateJobStatCounter(String queue, JobStatus jobStatus) {
		final JobStatCounter jobStatCounter = resourceMetadataManager.getJobStatCounter(queue);
		switch(jobStatus) {
			case SUCCEEDED:
				jobStatCounter.incrementSucceededJobCount();
				break;
			case FAILED:
				jobStatCounter.incrementFailedJobCount();
				break;
			case CANCELLED:
				jobStatCounter.incrementCancelledJobCount();
				break;
			case TIMEOUT:
				jobStatCounter.incrementTimeoutJobCount();
				break;
			default:
		}
		LOG.info("Job counter changed: queue=" + queue + ", counter=" + jobStatCounter.toJSONObject());
	}

	void handleInMemoryCompletedTask(int jobId) {
		JobInfo current = runningJobIdToInfos.get(jobId);
		if(current != null && (current.jobStatus == JobStatus.SUCCEEDED 
				|| current.jobStatus == JobStatus.FAILED 
				|| current.jobStatus == JobStatus.TIMEOUT 
				|| current.jobStatus == JobStatus.CANCELLED)) {
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
				.sorted((x, y) -> x.lastUpdatedTime - y.lastUpdatedTime < 0 ? -1 : 1)
				.limit(keptHistoryJobMaxCount)
				.forEach(ti -> {
					JobInfo ji = completedJobIdToInfos.remove(ti.jobId);
					LOG.info("In-mem job removed: " + ji);
				});
			}
		}
	}
	
	void handleInMemoryCompletedTask(TaskID id) {
		if(id != null) {
			// remove job/task from running queue
			handleInMemoryCompletedTask(id.jobId);
		}
	}

	void updateRedisState(int jobId, final String queue, final JSONObject job) throws Exception {
		JobQueueingService qs = getJobQueueingService(queue);
		qs.updateQueuedJob(jobId, job);
		LOG.info("Redis state changed: queue=" + qs.getQueueName() + ", job=" + job);
	}

	private void logInMemoryStateChanges(final JobInfo jobInfo, final TaskInfo taskInfo) {
		LOG.info("In-memory state changed: job=" + jobInfo + ", task=" + taskInfo); 
	}
	
	private void logInMemoryJobStateChanged(final JobInfo jobInfo) {
		LOG.info("In-memory job state changed: job=" + jobInfo);
	}
	
	void releaseResource(String queue, TaskType taskType) {
		taskResponseHandlingController.releaseResource(queue, taskType);
	}
	
	void incrementTimeoutTaskCount(String queue) {
		resourceMetadataManager.getTaskStatCounter(queue).incrementTimeoutTaskCount();
	}
	
	List<Job> getJobByState(JobStatus jobStatus) {
		return jobPersistenceService.getJobByState(jobStatus);
	}
	
	List<Task> getTasksFor(int jobId) {
		return taskPersistenceService.getTasksFor(jobId);
	}
	
	/**
	 * Listen the arrival of heartbeat messages in the queue of rabbit MQ, and decides whether a timeout
	 * task never return, whether updating the in-mem task information or not.
	 * 
	 * @author yanjun
	 */
	private final class HeartbeatConsumer extends AbstractRunnableConsumer {
		
		public HeartbeatConsumer(String queueName, Channel channel) {
			super(queueName, channel);
		}

		@Override
		public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
				throws IOException {
			if(body != null) {
				try {
					// resolve the returned message
					long deliveryTag = envelope.getDeliveryTag();
						String message = new String(body);
						LOG.info("Message received: deliveryTag=" + deliveryTag + ", message=" + message);

						JSONObject heartbeatMessage = JSONObject.parseObject(message);
						if(heartbeatMessage.containsKey(JSONKeys.TYPE)) {
							String type = heartbeatMessage.getString(JSONKeys.TYPE);
							switch(type) {
								case ScheduledConstants.HEARTBEAT_TYPE_TASK_PROGRESS:
									if(taskResponseHandlingController.isValid(heartbeatMessage)) {
										Heartbeat hb = new Heartbeat(getChannel(), heartbeatMessage, deliveryTag);
										rawHeartbeatMessages.add(hb);
										LOG.info("Added to rawHeartbeatMessages: " + hb);
									} else {
										LOG.warn("Invalid tasks response: " + heartbeatMessage);
										sendAck(getChannel(), deliveryTag);
									}
									break;
								default:
									LOG.warn("Unknown heartbeat: type=" + type + ", heartbeat=" + heartbeatMessage);
							}
					} else {
						// Ack unknown message
						sendAck(getChannel(), deliveryTag);
					}
				} catch (Exception e) {
					LOG.warn("Fail to consume message: ", e);
				}
			}
			
		}
		
	}
	
	private boolean sendAck(Channel channel, long deliveryTag) {
		try {
			if(channel != null && channel.isOpen()) {
				channel.basicAck(deliveryTag, false);
			}
		} catch (Exception e) {
			LOG.warn("Fail to send ack: deliveryTag=" + deliveryTag, e);
			return false;
		}
		return true;
	}
	
	private class Heartbeat {
		
		Channel channel;
		final JSONObject heartbeatMessage;
		long deliveryTag;
		
		public Heartbeat(JSONObject heartbeatMessage) {
			super();
			this.heartbeatMessage = heartbeatMessage;
		}
		
		public Heartbeat(Channel channel, JSONObject heartbeat, long deliveryTag) {
			super();
			this.channel = channel;
			this.heartbeatMessage = heartbeat;
			this.deliveryTag = deliveryTag;
		}
		
		@Override
		public String toString() {
			return "deliveryTag=" + deliveryTag + ", heartbeatMessage=" + heartbeatMessage.clone() + ", channel=" + channel;
		}
	}
	
	class JobInfo {
		
		final int jobId;
		final String queue;
		final int taskCount;
		volatile JobStatus jobStatus;
		volatile long lastUpdatedTime;
		final Lock inMemJobUpdateLock = new ReentrantLock();
		
		public JobInfo(int jobId, String queue, int taskCount) {
			super();
			this.jobId = jobId;
			this.queue = queue;
			this.taskCount = taskCount;
			lastUpdatedTime = Time.now();
		}
		
		@Override
		public int hashCode() {
			return 31 * jobId + 31 * queue.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			JobInfo other = (JobInfo) obj;
			return this.jobId == other.jobId && this.queue.equals(other.queue);
		}
		
		@Override
		public String toString() {
			final JSONObject description = new JSONObject(true);
			description.put(ScheduledConstants.JOB_ID, jobId);
			description.put(ScheduledConstants.QUEUE, queue);
			description.put(ScheduledConstants.TASK_COUNT, taskCount);
			description.put(ScheduledConstants.JOB_STATUS, jobStatus);
			description.put(ScheduledConstants.LAST_UPDATE_TS, lastUpdatedTime);
			return description.toString();
		}
	}
	
	class TaskID {
		
		final int jobId;
		final int taskId;
		final int serialNo;
		final TaskType taskType;
		
		public TaskID(int jobId, int taskId, int serialNo, TaskType taskType) {
			super();
			this.jobId = jobId;
			this.serialNo = serialNo;
			this.taskType = taskType;
			this.taskId = taskId;
		}
		
		@Override
		public int hashCode() {
			return 31 * taskId;
		}
		
		@Override
		public boolean equals(Object obj) {
			TaskID other = (TaskID) obj;
			return this.taskId == other.taskId;
		}
		
		protected JSONObject toJSONObject(){
			final JSONObject description = new JSONObject(true);
			description.put(ScheduledConstants.JOB_ID, jobId);
			description.put(ScheduledConstants.TASK_ID, taskId);
			description.put(ScheduledConstants.SERIAL_NO, serialNo);
			description.put(ScheduledConstants.TASK_TYPE, taskType);
			return description;
		}

		@Override
		public String toString(){
			return toJSONObject().toString();
		}
		
	}
	
	class TaskInfo extends TaskID {
		
		long scheduledTime;
		volatile long lastUpdatedTime;
		volatile TaskStatus taskStatus;
		String platformId;
		final TaskID id;
		
		public TaskInfo(TaskID id) {
			super(id.jobId, id.taskId, id.serialNo, id.taskType);
			this.id = id;
		}
		
		@Override
		protected JSONObject toJSONObject() {
			final JSONObject description = super.toJSONObject();
			description.put(ScheduledConstants.PLATFORM_ID, platformId);
			description.put(ScheduledConstants.TASK_STATUS, taskStatus);
			description.put(ScheduledConstants.LAST_UPDATE_TS, lastUpdatedTime);
			return description;
		}
		
		@Override
		public String toString() {
			return toJSONObject().toString();
		}
	}

}
