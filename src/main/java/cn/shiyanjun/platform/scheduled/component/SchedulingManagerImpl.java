package cn.shiyanjun.platform.scheduled.component;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Throwables;
import com.google.common.collect.Queues;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

import cn.shiyanjun.platform.api.constants.JSONKeys;
import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.api.utils.NamedThreadFactory;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.HeartbeatHandlingController;
import cn.shiyanjun.platform.scheduled.api.MQAccessService;
import cn.shiyanjun.platform.scheduled.api.RecoveryManager;
import cn.shiyanjun.platform.scheduled.api.ResourceManager;
import cn.shiyanjun.platform.scheduled.api.ResponseHandler;
import cn.shiyanjun.platform.scheduled.api.ScheduledController;
import cn.shiyanjun.platform.scheduled.api.SchedulingManager;
import cn.shiyanjun.platform.scheduled.api.SchedulingPolicy;
import cn.shiyanjun.platform.scheduled.api.StateManager;
import cn.shiyanjun.platform.scheduled.common.AbstractRunnableConsumer;
import cn.shiyanjun.platform.scheduled.common.Heartbeat;
import cn.shiyanjun.platform.scheduled.common.JobInfo;
import cn.shiyanjun.platform.scheduled.common.TaskID;
import cn.shiyanjun.platform.scheduled.common.TaskInfo;
import cn.shiyanjun.platform.scheduled.common.TaskOrder;
import cn.shiyanjun.platform.scheduled.component.ResourceManagerImpl.JobStatCounter;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
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
public class SchedulingManagerImpl implements SchedulingManager {

	private static final Log LOG = LogFactory.getLog(SchedulingManagerImpl.class);
	protected final ComponentManager componentManager;
	private ExecutorService executorService;
	private ScheduledExecutorService scheduledExecutorService;
	private final MQAccessService taskMQAccessService;
	private final MQAccessService heartbeatMQAccessService;
	private volatile boolean running = true;
	private SchedulingPolicy schedulingPolicy;
	private RecoveryManager recoveryManager;
	private final ResourceManager resourceManager;
	private final FreshTasksResponseProcessingManager freshTasksResponseProcessingManager;
	private final BlockingQueue<Heartbeat> rawHeartbeatMessages = Queues.newLinkedBlockingQueue();
	private final StaleJobChecker staleJobChecker;
	private StateManager stateManager;
	private HeartbeatHandlingController taskResponseHandlingController;
	private ScheduledController scheduledController;

	public SchedulingManagerImpl(ComponentManager componentManager) {
		super();
		this.componentManager = componentManager;
		taskMQAccessService = this.componentManager.getTaskMQAccessService();
		heartbeatMQAccessService = this.componentManager.getHeartbeatMQAccessService();
		resourceManager = this.componentManager.getResourceManager();
		freshTasksResponseProcessingManager = new FreshTasksResponseProcessingManager();
		recoveryManager = new RecoveryManagerImpl(componentManager);
		staleJobChecker = new StaleJobChecker(this);
	}
	
	@Override
	public void start() {
		schedulingPolicy = new MaxConcurrencySchedulingPolicy(componentManager);
		stateManager = componentManager.getStateManager();
		taskResponseHandlingController = new SimpleTaskResponseHandlingController(componentManager);
		scheduledController = componentManager.getScheduledController();
		
		// recover task responses
		try {
			recoveryManager.recover();
		} catch (Exception e) {
			LOG.fatal("Fail to recover:", e);
			Throwables.propagate(e);
		}
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
		final int staleTaskCheckIntervalSecs = componentManager.getContext().getInt(ConfigKeys.SCHEDULED_STALE_JOB_CHECK_INTERVAL_SECS, 900);
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
	
	/**
	 * Schedules a prepared task, and publish it to the MQ
	 * 
	 * @author yanjun
	 */
	private class SchedulingThread extends Thread {
		
		private final int scheduleTaskIntervalMillis = 5000;
		
		public SchedulingThread() {
			super();
		}
		
		@Override
		public void run() {
			while(running) {
				try {
					// for each job in Redis queue
					Optional<TaskOrder> offeredTask = schedulingPolicy.offerTask();
					offeredTask.ifPresent(taskOrder -> {
						String queue = taskOrder.getQueue();
						TaskType taskType = TaskType.fromCode(taskOrder.getTask().getTaskType()).get();
						int jobId = taskOrder.getTask().getJobId();
						int taskId = taskOrder.getTask().getId();
						int seqNo = taskOrder.getTask().getSeqNo();
						
						if(scheduledController.shouldCancelJob(jobId)) {
							// cancel a running job
							scheduledController.jobCancelled(jobId, () -> {
								try {
									stateManager.updateJobStatus(jobId, JobStatus.CANCELLED);
									stateManager.removeQueuedJob(queue, jobId);
								} finally {
									releaseResource(queue, new TaskID(jobId, taskId, seqNo, taskType));
								}
							});
						} else {
							stateManager.registerRunningJob(taskOrder);
							LOG.info("Prepare to schedule: queue=" + queue + ", taskType=" + taskType + ", " + taskOrder);
							
							stateManager.getRunningJob(jobId)
								.ifPresent(job -> scheduleTask(queue, taskType, taskOrder, job));
						}
					});
					
					// wait for available resource being released
					if(!offeredTask.isPresent()) {
						Thread.sleep(scheduleTaskIntervalMillis);
					}
				} catch (Exception e) {
					LOG.warn("Fail to schedule task: ", e);
				}
			}
		}
		
		private void scheduleTask(String queue, TaskType taskType, TaskOrder scheduledTask, JobInfo jobInfo) {
			int jobId = scheduledTask.getTask().getJobId();
			int taskId = scheduledTask.getTask().getId();
			int seqNo = scheduledTask.getTask().getSeqNo();
			int taskCount = scheduledTask.getTaskCount();
			boolean alreadyPublished = false;
			TaskID id = null;
			try {
				// update task/job status
				id = new TaskID(jobId, taskId, seqNo, taskType);
				stateManager.registerRunningTask(id, componentManager.getPlatformId());
				
				try {
					// here we should update some job/task statuses after message is published to rabbit MQ,
					// and lock the jobInfo to assure the time of these updates are ahead of returned task result processing
					jobInfo.lock();
					
					// publish to the MQ
					JSONObject taskMessage = buildTaskMessage(componentManager.getPlatformId(), scheduledTask.getTask(), taskCount);
					alreadyPublished = taskMQAccessService.produceMessage(taskMessage.toJSONString());
					
					stateManager.taskPublished(id);
					
					logTaskCounterChanged(queue);
				} finally {
					jobInfo.unlock();
				}
			} catch(Exception e) {
				LOG.error("Fail to schedule task: jobId=" + jobId + ", taskId=" + taskId + ", seqNo=" + seqNo, e);
				if(!alreadyPublished) {
					releaseResource(queue, id);
				}
				
				// update job status
				stateManager.updateJobStatus(jobId, JobStatus.FAILED);
				
				// handle in-memory completed task
				stateManager.handleInMemoryCompletedJob(id);
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
					if(taskResponseHandlingController.isTenuredTasksResponse(rawTasksResponse.getMessage())) {
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
	 * 		{"type":"taskProgress","tasks":[{"result":{"resultCount":6910757,"status":5},"jobId":1328,"taskType":2,"taskId":3404,"seqNo":3,"status":"SUCCEEDED"}]}</br>
	 * 		</pre>
	 * </ul>
	 * 
	 * @author yanjun
	 */
	final class FreshTasksResponseProcessingManager implements Runnable {
		
		final BlockingQueue<Heartbeat> tasksResponseMessageQueue = Queues.newLinkedBlockingQueue();
		final ResponseHandler<Heartbeat> handler = new FreshHeartbeatHandler();
		
		@Override
		public void run() {
			while(running) {
				try {
					Heartbeat tasksResponse = tasksResponseMessageQueue.take();
					LOG.info("Takes a fresh tasks response: " + tasksResponse.getMessage());
					handler.handle(tasksResponse);
				} catch (Exception e) {
					LOG.warn("Fail to process heartbeat: ", e);
				}
			}
		}
		
	}
	
	private JSONObject buildTaskMessage(String platformId, Task task, int taskCount) {
		JSONObject taskMessage = new JSONObject(true);
		taskMessage.put(ScheduledConstants.JOB_ID, task.getJobId());
    	taskMessage.put(ScheduledConstants.PLATFORM_ID, platformId);
    	taskMessage.put(ScheduledConstants.TASK_ID, task.getId());
    	taskMessage.put(ScheduledConstants.ROLE, task.getTaskType());
        taskMessage.put(ScheduledConstants.SEQ_NO, task.getSeqNo());
        taskMessage.put(ScheduledConstants.TASK_COUNT, taskCount);
        taskMessage.put(ScheduledConstants.PARAMS, task.getParams());
        return taskMessage;
    }
	
	private final class FreshHeartbeatHandler implements ResponseHandler<Heartbeat> {

		@Override
		public void handle(final Heartbeat hb) {
			LOG.info("Prepare to handle heartbeat: " + hb);
			JSONObject message = hb.getMessage();
			JSONArray tasks = message.getJSONArray(ScheduledConstants.TASKS);
			String platformId = message.getString(ScheduledConstants.PLATFORM_ID);
			boolean isTenuredTasksResponse = taskResponseHandlingController.isTenuredTasksResponse(message);
			final Channel channel = hb.getChannel();
			long deliveryTag = hb.getDeliveryTag();
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
			int seqNo = taskResponse.getIntValue(ScheduledConstants.SEQ_NO);
			int taskTypeCode = taskResponse.getIntValue(ScheduledConstants.TASK_TYPE);
			int taskId = taskResponse.getIntValue(ScheduledConstants.TASK_ID);
			String status = taskResponse.getString(ScheduledConstants.STATUS);
			TaskStatus newTaskStatus = TaskStatus.valueOf(status);
			TaskType taskType = TaskType.fromCode(taskTypeCode).get();
			final TaskID id = new TaskID(jobId, taskId, seqNo, taskType);
			taskResponse.put(ScheduledConstants.PLATFORM_ID, platformId);
			LOG.info("Process task response: " + taskResponse);
			
			// recover memory structures for the tenured task, if necessary
			taskResponseHandlingController.recoverTaskInMemoryStructures(taskResponse);
			
			Optional<TaskInfo> taskInfo = stateManager.getRunningTask(id);
			Optional<JobInfo> jobInfo = stateManager.getRunningJob(jobId);
			LOG.info("Get in-memory infos: taskInfo=" + taskInfo + ", jobInfo=" + jobInfo);
			
			jobInfo.ifPresent(ji -> {
				try {
					ji.lock();
					
					// lock obtained, should check in-mem job status, because maybe
					// stale task checker has already update this job to timeout(actually FAILED)
					if(ji.getJobStatus() == JobStatus.RUNNING) {
						if(newTaskStatus == taskInfo.get().getTaskStatus()) {
							long now = Time.now();
							ji.setLastUpdatedTime(now);
							taskInfo.get().setLastUpdatedTime(now);
						} else {
							final Optional<String> q = stateManager.getRunningJobQueueName(jobId);
							q.ifPresent(queue -> {
								LOG.debug("START to handle task result: " + taskResponse);
								
								for (int i = 0; i < 3; i++) {
									try {
										processTaskResponse(ji, id, newTaskStatus, taskResponse, isTenuredTasksResponse);
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
					ji.unlock();
				}
			});
		}
		
		private void processTaskResponse(JobInfo jobInfo, final TaskID id, TaskStatus taskStatus, JSONObject taskResponse, boolean isTenuredTasksResponse) throws Exception {
			LOG.debug("Process task result: " + taskResponse);
			final String queue = jobInfo.getQueue();
			switch(taskStatus) {
				case SUCCEEDED:
					LOG.info("Task succeeded: " + taskResponse);
					taskResponseHandlingController.releaseResource(queue, id, isTenuredTasksResponse);
					stateManager.updateTaskStatus(id.getTaskId(), taskStatus, 
							Optional.ofNullable(taskResponse.getInteger(ScheduledConstants.RESULT_COUNT)));
					processJobStatus(jobInfo, id, taskStatus);
					
					taskResponseHandlingController.incrementSucceededTaskCount(queue, taskResponse);
					logTaskCounterChanged(queue);
					break;
				case FAILED:
					LOG.info("Task failed: " + taskResponse);
					taskResponseHandlingController.releaseResource(queue, id, isTenuredTasksResponse);
					stateManager.updateTaskStatus(id.getTaskId(), taskStatus);
					processJobStatus(jobInfo, id, taskStatus);
					
					taskResponseHandlingController.incrementFailedTaskCount(queue, taskResponse);
					logTaskCounterChanged(queue);
					break;
				case RUNNING:
					LOG.info("Task running: " + taskResponse);
					Optional<TaskInfo> taskInfo = stateManager.getRunningTask(id);
					taskInfo.ifPresent(ti -> {
						ti.setLastUpdatedTime(Time.now());
					});
					break;
				default:
					LOG.warn("Unknown state for task: state=" + taskStatus + ", task=" + id);						
			}
		}
		
	}
	
	void logTaskCounterChanged(final String queue) {
		JSONObject stat = resourceManager.getTaskStatCounter(queue).toJSONObject();
		stat.put("runningTaskCount", resourceManager.getRunningTaskCount(queue));
		LOG.info("Task counter changed: queue=" + queue + ", counter=" + stat.toJSONString());
	}
	
	private void processJobStatus(JobInfo jobInfo, TaskID id, TaskStatus taskStatus) throws Exception {
		int jobId = jobInfo.getJobId();
		String queue = jobInfo.getQueue();
		int taskCount = jobInfo.getTaskCount();
		Collection<TaskInfo> tasks = stateManager.getRunningTasks(jobId);
		
		TaskStatus status = taskStatus;
		JobStatus jobStatus = jobInfo.getJobStatus();
		if(scheduledController.shouldCancelJob(jobId)) {
			status = TaskStatus.CANCELLED;
			jobStatus = JobStatus.CANCELLED;
		}
		
		// the last task was returned back
		if(taskCount == tasks.size()) {
			jobStatus = (status == TaskStatus.SUCCEEDED ? JobStatus.SUCCEEDED : 
				(jobStatus == JobStatus.CANCELLED ? jobStatus : JobStatus.FAILED));
			stateManager.removeQueuedJob(queue, jobId);
			stateManager.updateJobStatus(jobId, jobStatus);
			
			jobInfo.setLastUpdatedTime(Time.now());
			jobInfo.setJobStatus(jobStatus);
			LOG.info("In-memory job state changed: job=" + jobInfo);
		} else {
			// non-last task
			JSONObject queuedJob = stateManager.retrieveQueuedJob(queue, jobId);
			TaskInfo taskInfo = stateManager.getRunningTask(id).get();
			taskInfo.setLastUpdatedTime(Time.now());
			if(status == TaskStatus.SUCCEEDED) {
				stateManager.updateJobStatus(jobId, jobStatus, taskInfo.getLastUpdatedTime());
				
				// task was succeeded, update job status to QUEUEING in Redis queue to make next task be scheduled
				queuedJob.put(ScheduledConstants.JOB_STATUS, JobStatus.QUEUEING.toString());
				queuedJob.put(ScheduledConstants.TASK_STATUS, TaskStatus.SUCCEEDED.toString());
				queuedJob.put(ScheduledConstants.LAST_UPDATE_TS, Time.now());
				stateManager.updateQueuedJob(jobId, queue, queuedJob);
				// update in-memory task status
				taskInfo.setTaskStatus(status);
				logInMemoryStateChanges(jobInfo, taskInfo);
			} else {
				// task was failed/cancelled, remove from Redis queue, and update job status to FAILED in job database.
				// TODO later maybe we should give it a flexible policy, such as giving a chance to be rescheduled.
				stateManager.removeQueuedJob(queue, jobId);
				
				stateManager.updateJobStatus(jobId, jobStatus);
				// update in-memroy job/task status
				jobInfo.setJobStatus(jobStatus);
				taskInfo.setTaskStatus(status);
				logInMemoryStateChanges(jobInfo, taskInfo);
			}
		}
		
		if(scheduledController.shouldCancelJob(jobId)) {
			scheduledController.jobCancelled(jobId, () -> {
				// do nothing
			}); 
		}
		
		// handle in-memory completed task
		stateManager.handleInMemoryCompletedJob(id);
		
		// update job statistics counter
		updateJobStatCounter(queue, jobStatus);
	}
	
	void updateJobStatCounter(String queue, JobStatus jobStatus) {
		final JobStatCounter jobStatCounter = resourceManager.getJobStatCounter(queue);
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

	private void logInMemoryStateChanges(final JobInfo jobInfo, final TaskInfo taskInfo) {
		LOG.info("In-memory state changed: job=" + jobInfo + ", task=" + taskInfo); 
	}
	
	void releaseResource(String queue, TaskID id) {
		taskResponseHandlingController.releaseResource(queue, id);
	}
	
	void incrementTimeoutTaskCount(String queue) {
		resourceManager.getTaskStatCounter(queue).incrementTimeoutTaskCount();
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
			Optional.ofNullable(body).ifPresent(b -> {
				try {
					// resolve the returned message
					long deliveryTag = envelope.getDeliveryTag();
					String message = new String(b);
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
			});
		}
		
	}
	
	private boolean sendAck(Channel channel, long deliveryTag) {
		return Optional.ofNullable(channel)
				.filter(ch -> ch.isOpen())
				.map(ch -> {
			try {
				ch.basicAck(deliveryTag, false);
			} catch (Exception e) {
				LOG.warn("Fail to send ack: deliveryTag=" + deliveryTag, e);
				return false;
			}
			return true;
		}).orElse(false);
	}
	
	@Override
	public ComponentManager getComponentManager() {
		return componentManager;
	}
	
	@Override
	public RecoveryManager getRecoveryManager() {
		return recoveryManager;
	};

	FreshTasksResponseProcessingManager getFreshTasksResponseProcessingManager() {
		return freshTasksResponseProcessingManager;
	}
	
}
