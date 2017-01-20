package cn.shiyanjun.platform.scheduled.component;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;

import cn.shiyanjun.platform.api.constants.JSONKeys;
import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.JobPersistenceService;
import cn.shiyanjun.platform.scheduled.api.JobQueueingService;
import cn.shiyanjun.platform.scheduled.api.QueueingManager;
import cn.shiyanjun.platform.scheduled.api.RecoveryManager;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
import cn.shiyanjun.platform.scheduled.dao.entities.Job;

public class RecoveryManagerImpl implements RecoveryManager {

	private static final Log LOG = LogFactory.getLog(RecoveryManagerImpl.class);
	private final ComponentManager manager;
	private final JobPersistenceService jobPersistenceService;
	private final QueueingManager queueingManager;
	private final BlockingQueue<JSONObject> needRecoveredTaskQueue = Queues.newLinkedBlockingQueue();
	private final Map<Integer, JSONObject> processedPendingRecoveryJobs = Maps.newHashMap();

	public RecoveryManagerImpl(ComponentManager manager) {
		super();
		this.manager = manager;
		jobPersistenceService = manager.getJobPersistenceService();
		queueingManager = manager.getQueueingManager();
	}

	@Override
	public void start() {
		try {
			manager.getHeartbeatMQAccessService().start();
			
			LOG.info("Start to read messages from MQ ...");
			final Channel channel = manager.getHeartbeatMQAccessService().getChannel();
			final String q = manager.getHeartbeatMQAccessService().getQueueName();
			
			while(true) {
				GetResponse response = channel.basicGet(q, false);
				if(response != null) {
					long deliveryTag = response.getEnvelope().getDeliveryTag();
					String message = new String(response.getBody(), "UTF-8");
					LOG.info("Received msg: deliveryTag=" + deliveryTag + ", message=" + message);
					
					JSONObject result = JSONObject.parseObject(message);
					if(result.containsKey(JSONKeys.TYPE)) {
						String type = result.getString(JSONKeys.TYPE);
						switch(type) {
							case ScheduledConstants.HEARTBEAT_TYPE_TASK_PROGRESS:
								
								// {"type":"taskProgress","platform_id":"7fe13e9879314da38bb7abc8b61657bb","tasks":[{"result":{"traceId":"1480402382967","callId":"1","resultCount":"1423","message":"SUCCESS","status":5},"jobId":1,"taskType":1,"taskId":1,"seqNo":1,"status":"SUCCEEDED"}]}
								// add flag 'needRecovering' to a tasks response
								result.put(ScheduledConstants.NEED_RECOVERING, ScheduledConstants.YES);
								needRecoveredTaskQueue.add(result);
								channel.basicAck(deliveryTag, false);
								break;
						}
					} else {
						channel.basicAck(deliveryTag, false);
					}
				} else {
					break;
				}
			}
			LOG.info("Complete to read MQ messages: q=" + q);
			
			// update Redis statuses
			recoverRedisStates();
			
			// recover job/task statuses
			processPendingTaskResponses();
		} catch (Exception e) {
			LOG.error("Recovery failure:", e);
			Throwables.propagate(e);
		} finally {
			manager.getHeartbeatMQAccessService().stop();
		}
	}
	
	// | Status transitions In Redis |
	//     Job : QUEUEING -> SCHEDULED -> RUNNING -> SUCCEEDED/FAILED
	//     Task: WAIT_TO_BE_SCHEDULED -> SCHEDULED -> RUNNING -> SUCCEEDED/FAILED
	private void recoverRedisStates() {
		queueingManager.queueNames().forEach(queue -> {
			JobQueueingService qs = queueingManager.getQueueingContext(queue).getJobQueueingService();
			Set<String> jobs = qs.getJobs();
			jobs.forEach(strJob -> {
				JSONObject job = JSONObject.parseObject(strJob);
				int jobId = job.getIntValue(ScheduledConstants.JOB_ID);
				String strJobStatus = job.getString(ScheduledConstants.JOB_STATUS);
				JobStatus jobStatus = JobStatus.valueOf(strJobStatus);
				if(jobStatus == JobStatus.SCHEDULED) {
					job.put(ScheduledConstants.JOB_STATUS, JobStatus.QUEUEING.toString());
					job.put(ScheduledConstants.LAST_UPDATE_TS, Time.now());
					job.put(ScheduledConstants.TASK_STATUS, TaskStatus.CREATED.toString());
					qs.updateQueuedJob(jobId, job);
					LOG.info("Job in Redis recovered: " + job);
				} else if(jobStatus == JobStatus.RUNNING 
						|| jobStatus == JobStatus.SUCCEEDED || jobStatus == JobStatus.FAILED) {
					qs.remove(String.valueOf(jobId));
					LOG.info("Job in Redis removed: " + job);
				}
			});
		});
	}

	@Override
	public void stop() {
		
	}
	
	private void processPendingTaskResponses() {
		Set<Integer> completedJobIds = Sets.newHashSet();
		// Map<jobId, Map<taskId, List<JSONObject>>>
		Map<Integer, Map<Integer, List<JSONObject>>> jobs = Maps.newLinkedHashMap();
		needRecoveredTaskQueue.forEach(jobData -> {
			JSONArray tasks = jobData.getJSONArray(ScheduledConstants.TASKS);
			if(tasks != null && !tasks.isEmpty()) {
				for (int i = 0; i < tasks.size(); i++) {
					JSONObject taskResponse = tasks.getJSONObject(i);
					int jobId = taskResponse.getIntValue(ScheduledConstants.JOB_ID);
					int taskId = taskResponse.getIntValue(ScheduledConstants.TASK_ID);
					int seqNo = taskResponse.getIntValue(ScheduledConstants.SEQ_NO);
					int taskCount = taskResponse.getIntValue(ScheduledConstants.TASK_COUNT);
					TaskStatus taskStatus = TaskStatus.valueOf(taskResponse.getString(ScheduledConstants.STATUS));
					
					if(!completedJobIds.contains(jobId)) {
						// job status in db is in (FAILED, SUCCEED), discard task response message
						Job job = jobPersistenceService.retrieveJob(jobId);
						if(job != null) {
							if(job.getStatus() == JobStatus.SUCCEEDED.getCode() 
									|| job.getStatus() == JobStatus.FAILED.getCode()) {
								completedJobIds.add(jobId);
								JobStatus jobDBStatus = job.getStatus() == 
										JobStatus.SUCCEEDED.getCode() ? JobStatus.SUCCEEDED : JobStatus.FAILED;
								LOG.info("Discard task response, due to job in db completed: jobId=" + jobId + ", jobStatus=" + jobDBStatus + ", taskResponse=" + taskResponse);
							} else {
								// job in DB with status not in (FAILED, SUCCEED)
								// task in response with status in (FAILED, SUCCEED)
								if(taskStatus == TaskStatus.FAILED 
										|| (taskStatus == TaskStatus.SUCCEEDED && taskCount == seqNo)) {
									processCompletedJob(jobId, taskResponse, taskStatus);
									completedJobIds.add(jobId);
								} else {
									Map<Integer, List<JSONObject>> cachedTasks = jobs.get(jobId);
									if(cachedTasks == null) {
										cachedTasks = Maps.newLinkedHashMap();
										jobs.put(jobId, cachedTasks);
										cachedTasks.put(taskId, Lists.newArrayList());
									}
									cachedTasks.get(taskId).add(taskResponse);
								}
							}
						} else {
							LOG.warn("Unknown job: jobId=" + jobId);
						}
					} else {
						LOG.warn("Job already completed: taskResponse=" + taskResponse);
					}
				}
			}
		});
		LOG.info("Completed jobs: " + completedJobIds);
		
		jobs.keySet().forEach(jobId -> {
			Map<Integer, List<JSONObject>> taskMap = jobs.get(jobId);
			if(taskMap.size() == 1) {
				// just one task for the job, choose it as the last arrived task response
				Map.Entry<Integer, List<JSONObject>> entry = taskMap.entrySet().iterator().next();
				List<JSONObject> taskList = entry.getValue();
				processedPendingRecoveryJobs.put(jobId, taskList.get(taskList.size() - 1));
			} else {
				// multiple tasks, choose the last arrived task && the last arrived task response
				Iterator<Integer> iter = taskMap.keySet().iterator();
				Integer lastTaskId = null;
				while(iter.hasNext()) {
					lastTaskId = iter.next();
				}
				List<JSONObject> taskResponseList = taskMap.get(lastTaskId);
				processedPendingRecoveryJobs.put(jobId, taskResponseList.get(taskResponseList.size() - 1));
			}
		});
		
		// add flag 'needRecovering' to each tenured task response
		processedPendingRecoveryJobs.values().stream().forEach(taskResponse -> {
			taskResponse.put(ScheduledConstants.NEED_RECOVERING, ScheduledConstants.YES);
		});
		
		LOG.info("Pending recovery status: jobCount=" + jobs.size() + ", jobs=" + jobs);		
	}

	@Override
	public Map<Integer, JSONObject> getPendingTaskResponses() {
		return processedPendingRecoveryJobs;
	}

	private void processCompletedJob(int jobId, JSONObject taskResponse, TaskStatus taskStatus) {
		try {
			JobStatus targetJobStatus = JobStatus.valueOf(taskStatus.name());
			updateJobInfo(jobId, targetJobStatus);
		} catch (Exception e) {
			LOG.warn("Fail to update job: jobId=" + jobId + ", taskResponse=" + taskResponse, e);
		}
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

}
