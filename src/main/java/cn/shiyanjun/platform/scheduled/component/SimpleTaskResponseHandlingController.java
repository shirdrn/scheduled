package cn.shiyanjun.platform.scheduled.component;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;

import cn.shiyanjun.platform.api.constants.JSONKeys;
import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.HeartbeatHandlingController;
import cn.shiyanjun.platform.scheduled.api.ResourceManager;
import cn.shiyanjun.platform.scheduled.api.ResponseHandler;
import cn.shiyanjun.platform.scheduled.api.StateManager;
import cn.shiyanjun.platform.scheduled.common.Heartbeat;
import cn.shiyanjun.platform.scheduled.common.TaskID;
import cn.shiyanjun.platform.scheduled.component.SchedulingManagerImpl.FreshTasksResponseProcessingManager;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;

public final class SimpleTaskResponseHandlingController implements HeartbeatHandlingController {
	
	private static final Log LOG = LogFactory.getLog(SimpleTaskResponseHandlingController.class);
	private final boolean isRecoveryFeatureEnabled;
	private final ComponentManager componentManager;
	private final SchedulingManagerImpl sched;
	private final ResourceManager resourceManager;
	private final StateManager stateManager;
	private final ResponseHandler<Heartbeat> tenuredInflightTasksResponseHandler = new TenuredInflightTaskResponseHandler();
	private final ResponseHandler<JSONObject> tenuredInMQTasksResponseHandler = new TenuredInMQTaskResponseHandler();
	private final FreshTasksResponseProcessingManager freshTasksResponseProcessingManager;
	
	public SimpleTaskResponseHandlingController(ComponentManager componentManager) {
		this.componentManager = componentManager;
		this.sched = (SchedulingManagerImpl) componentManager.getSchedulingManager();
		isRecoveryFeatureEnabled = componentManager.getContext().getBoolean(ConfigKeys.SCHEDULED_RECOVERY_FEATURE_ENABLED, false);
		LOG.info("Configs: isRecoveryFeatureEnabled=" + isRecoveryFeatureEnabled);
		
		this.resourceManager = componentManager.getResourceManager();
		this.stateManager = componentManager.getStateManager();
		freshTasksResponseProcessingManager = sched.getFreshTasksResponseProcessingManager();
	}
	
	@Override
	public void incrementSucceededTaskCount(String queue, JSONObject taskResponse) {
		if(!isTenuredTasksResponse(taskResponse)) {
			resourceManager.getTaskStatCounter(queue).incrementSucceededTaskCount();
		}
	}
	
	@Override
	public void incrementFailedTaskCount(String queue, JSONObject taskResponse) {
		if(!isTenuredTasksResponse(taskResponse)) {
			resourceManager.getTaskStatCounter(queue).incrementFailedTaskCount();
		}
	}
	
	@Override
	public void recoverTasks() {
		if(isRecoveryFeatureEnabled) {
			Map<Integer, JSONObject> jobs = componentManager.getRecoveryManager().getPendingTaskResponses();
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
			Map<Integer, JSONObject> jobs = componentManager.getRecoveryManager().getPendingTaskResponses();
			jobs.keySet().forEach(jobId -> {
				JSONObject taskResponse = null;
				try {
					taskResponse = jobs.get(jobId);
					if(!isJobTaskCompleted(taskResponse)) {
						stateManager.updateJobStatus(jobId, JobStatus.FAILED);
					}
				} catch (Exception e) {
					LOG.warn("Fail to recover task: " + taskResponse, e);
				}
			});
			
			stateManager.updateJobStatus(JobStatus.RUNNING, JobStatus.FAILED);
		}
	}
	
	private boolean isJobTaskCompleted(JSONObject taskResponse) {
		int jobId = taskResponse.getIntValue(ScheduledConstants.JOB_ID);
		// for tenured task response
		if(isTenuredTasksResponse(taskResponse)) {
			return stateManager.isJobCompleted(jobId);
		} else {
			return stateManager.isInMemJobCompleted(jobId);
		}
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
		JSONObject tasksResponse = hb.getMessage();
		JSONArray tasks = hb.getMessage().getJSONArray(ScheduledConstants.TASKS);
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
						stateManager.updateJobStatus(jobId, JobStatus.FAILED);
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
		LOG.debug("protocol.getPlatformId() = " + componentManager.getPlatformId());
		LOG.debug("tasksResponse.getPlatformId() = " + platformId);
		return !componentManager.getPlatformId().equals(platformId);
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
			stateManager.recoverTaskInMemoryStructures(taskResponse);
		}
	}
	
	@Override
	public void releaseResource(final String queue, TaskID id, boolean isTenuredTaskResponse) {
		// if a fresh task response, operate resource counter
		if(!isTenuredTaskResponse) {
			releaseResource(queue, id);
		}
	}
	
	@Override
	public void releaseResource(final String queue, TaskID id) {
		resourceManager.releaseResource(queue, id.getJobId(), id.getTaskId(), id.getTaskType());
		resourceManager.currentResourceStatuses();
		sched.logTaskCounterChanged(queue);
	}
	
	/**
	 * Handle task responses with a valid MQ channel. 
	 * In this case scheduling platform is started, but the tenured task responses are not published
	 * to MQ by running platform.
	 * 
	 * @author yanjun
	 */
	private final class TenuredInflightTaskResponseHandler implements ResponseHandler<Heartbeat> {

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
	private final class TenuredInMQTaskResponseHandler implements ResponseHandler<JSONObject> {

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
	
}
