package cn.shiyanjun.platform.scheduled.api;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.scheduled.common.Heartbeat;
import cn.shiyanjun.platform.scheduled.common.TaskID;

public interface HeartbeatHandlingController {

	void recoverTasks();
	
	boolean isTenuredTasksResponse(JSONObject tasksResponse);
	boolean isValid(JSONObject tasksResponse);
	void processTenuredTasksResponse(Heartbeat hb);
	void processFreshTasksResponse(Heartbeat hb);
	void recoverTaskInMemoryStructures(JSONObject taskResponse) throws Exception;
	
	void releaseResource(final String queue, TaskID id);
	void releaseResource(final String queue, TaskID id, boolean isTenuredTaskResponse);
	
	void incrementSucceededTaskCount(String queue, JSONObject taskResponse);
	void incrementFailedTaskCount(String queue, JSONObject taskResponse);
}
