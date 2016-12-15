package cn.shiyanjun.platform.scheduled.api;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.constants.TaskType;

public interface RestExporter {

	Set<String> queueingNames();
	Collection<String> getWaitingJobs(String queue, String jobId);
	void prioritize(String queue, int jobId);
	Map<Integer, JSONObject> getQueuedJobStatuses(String queue);
	Map<String, JSONObject> getQueueStatuses();
	
	
	void updateResourceAmount(String queue, TaskType taskType, int amount);
	
	void setSchedulingOpened(boolean isSchedulingOpened);
	boolean isSchedulingOpened();
}
