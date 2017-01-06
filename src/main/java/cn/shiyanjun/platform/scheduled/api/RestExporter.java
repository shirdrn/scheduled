package cn.shiyanjun.platform.scheduled.api;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.api.utils.Pair;

public interface RestExporter {

	Set<String> queueingNames();
	Collection<String> getWaitingJobs(String queue, String jobId);
	void prioritize(String queue, int jobId);
	Map<Integer, JSONObject> getQueuedJobStatuses(String queue);
	/**
	 * Get all Redis queues' statuses
	 * @return
	 */
	Map<String, JSONObject> getQueueStatuses();
	/**
	 * Cancel a running job.
	 * @param jobId
	 * @return
	 */
	boolean cancelJob(int jobId);
	
	/**
	 * Update soft constraint amount for a specified task type resource.
	 * @param queue
	 * @param taskType
	 * @param amount
	 */
	void updateResourceAmount(String queue, TaskType taskType, int amount);
	
	void setSchedulingOpened(boolean isSchedulingOpened);
	boolean isSchedulingOpened();
	
	Pair<String, String> queryMaintenanceTimeSegment();
	void updateMaintenanceTimeSegment(String startTime, String endTime);
}
