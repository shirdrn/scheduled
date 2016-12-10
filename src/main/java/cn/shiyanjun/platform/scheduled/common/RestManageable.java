package cn.shiyanjun.platform.scheduled.common;

import java.util.Collection;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

public interface RestManageable {

	/**
	 * Get waiting jobs before specified job in the <code>queue</code>.
	 * @param queue
	 * @param jobId
	 * @return
	 */
	Collection<String> getWaitingJobs(String queue, String jobId);
	
	/**
	 * Prioritize the given job in the <code>queue</code>.
	 * @param queue
	 * @param jobId
	 */
	void prioritize(String queue, int jobId);
	
	/**
	 * Get all statuses for jobs in the <code>queue</code>.
	 * @param queue
	 * @return
	 */
	Map<Integer, JSONObject> getQueuedJobStatuses(String queue);
	
	/**
	 * Get all queue statuses.
	 * @return
	 */
	Map<String, JSONObject> getQueueStatuses();
	
}
