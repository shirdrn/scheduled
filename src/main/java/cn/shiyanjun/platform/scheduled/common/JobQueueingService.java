package cn.shiyanjun.platform.scheduled.common;

import com.alibaba.fastjson.JSONObject;

import java.util.Set;

/**
 * Job queueing service. Its responsibilities are to manage all
 * operations for the queueing queue.
 * 
 * @author yanjun
 */
public interface JobQueueingService {

	String getQueueName();
	
	/**
	 * Enqueue a job to the specified <code>queue</code>.
	 * @param jobId
	 * @param jsonJobDetail
	 */
	void enqueue(int jobId, String jsonJobDetail);
	
	/**
	 * Prioritize a job in the specified <code>queue</code>
	 * @param jobId
	 */
	void prioritize(int jobId);
	
	/**
	 * Retrieve queued job information.
	 * @param jobId
	 * @return
	 */
	JSONObject retrieve(int jobId);
	
	/**
	 * Update queued job information.
	 * @param jobId
	 * @param oldJob
	 * @param newJob
	 */
	void updateQueuedJob(int jobId, JSONObject newJob);
	
	/**
	 * Remove a job from the specified <code>queue</code>.
	 * @param jobId
	 */
	void remove(String jobId);
	
	/**
	 * Retrieve all jobs from the given <code>queue</code>.
	 * @return
	 */
	Set<String> getJobs();
	
	/**
	 * Get waiting jobs before the specified <code>jobId</code> in the <code>queue</code>.
	 * @param jobId
	 * @return
	 */
	Set<String> getWaitingJobsBefore(String jobId);
}
