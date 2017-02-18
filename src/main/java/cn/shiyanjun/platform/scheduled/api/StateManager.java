package cn.shiyanjun.platform.scheduled.api;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.scheduled.common.JobInfo;
import cn.shiyanjun.platform.scheduled.common.TaskID;
import cn.shiyanjun.platform.scheduled.common.TaskInfo;
import cn.shiyanjun.platform.scheduled.common.TaskOrder;
import cn.shiyanjun.platform.scheduled.dao.entities.Job;
import cn.shiyanjun.platform.scheduled.dao.entities.Task;

public interface StateManager {

	void registerRunningJob(TaskOrder taskOrder);
	Optional<JobInfo> getRunningJob(int jobId);
	Collection<JobInfo> getRunningJobs();
	void updateJobStatus(int jobId, JobStatus jobStatus);
	void updateJobStatus(int jobId, JobStatus jobStatus, long timestamp);
	void updateJobStatus(JobStatus currentStatus, JobStatus targetStatus);
	Job retrieveJob(int jobId);
	List<Job> retrieveJobs(JobStatus jobStatus);
	boolean isJobCompleted(int jobId);
	boolean isInMemJobCompleted(int jobId);
	Optional<String> getRunningJobQueueName(int jobId);
	
	void registerRunningTask(TaskID id, String platformId);
	Optional<TaskInfo> getRunningTask(TaskID id);
	Collection<TaskInfo> getRunningTasks();
	void taskPublished(TaskID id) throws Exception;
	void updateTaskStatus(int jobId, int taskId, TaskStatus taskStatus);
	void updateTaskStatus(int jobId, int taskId, TaskStatus taskStatus, long timestamp);
	void updateTaskStatus(TaskID id, TaskStatus taskStatus, JSONObject taskResponse);
	List<Task> retrieveTasks(int jobId);
	
	void recoverTaskInMemoryStructures(JSONObject taskResponse) throws Exception;
	
	Set<String> queueNames();
	JSONObject retrieveQueuedJob(String queue, int jobId);
	Set<JSONObject> retrieveQueuedJobs(String queue);
	void updateQueuedJob(int jobId, String queue, JSONObject job) throws Exception;
	boolean removeQueuedJob(String queue, int jobId);
	
	void handleInMemoryCompletedJob(int jobId);
	void handleInMemoryCompletedJob(TaskID id);
	void handleInMemoryTimeoutJob(JobInfo jobInfo, int keptTimeoutJobMaxCount);
}
