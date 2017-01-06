package cn.shiyanjun.platform.scheduled.api;

public interface JobController {

	boolean cancelJob(int jobId);
	boolean shouldCancelJob(int jobId);
	void jobCancelled(int jobId, Runnable action);
}
