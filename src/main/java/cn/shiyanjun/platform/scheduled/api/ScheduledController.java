package cn.shiyanjun.platform.scheduled.api;

public interface ScheduledController {

	boolean cancelJob(int jobId);
	boolean cancelJob(int jobId, Runnable action);
	boolean shouldCancelJob(int jobId);
	void jobCancelled(int jobId, Runnable action);
	
	void setSchedulingOpened(boolean isSchedulingOpened);
	boolean isSchedulingOpened();
}
