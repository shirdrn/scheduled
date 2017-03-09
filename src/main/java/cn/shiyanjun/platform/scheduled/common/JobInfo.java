package cn.shiyanjun.platform.scheduled.common;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.constants.JobStatus;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;

public final class JobInfo {

	final int jobId;
	final String queue;
	final int taskCount;
	volatile JobStatus jobStatus;
	volatile long lastUpdatedTime;
	final Lock inMemJobUpdateLock = new ReentrantLock();
	
	public JobInfo(int jobId, String queue, int taskCount) {
		super();
		this.jobId = jobId;
		this.queue = queue;
		this.taskCount = taskCount;
		lastUpdatedTime = Time.now();
	}
	
	public void lock() {
		inMemJobUpdateLock.lock();
	}
	
	public void unlock() {
		inMemJobUpdateLock.unlock();
	}
	
	public void setJobStatus(JobStatus jobStatus) {
		this.jobStatus = jobStatus;
	}

	public long getLastUpdatedTime() {
		return lastUpdatedTime;
	}

	public int getJobId() {
		return jobId;
	}

	public JobStatus getJobStatus() {
		return jobStatus;
	}

	public void setLastUpdatedTime(long lastUpdatedTime) {
		this.lastUpdatedTime = lastUpdatedTime;
	}

	public String getQueue() {
		return queue;
	}

	public int getTaskCount() {
		return taskCount;
	}
	
	@Override
	public int hashCode() {
		return 31 * jobId + 31 * queue.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		JobInfo other = (JobInfo) obj;
		return this.jobId == other.jobId && this.queue.equals(other.queue);
	}
	
	@Override
	public String toString() {
		final JSONObject description = new JSONObject(true);
		description.put(ScheduledConstants.JOB_ID, jobId);
		description.put(ScheduledConstants.QUEUE, queue);
		description.put(ScheduledConstants.TASK_COUNT, taskCount);
		description.put(ScheduledConstants.JOB_STATUS, jobStatus);
		description.put(ScheduledConstants.LAST_UPDATE_TS, lastUpdatedTime);
		return description.toString();
	}

}
