package cn.shiyanjun.platform.scheduled.common;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;

public class TaskInfo extends TaskID {

	long scheduledTime;
	volatile long lastUpdatedTime;
	volatile TaskStatus taskStatus;
	String platformId;
	final TaskID id;
	
	public TaskInfo(TaskID id) {
		super(id.jobId, id.taskId, id.seqNo, id.taskType);
		this.id = id;
	}
	
	public TaskInfo(TaskID id, String platformId) {
		super(id.jobId, id.taskId, id.seqNo, id.taskType);
		this.id = id;
		this.platformId = platformId;
		this.lastUpdatedTime = Time.now();
		this.scheduledTime = lastUpdatedTime;
		this.taskStatus = TaskStatus.SCHEDULED;
	}
	
	public TaskStatus getTaskStatus() {
		return taskStatus;
	}

	public void setTaskStatus(TaskStatus taskStatus) {
		this.taskStatus = taskStatus;
	}

	public long getLastUpdatedTime() {
		return lastUpdatedTime;
	}

	public void setLastUpdatedTime(long lastUpdatedTime) {
		this.lastUpdatedTime = lastUpdatedTime;
	}

	public String getPlatformId() {
		return platformId;
	}

	public void setPlatformId(String platformId) {
		this.platformId = platformId;
	}
	
	@Override
	protected JSONObject toJSONObject() {
		final JSONObject description = super.toJSONObject();
		description.put(ScheduledConstants.PLATFORM_ID, platformId);
		description.put(ScheduledConstants.TASK_STATUS, taskStatus);
		description.put(ScheduledConstants.LAST_UPDATE_TS, lastUpdatedTime);
		return description;
	}
	
	@Override
	public String toString() {
		return toJSONObject().toString();
	}

}
