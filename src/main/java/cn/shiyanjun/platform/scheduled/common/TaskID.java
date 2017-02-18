package cn.shiyanjun.platform.scheduled.common;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;

public class TaskID {

	final int jobId;
	final int taskId;
	final int seqNo;
	final TaskType taskType;
	
	public TaskID(int jobId, int taskId, int seqNo, TaskType taskType) {
		super();
		this.jobId = jobId;
		this.seqNo = seqNo;
		this.taskType = taskType;
		this.taskId = taskId;
	}
	
	public int getJobId() {
		return jobId;
	}

	public int getTaskId() {
		return taskId;
	}

	public int getSeqNo() {
		return seqNo;
	}

	public TaskType getTaskType() {
		return taskType;
	}
	
	@Override
	public int hashCode() {
		return 31 * taskId;
	}
	
	@Override
	public boolean equals(Object obj) {
		TaskID other = (TaskID) obj;
		return this.taskId == other.taskId;
	}
	
	protected JSONObject toJSONObject(){
		final JSONObject description = new JSONObject(true);
		description.put(ScheduledConstants.JOB_ID, jobId);
		description.put(ScheduledConstants.TASK_ID, taskId);
		description.put(ScheduledConstants.SEQ_NO, seqNo);
		description.put(ScheduledConstants.TASK_TYPE, taskType);
		return description;
	}

	@Override
	public String toString(){
		return toJSONObject().toString();
	}

}
