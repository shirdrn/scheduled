package cn.shiyanjun.platform.scheduled.common;

import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.scheduled.dao.entities.Task;

public class ScheduledTask {

	private Task task;
	private int taskCount;
	
	public ScheduledTask(Task task) {
		super();
		this.task = task;
	}
	
	public Task getTask() {
		return task;
	}
	public void setTask(Task task) {
		this.task = task;
	}
	public int getTaskCount() {
		return taskCount;
	}
	public void setTaskCount(int taskCount) {
		this.taskCount = taskCount;
	}
	
	@Override
	public String toString() {
		return " jobId=" + task.getJobId() + 
				", taskId=" + task.getId() + 
				", serialNo=" + task.getSerialNo() + 
				", taskType=" + TaskType.fromCode(task.getTaskType()) + 
				", taskCount=" + taskCount;
	}
	
}
