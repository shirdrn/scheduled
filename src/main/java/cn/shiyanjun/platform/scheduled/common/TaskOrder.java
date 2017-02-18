package cn.shiyanjun.platform.scheduled.common;

import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.scheduled.dao.entities.Task;

public class TaskOrder {

	private String queue;
	private Task task;
	private int taskCount;
	
	public TaskOrder(String queue, Task task) {
		super();
		this.queue = queue;
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
	
	public String getQueue() {
		return queue;
	}
	
	@Override
	public String toString() {
		return " jobId=" + task.getJobId() + 
				", taskId=" + task.getId() + 
				", seqNo=" + task.getSeqNo() + 
				", taskType=" + TaskType.fromCode(task.getTaskType()) + 
				", taskCount=" + taskCount;
	}

}
