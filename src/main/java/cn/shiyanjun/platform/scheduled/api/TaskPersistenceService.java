package cn.shiyanjun.platform.scheduled.api;

import java.util.List;

import cn.shiyanjun.platform.scheduled.dao.entities.Task;

public interface TaskPersistenceService {

	void insertTasks(List<Task> tasks);
	
	void updateTaskByID(Task task);

	List<Task> getTasksFor(int jobId);

}
