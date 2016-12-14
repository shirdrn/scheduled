package cn.shiyanjun.platform.scheduled.common;

import java.util.Optional;

import cn.shiyanjun.platform.api.constants.TaskType;

public interface SchedulingStrategy {
	
	/**
	 * Offer a task to be scheduled
	 * @param queueName
	 * @param taskType
	 * @return
	 */
    public Optional<TaskOrder> offerTask(String queueName, TaskType taskType);
    
}
