package cn.shiyanjun.platform.scheduled.api;

import java.util.Optional;

import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.scheduled.common.TaskOrder;

public interface SchedulingPolicy {
	
	/**
	 * Offer a task to be scheduled
	 * @param queueName
	 * @param taskType
	 * @return
	 */
    public Optional<TaskOrder> offerTask(String queueName, TaskType taskType);
    
}
