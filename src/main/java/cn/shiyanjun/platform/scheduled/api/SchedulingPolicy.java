package cn.shiyanjun.platform.scheduled.api;

import java.util.Optional;

import cn.shiyanjun.platform.scheduled.common.TaskOrder;

public interface SchedulingPolicy {
	
	/**
	 * Offer a task to be scheduled
	 * @return
	 */
    public Optional<TaskOrder> offerTask();
    
}
