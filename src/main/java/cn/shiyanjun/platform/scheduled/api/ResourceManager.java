package cn.shiyanjun.platform.scheduled.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.api.utils.Pair;
import cn.shiyanjun.platform.scheduled.component.ResourceManagerImpl.JobStatCounter;
import cn.shiyanjun.platform.scheduled.component.ResourceManagerImpl.TaskStatCounter;

public interface ResourceManager {

	void registerResource(String queue, List<Pair<TaskType, Integer>> amounts);
	
	void allocateResource(String queue, TaskType taskType);
	void releaseResource(String queue, int jobId, int taskId, TaskType taskType);
	int availableResource(String queue, TaskType taskType);
	Set<TaskType> taskTypes(String queue);
	
	int getRunningTaskCount(String queue);
	JobStatCounter getJobStatCounter(String queue);
	TaskStatCounter getTaskStatCounter(String queue);
	
	void updateReportedResources(Map<TaskType, Integer> resources);
	void currentResourceStatuses();
	
	void updateResourceAmount(String queue, TaskType taskType, int amount);
	
	void registerQueueCapacity(String queue, int capacity);
	Map<String, Integer> queueCapacities();
}
