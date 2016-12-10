package cn.shiyanjun.platform.scheduled.common;

import java.util.Map;
import java.util.Set;

import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.scheduled.component.ResourceMetadataManagerImpl.TaskStatCounter;

/**
 * Created by luogang on 2016/7/26.
 */
public interface ResourceMetadataManager {

	void allocateResource(String queue, TaskType taskType);
	void releaseResource(String queue, TaskType taskType);
	int queryResource(String queue, TaskType taskType);
	Set<TaskType> taskTypes(String queue);
	
	int getRunningTaskCount(String queue);
	TaskStatCounter getTaskStatCounter(String queue);
	
	void updateReportedResources(Map<TaskType, Integer> resources);
	void currentResourceStatuses();
	
	void updateResourceAmount(String queue, TaskType taskType, int amount);
}
