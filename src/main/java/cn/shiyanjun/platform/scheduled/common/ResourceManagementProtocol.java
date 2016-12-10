package cn.shiyanjun.platform.scheduled.common;

import cn.shiyanjun.platform.api.Context;
import redis.clients.jedis.JedisPool;

public interface ResourceManagementProtocol {

	String getPlatformId();
	
	Context getContext();
	
	JobPersistenceService getJobPersistenceService();
	
	TaskPersistenceService getTaskPersistenceService();
	
	MQAccessService getTaskMQAccessService();
	MQAccessService getHeartbeatMQAccessService();

	QueueingManager getQueueingManager();
	
	SchedulingManager getSchedulingManager();
	
	JedisPool getJedisPool();

	SchedulingStrategy getSchedulingStrategy();
	
	ResourceMetadataManager getResourceMetadataManager();

	RestManageable getRestManageable();
	
	RecoveryManager getRecoveryManager();

	
}
