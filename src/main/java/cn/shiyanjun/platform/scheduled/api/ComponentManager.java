package cn.shiyanjun.platform.scheduled.api;

import cn.shiyanjun.platform.api.Context;
import redis.clients.jedis.JedisPool;

public interface ComponentManager {

	String getPlatformId();
	Context getContext();
	
	JobPersistenceService getJobPersistenceService();
	TaskPersistenceService getTaskPersistenceService();
	MQAccessService getTaskMQAccessService();
	MQAccessService getHeartbeatMQAccessService();

	JobFetcher getJobFetcher();
	QueueingManager getQueueingManager();
	SchedulingManager getSchedulingManager();
	SchedulingPolicy getSchedulingPolicy();
	ResourceManager getResourceManager();
	RestExporter getRestExporter();
	RecoveryManager getRecoveryManager();
	JobController getJobController();
	
	JedisPool getJedisPool();

	
}
