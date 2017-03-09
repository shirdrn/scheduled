package cn.shiyanjun.platform.scheduled.api;

import cn.shiyanjun.platform.api.Context;
import redis.clients.jedis.JedisPool;

public interface ComponentManager extends ScheduledController {

	Context getContext();
	String getPlatformId();
	
	MQAccessService getTaskMQAccessService();
	MQAccessService getHeartbeatMQAccessService();

	JobFetcher getJobFetcher();
	QueueingManager getQueueingManager();
	SchedulingManager getSchedulingManager();
	ResourceManager getResourceManager();
	RestExporter getRestExporter();
	RecoveryManager getRecoveryManager();
	StateManager getStateManager();
	
	JedisPool getJedisPool();

	
}
