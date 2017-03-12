package cn.shiyanjun.platform.scheduled.api;

import cn.shiyanjun.platform.api.Context;
import redis.clients.jedis.JedisPool;

public interface ComponentManager {

	Context getContext();
	String getPlatformId();
	
	MQAccessService getTaskMQAccessService();
	MQAccessService getHeartbeatMQAccessService();

	JobFetcher getJobFetcher();
	QueueingManager getQueueingManager();
	SchedulingManager getSchedulingManager();
	ResourceManager getResourceManager();
	RestExporter getRestExporter();
	StateManager getStateManager();
	
	JedisPool getJedisPool();
	ScheduledController getScheduledController();
	
}
