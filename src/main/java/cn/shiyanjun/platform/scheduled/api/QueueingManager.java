package cn.shiyanjun.platform.scheduled.api;

import java.util.Set;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.LifecycleAware;
import cn.shiyanjun.platform.scheduled.component.QueueingManagerImpl.QueueingContext;

public interface QueueingManager extends JobCollector<JSONObject>, LifecycleAware {

	void registerQueue(String queueName, int... jobTypes);
	
	QueueingContext getQueueingContext(String queueName);
	
	Set<String> queueNames();
	
	String getQueueName(int jobType);
}
