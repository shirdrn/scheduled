package cn.shiyanjun.platform.scheduled.common;

import java.util.Set;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.LifecycleAware;
import cn.shiyanjun.platform.scheduled.component.DefaultQueueingManager.QueueingContext;

public interface QueueingManager extends Dispatcher<JSONObject>, LifecycleAware {

	void registerQueue(String queueName, int... jobTypes);
	
	QueueingContext getQueueingContext(String queueName);
	
	Set<String> queueNames();
	
	String getQueueName(int jobType);
}
