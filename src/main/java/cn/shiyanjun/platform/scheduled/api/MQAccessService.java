package cn.shiyanjun.platform.scheduled.api;

import com.rabbitmq.client.Channel;

import cn.shiyanjun.platform.api.LifecycleAware;

/**
 * MQ access service protocol. You can:
 * <ol>
 * 	<li>Sends message to a MQ by invoking method <code>produceMessage</code>.</li>
 * 	<li>Consumes message from a MQ by adding implemented {@link RunnableConsumer} instances.</li>
 * </ol>
 * 
 * @author yanjun
 */
public interface MQAccessService extends LifecycleAware {

	/**
	 * Sends a message to MQ storage.
	 * @param message
	 * @return
	 */
	boolean produceMessage(String message);
	
	/**
	 * Add a {@link RunnableConsumer} instance, which is managed by
	 * the {@link MQAccessService}.
	 * @param consumer
	 */
	void addMessageConsumer(RunnableConsumer consumer);
	
	Channel getChannel();
	
	String getQueueName();
}
