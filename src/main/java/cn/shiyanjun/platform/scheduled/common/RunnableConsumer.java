package cn.shiyanjun.platform.scheduled.common;

import com.rabbitmq.client.Consumer;

/**
 * A runnable consumer is bound to a unique MQ queue, which is identified
 * the <code>queueName</code>. In other words, messages in a queue just 
 * consumed by one consumer.
 * 
 * @author yanjun
 */
public interface RunnableConsumer extends Consumer, Runnable {

}
