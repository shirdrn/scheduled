package cn.shiyanjun.platform.scheduled.component;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;

import cn.shiyanjun.platform.scheduled.common.AbstractMQAccessService;
import cn.shiyanjun.platform.scheduled.common.RunnableConsumer;

public class RabbitMQAccessService extends AbstractMQAccessService {

	private static final Log LOG = LogFactory.getLog(RabbitMQAccessService.class);
	private final Set<Consumer> consumers = new HashSet<>();
	private ExecutorService executorService;
	private volatile boolean running;
	
	public RabbitMQAccessService(String queueName, ConnectionFactory connectionFactory) {
		super(queueName, connectionFactory);
	}
	
	@Override
	public void start() {
		if(!running) {
			executorService = Executors.newCachedThreadPool();
			super.start();
			running = true;
		} else {
			LOG.warn("RabbitMQ access service has started.");
		}
	}
	
	@Override
	public void stop() {
		executorService.shutdown();
		super.stop();
		consumers.clear();
		running = false;
	}
	
	@Override
	public boolean produceMessage(String message) {
		try {
			channel.basicPublish("", queueName, null, message.getBytes());
			LOG.info("Message published: " + message);
			return true;
		} catch (IOException e) {
			LOG.error("Message publish failure: " + message + ", cause=" + e);
			return false;
		}
	}

	@Override
	public synchronized void addMessageConsumer(RunnableConsumer consumer) {
		if(!consumers.contains(consumer)) {
			consumers.add(consumer);
			executorService.execute(consumer);
		} else {
			LOG.warn("Consumer already existed: " + consumer);
		}
	}
	
}
