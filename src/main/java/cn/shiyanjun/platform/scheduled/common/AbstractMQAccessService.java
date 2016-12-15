package cn.shiyanjun.platform.scheduled.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import cn.shiyanjun.platform.scheduled.api.MQAccessService;

public abstract class AbstractMQAccessService implements MQAccessService {

	private static final Log LOG = LogFactory.getLog(AbstractMQAccessService.class);
	private final ConnectionFactory connectionFactory;
	protected Channel channel;
	protected Connection connection;
	protected final String queueName;
	 
	public AbstractMQAccessService(String queueName, ConnectionFactory connectionFactory) {
		super();
		this.queueName = queueName;
		this.connectionFactory = connectionFactory;
	}
	
	@Override
	public void start() {
		try {
			connection = connectionFactory.newConnection();	
			channel = connection.createChannel();
			try {
				Map<String, Object> args = new HashMap<String, Object>();
				channel.queueDeclare(queueName, true, false, false, args);
			} catch (IOException e) {
				LOG.error("set queue message expired exception: " + queueName);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void stop() {
		try {
			channel.close();
			connection.close();
		} catch (Exception e) {
			LOG.warn("Failed to close: connection=" + connection + ", channel=" + channel);
		}
	}
	
	@Override
	public Channel getChannel() {
		return channel;
	}
	
	@Override
	public String getQueueName() {
		return queueName;
	}
	
}
