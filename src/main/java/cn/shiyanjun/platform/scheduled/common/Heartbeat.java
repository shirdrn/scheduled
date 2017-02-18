package cn.shiyanjun.platform.scheduled.common;

import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.Channel;

public class Heartbeat {

	private Channel channel;
	private final JSONObject message;
	private long deliveryTag;
	
	public Heartbeat(JSONObject message) {
		super();
		this.message = message;
	}
	
	public Heartbeat(Channel channel, JSONObject heartbeat, long deliveryTag) {
		super();
		this.channel = channel;
		this.message = heartbeat;
		this.deliveryTag = deliveryTag;
	}
	
	public Channel getChannel() {
		return channel;
	}
	
	public JSONObject getMessage() {
		return message;
	}
	
	public long getDeliveryTag() {
		return deliveryTag;
	}
	
	@Override
	public String toString() {
		return "deliveryTag=" + deliveryTag + ", message=" + message.clone() + ", channel=" + channel;
	}
}
