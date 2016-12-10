package cn.shiyanjun.platform.scheduled.utils.simulator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.ConnectionFactory;

import cn.shiyanjun.platform.api.constants.JSONKeys;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.scheduled.common.MQAccessService;
import cn.shiyanjun.platform.scheduled.component.RabbitMQAccessService;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
import cn.shiyanjun.platform.scheduled.utils.ResourceUtils;

public class RabbitMQProducerSimulator {

	private static final String rabbitmqConfig = "rabbitmq.properties";
	
	public static void main(String[] args) {
		
		String taskQName = "/scheduled/mq_heartbeat";
		final ConnectionFactory connectionFactory = ResourceUtils.registerAndGetResource(rabbitmqConfig, ConnectionFactory.class);
		final MQAccessService taskMQAccessService = new RabbitMQAccessService(taskQName, connectionFactory);
		taskMQAccessService.start();
		
		String platformId = "343e99d93bf440ed9bbddc491943d4e8";
		int taskId = 7;
		int jobId = 1;
		int serialNo = 7;
		TaskStatus status = TaskStatus.SUCCEEDED;
		int taskType = 2;
		
		JSONObject message = new JSONObject(true);
		JSONArray taskArray = new JSONArray();
		message.put(JSONKeys.TYPE, ScheduledConstants.HEARTBEAT_TYPE_TASK_PROGRESS);
		message.put(ScheduledConstants.PLATFORM_ID, platformId);
		
		JSONObject jo = new JSONObject(true);
		jo.put(ScheduledConstants.JOB_ID, jobId);
		jo.put(ScheduledConstants.TASK_ID, taskId);
		jo.put(ScheduledConstants.TASK_TYPE, taskType);
		jo.put(ScheduledConstants.SERIAL_NO, serialNo);
		jo.put(ScheduledConstants.STATUS, status.toString());
		jo.put("resultCount", "1423");
		taskArray.add(jo);
		
		message.put(ScheduledConstants.TASKS, taskArray);
		
		System.out.println(message);
		
		taskMQAccessService.produceMessage(message.toJSONString());
		
		taskMQAccessService.stop();
		
	}

}
