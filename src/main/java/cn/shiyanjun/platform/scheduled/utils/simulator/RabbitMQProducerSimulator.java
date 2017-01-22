package cn.shiyanjun.platform.scheduled.utils.simulator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.ConnectionFactory;

import cn.shiyanjun.platform.api.constants.JSONKeys;
import cn.shiyanjun.platform.api.constants.TaskStatus;
import cn.shiyanjun.platform.scheduled.api.MQAccessService;
import cn.shiyanjun.platform.scheduled.component.RabbitMQAccessService;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
import cn.shiyanjun.platform.scheduled.utils.ResourceUtils;

public class RabbitMQProducerSimulator {

	private static final String rabbitmqConfig = "rabbitmq.properties";
	
	public static void main(String[] args) throws Exception {
		
		String taskQName = "/scheduled/mq_heartbeat";
		final ConnectionFactory connectionFactory = ResourceUtils.registerAndGetResource(rabbitmqConfig, ConnectionFactory.class);
		final MQAccessService taskMQAccessService = new RabbitMQAccessService(taskQName, connectionFactory);
		taskMQAccessService.start();
		
		String platformId = "094d5d0f0a1348b1b4e9316ba0a9027f";
		int jobId = 1;
		int[] taskIds 		= new int[] {1, 2, 3, 4, 5, 6, 7};
		int[] seqNos 		= new int[] {1, 2, 3, 4, 5, 6, 7};
		int[] taskTypes 	= new int[] {1, 1, 1, 1, 1, 1, 2};
		
		for (int i = 0; i < taskIds.length; i++) {
			int taskId = taskIds[i];
			int seqNo = seqNos[i];
			int taskType = taskTypes[i];
			TaskStatus status = TaskStatus.SUCCEEDED;
			
			JSONObject message = new JSONObject(true);
			JSONArray taskArray = new JSONArray();
			message.put(JSONKeys.TYPE, ScheduledConstants.HEARTBEAT_TYPE_TASK_PROGRESS);
			message.put(ScheduledConstants.PLATFORM_ID, platformId);
			
			JSONObject jo = new JSONObject(true);
			jo.put(ScheduledConstants.JOB_ID, jobId);
			jo.put(ScheduledConstants.TASK_ID, taskId);
			jo.put(ScheduledConstants.TASK_TYPE, taskType);
			jo.put(ScheduledConstants.SEQ_NO, seqNo);
			jo.put(ScheduledConstants.STATUS, status.toString());
			jo.put("resultCount", "1423");
			taskArray.add(jo);
			
			message.put(ScheduledConstants.TASKS, taskArray);
			
			System.out.println(message);
			
			taskMQAccessService.produceMessage(message.toJSONString());
			
			Thread.sleep(10000);
		}
		
		taskMQAccessService.stop();
		
	}

}
