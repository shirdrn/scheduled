package cn.shiyanjun.platform.scheduled;

import java.util.Iterator;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ibatis.session.SqlSessionFactory;

import com.rabbitmq.client.ConnectionFactory;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.LifecycleAware;
import cn.shiyanjun.platform.api.common.AbstractComponent;
import cn.shiyanjun.platform.api.common.ContextImpl;
import cn.shiyanjun.platform.scheduled.common.JobPersistenceService;
import cn.shiyanjun.platform.scheduled.common.MQAccessService;
import cn.shiyanjun.platform.scheduled.common.QueueingManager;
import cn.shiyanjun.platform.scheduled.common.RecoveryManager;
import cn.shiyanjun.platform.scheduled.common.GlobalResourceManager;
import cn.shiyanjun.platform.scheduled.common.ResourceManager;
import cn.shiyanjun.platform.scheduled.common.RestManageable;
import cn.shiyanjun.platform.scheduled.common.SchedulingManager;
import cn.shiyanjun.platform.scheduled.common.SchedulingStrategy;
import cn.shiyanjun.platform.scheduled.common.TaskPersistenceService;
import cn.shiyanjun.platform.scheduled.component.DefaultQueueingManager;
import cn.shiyanjun.platform.scheduled.component.DefaultRecoveryManager;
import cn.shiyanjun.platform.scheduled.component.DefaultSchedulingManager;
import cn.shiyanjun.platform.scheduled.component.JobPersistenceServiceImpl;
import cn.shiyanjun.platform.scheduled.component.MaxConcurrencySchedulingStrategy;
import cn.shiyanjun.platform.scheduled.component.RabbitMQAccessService;
import cn.shiyanjun.platform.scheduled.component.ResourceMetadataManagerImpl;
import cn.shiyanjun.platform.scheduled.component.RestManagementExporter;
import cn.shiyanjun.platform.scheduled.component.ScheduledJobFetcher;
import cn.shiyanjun.platform.scheduled.component.TaskPersistenceServiceImpl;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.dao.DaoFactory;
import cn.shiyanjun.platform.scheduled.utils.ConfigUtils;
import cn.shiyanjun.platform.scheduled.utils.ResourceUtils;
import redis.clients.jedis.JedisPool;

public final class ScheduledMain extends AbstractComponent implements LifecycleAware, GlobalResourceManager {

	private static final Log LOG = LogFactory.getLog(ScheduledMain.class);
	private final String queueingConfig = "queueings.properties";
	private final String redisConfig = "redis.properties";
	private final String rabbitmqConfig = "rabbitmq.properties";
	private final String platformId;
	private LifecycleAware scheduledJobFetcher;
	private RecoveryManager recoveryManager;
	private QueueingManager queueingManager;
	private SchedulingManager schedulingManager;
	private JobPersistenceService jobPersistenceService;
	private TaskPersistenceService taskPersistenceService;
	private MQAccessService taskMQAccessService;
	private MQAccessService heartbeatMQAccessService;
	private SchedulingStrategy schedulingStrategy;
	private ResourceManager resourceMetadataManager;
	private RestManageable restManageable;
	
	public ScheduledMain(final Context context) {
		super(context);
		platformId = UUID.randomUUID().toString().replaceAll("\\-", "");
		LOG.info("Platform ID: " + platformId);
	}
	
	@Override
	public String getPlatformId() {
		return platformId;
	}

	@Override
	public void start() {
		// create & cache resource instances
		ResourceUtils.registerResource(redisConfig, JedisPool.class);

		final DaoFactory daoFactory = DaoFactory.newInstance();
		final SqlSessionFactory sqlSessionFactory = daoFactory.getSqlSessionFactory();
		jobPersistenceService = new JobPersistenceServiceImpl(sqlSessionFactory);
		taskPersistenceService = new TaskPersistenceServiceImpl(sqlSessionFactory);

		String taskQName = context.get(ConfigKeys.SCHEDULED_MQ_TASK_QUEUE_NAME);
		String hbQName = context.get(ConfigKeys.SCHEDULED_MQ_HEARTBEAT_QUEUE_NAME);
		final ConnectionFactory connectionFactory = ResourceUtils.registerAndGetResource(rabbitmqConfig, ConnectionFactory.class);
		taskMQAccessService = new RabbitMQAccessService(taskQName, connectionFactory);
		heartbeatMQAccessService = new RabbitMQAccessService(hbQName, connectionFactory);
		
		resourceMetadataManager = new ResourceMetadataManagerImpl(context);
		queueingManager = new DefaultQueueingManager(this);
		scheduledJobFetcher = new ScheduledJobFetcher(this);
		schedulingStrategy = new MaxConcurrencySchedulingStrategy(this);
		schedulingManager = new DefaultSchedulingManager(this);
		recoveryManager = new DefaultRecoveryManager(this);
		restManageable = new RestManagementExporter(this);

		// map job types to Redis queue names
		parseRedisQueueJobRelations();
				
		taskMQAccessService.start();
		recoveryManager.start();
		
		scheduledJobFetcher.start();
		schedulingManager.start();
		queueingManager.start();
	}
	
	private void parseRedisQueueJobRelations() {
		Context ctx = new ContextImpl(queueingConfig);
		Iterator<Object> iter = ctx.keyIterator();
		while(iter.hasNext()) {
			String queueName = iter.next().toString();
			String[] values = ctx.get(queueName).split("\\s*,\\s*");
			int[] types = new int[values.length];
			for(int i=0; i<values.length; i++) {
				types[i] = Integer.parseInt(values[i]);
			}
			queueingManager.registerQueue(queueName, types);
		}
	}
	
	@Override
	public void stop() {
		schedulingManager.stop();
		recoveryManager.stop();
		scheduledJobFetcher.stop();
		queueingManager.stop();
		taskMQAccessService.stop();
		heartbeatMQAccessService.stop();
		ResourceUtils.closeAll();
	}

	@Override
	public ResourceManager getResourceMetadataManager() {
		return resourceMetadataManager;
	}

	@Override
	public JedisPool getJedisPool() {
		return ResourceUtils.getResource(JedisPool.class);
	}

	@Override
	public SchedulingStrategy getSchedulingStrategy() {
		return schedulingStrategy;
	}

	@Override
	public QueueingManager getQueueingManager() {
		return queueingManager;
	}
	
	@Override
	public SchedulingManager getSchedulingManager() {
		return schedulingManager;
	}
	
	@Override
	public JobPersistenceService getJobPersistenceService() {
		return jobPersistenceService;
	}

	@Override
	public TaskPersistenceService getTaskPersistenceService() {
		return taskPersistenceService;
	}
	
	@Override
	public MQAccessService getTaskMQAccessService() {
		return taskMQAccessService;
	}

	@Override
	public MQAccessService getHeartbeatMQAccessService() {
		return heartbeatMQAccessService;
	}
	
	@Override
	public RestManageable getRestManageable() {
		return restManageable;
	}
	
	@Override
	public RecoveryManager getRecoveryManager() {
		return recoveryManager;
	}
	
	public static void main(String[] args) {
		Context context = ConfigUtils.getDefaultContext();
		LifecycleAware component = new ScheduledMain(context);
		component.start();
	}

}
