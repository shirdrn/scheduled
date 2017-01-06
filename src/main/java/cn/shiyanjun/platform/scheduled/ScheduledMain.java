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
import cn.shiyanjun.platform.api.utils.ComponentUtils;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.JobController;
import cn.shiyanjun.platform.scheduled.api.JobFetcher;
import cn.shiyanjun.platform.scheduled.api.JobPersistenceService;
import cn.shiyanjun.platform.scheduled.api.MQAccessService;
import cn.shiyanjun.platform.scheduled.api.QueueingManager;
import cn.shiyanjun.platform.scheduled.api.RecoveryManager;
import cn.shiyanjun.platform.scheduled.api.ResourceManager;
import cn.shiyanjun.platform.scheduled.api.RestExporter;
import cn.shiyanjun.platform.scheduled.api.RestServer;
import cn.shiyanjun.platform.scheduled.api.SchedulingManager;
import cn.shiyanjun.platform.scheduled.api.SchedulingPolicy;
import cn.shiyanjun.platform.scheduled.api.TaskPersistenceService;
import cn.shiyanjun.platform.scheduled.component.JobPersistenceServiceImpl;
import cn.shiyanjun.platform.scheduled.component.MaxConcurrencySchedulingPolicy;
import cn.shiyanjun.platform.scheduled.component.QueueingManagerImpl;
import cn.shiyanjun.platform.scheduled.component.RabbitMQAccessService;
import cn.shiyanjun.platform.scheduled.component.RecoveryManagerImpl;
import cn.shiyanjun.platform.scheduled.component.ResourceManagerImpl;
import cn.shiyanjun.platform.scheduled.component.ScheduledJobFetcher;
import cn.shiyanjun.platform.scheduled.component.ScheduledRestExporter;
import cn.shiyanjun.platform.scheduled.component.ScheduledRestServer;
import cn.shiyanjun.platform.scheduled.component.SchedulingManagerImpl;
import cn.shiyanjun.platform.scheduled.component.TaskPersistenceServiceImpl;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.dao.DaoFactory;
import cn.shiyanjun.platform.scheduled.rest.CancelJobServlet;
import cn.shiyanjun.platform.scheduled.rest.MaintenanceServlet;
import cn.shiyanjun.platform.scheduled.rest.QueueingServlet;
import cn.shiyanjun.platform.scheduled.rest.ResourceServlet;
import cn.shiyanjun.platform.scheduled.rest.SchedulingServlet;
import cn.shiyanjun.platform.scheduled.utils.ConfigUtils;
import cn.shiyanjun.platform.scheduled.utils.ResourceUtils;
import redis.clients.jedis.JedisPool;

public final class ScheduledMain extends AbstractComponent implements LifecycleAware, ComponentManager {

	private static final Log LOG = LogFactory.getLog(ScheduledMain.class);
	private final String queueingConfig = "queueings.properties";
	private final String redisConfig = "redis.properties";
	private final String rabbitmqConfig = "rabbitmq.properties";
	private final String platformId;
	private JobFetcher jobFetcher;
	private RecoveryManager recoveryManager;
	private QueueingManager queueingManager;
	private SchedulingManager schedulingManager;
	private JobPersistenceService jobPersistenceService;
	private TaskPersistenceService taskPersistenceService;
	private MQAccessService taskMQAccessService;
	private MQAccessService heartbeatMQAccessService;
	private SchedulingPolicy schedulingPolicy;
	private ResourceManager resourceMetadataManager;
	private RestExporter restManageable;
	private RestServer restServer;
	
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
		try {
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
			
			resourceMetadataManager = new ResourceManagerImpl(context);
			queueingManager = new QueueingManagerImpl(this);
			jobFetcher = new ScheduledJobFetcher(this);
			schedulingPolicy = new MaxConcurrencySchedulingPolicy(this);
			schedulingManager = new SchedulingManagerImpl(this);
			recoveryManager = new RecoveryManagerImpl(this);
			restManageable = new ScheduledRestExporter(this);
			configureRestServer();

			// map job types to Redis queue names
			parseRedisQueueJobRelations();
					
			taskMQAccessService.start();
			recoveryManager.start();
			
			jobFetcher.start();
			schedulingManager.start();
			queueingManager.start();
			restServer.start();
		} catch (Exception e) {
			stop();
		}
	}

	private void configureRestServer() {
		restServer = new ScheduledRestServer(this);
		restServer.register("/admin/resource", ResourceServlet.class);
		restServer.register("/admin/queueing", QueueingServlet.class);
		restServer.register("/admin/scheduling", SchedulingServlet.class);
		restServer.register("/admin/maintenance", MaintenanceServlet.class);
		restServer.register("/admin/cancelJob", CancelJobServlet.class);
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
		try {
			ComponentUtils.stopAllQuietly(
					schedulingManager, recoveryManager, jobFetcher, queueingManager, 
					taskMQAccessService, heartbeatMQAccessService, restServer);
			ResourceUtils.closeAll();
		} catch (Exception e) {}
	}

	@Override
	public ResourceManager getResourceManager() {
		return resourceMetadataManager;
	}

	@Override
	public JedisPool getJedisPool() {
		return ResourceUtils.getResource(JedisPool.class);
	}

	@Override
	public SchedulingPolicy getSchedulingPolicy() {
		return schedulingPolicy;
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
	public RestExporter getRestExporter() {
		return restManageable;
	}
	
	@Override
	public RecoveryManager getRecoveryManager() {
		return recoveryManager;
	}
	
	@Override
	public JobFetcher getJobFetcher() {
		return jobFetcher;
	}
	
	@Override
	public JobController getJobController() {
		return schedulingManager;
	}
	
	public static void main(String[] args) {
		Context context = ConfigUtils.getDefaultContext();
		LifecycleAware component = new ScheduledMain(context);
		component.start();
	}

}
