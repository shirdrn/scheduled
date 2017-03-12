package cn.shiyanjun.platform.scheduled;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ibatis.session.SqlSessionFactory;

import com.google.common.base.Strings;
import com.rabbitmq.client.ConnectionFactory;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.LifecycleAware;
import cn.shiyanjun.platform.api.common.AbstractComponent;
import cn.shiyanjun.platform.api.common.ContextImpl;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.api.utils.ComponentUtils;
import cn.shiyanjun.platform.api.utils.Pair;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.JobFetcher;
import cn.shiyanjun.platform.scheduled.api.JobPersistenceService;
import cn.shiyanjun.platform.scheduled.api.MQAccessService;
import cn.shiyanjun.platform.scheduled.api.QueueingManager;
import cn.shiyanjun.platform.scheduled.api.ResourceManager;
import cn.shiyanjun.platform.scheduled.api.RestExporter;
import cn.shiyanjun.platform.scheduled.api.RestServer;
import cn.shiyanjun.platform.scheduled.api.ScheduledController;
import cn.shiyanjun.platform.scheduled.api.SchedulingManager;
import cn.shiyanjun.platform.scheduled.api.StateManager;
import cn.shiyanjun.platform.scheduled.api.StorageService;
import cn.shiyanjun.platform.scheduled.api.TaskPersistenceService;
import cn.shiyanjun.platform.scheduled.component.JobPersistenceServiceImpl;
import cn.shiyanjun.platform.scheduled.component.QueueingManagerImpl;
import cn.shiyanjun.platform.scheduled.component.RabbitMQAccessService;
import cn.shiyanjun.platform.scheduled.component.ResourceManagerImpl;
import cn.shiyanjun.platform.scheduled.component.ScheduledControllerImpl;
import cn.shiyanjun.platform.scheduled.component.ScheduledJobFetcher;
import cn.shiyanjun.platform.scheduled.component.ScheduledRestExporter;
import cn.shiyanjun.platform.scheduled.component.ScheduledRestServer;
import cn.shiyanjun.platform.scheduled.component.SchedulingManagerImpl;
import cn.shiyanjun.platform.scheduled.component.StateManagerImpl;
import cn.shiyanjun.platform.scheduled.component.TaskPersistenceServiceImpl;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.dao.DaoFactory;
import cn.shiyanjun.platform.scheduled.rest.CancelJobServlet;
import cn.shiyanjun.platform.scheduled.rest.MaintenanceServlet;
import cn.shiyanjun.platform.scheduled.rest.QueueingServlet;
import cn.shiyanjun.platform.scheduled.rest.ResourceServlet;
import cn.shiyanjun.platform.scheduled.rest.SchedulingServlet;
import cn.shiyanjun.platform.scheduled.utils.ConfigUtils;
import cn.shiyanjun.platform.scheduled.utils.HookUtils;
import cn.shiyanjun.platform.scheduled.utils.ResourceUtils;
import redis.clients.jedis.JedisPool;

public final class ScheduledMain extends AbstractComponent implements LifecycleAware, ComponentManager, StorageService {

	private static final Log LOG = LogFactory.getLog(ScheduledMain.class);
	private final String queueingConfig = "queueings.properties";
	private final String redisConfig = "redis.properties";
	private final String rabbitmqConfig = "rabbitmq.properties";
	private final String platformId;
	private JobFetcher jobFetcher;
	private QueueingManager queueingManager;
	private SchedulingManager schedulingManager;
	private JobPersistenceService jobPersistenceService;
	private TaskPersistenceService taskPersistenceService;
	private MQAccessService taskMQAccessService;
	private MQAccessService heartbeatMQAccessService;
	private ResourceManager resourceManager;
	private RestExporter restManageable;
	private RestServer restServer;
	private StateManager stateManager;
	private ScheduledController scheduledController;
	
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
			
			stateManager = new StateManagerImpl(this);
			resourceManager = new ResourceManagerImpl(context);
			queueingManager = new QueueingManagerImpl(this);
			stateManager.setQueueingManager(queueingManager);
			
			jobFetcher = new ScheduledJobFetcher(this);
			schedulingManager = new SchedulingManagerImpl(this);
			restManageable = new ScheduledRestExporter(this);
			configureRestServer();
			
			scheduledController = new ScheduledControllerImpl(this);

			// map job types to Redis queue names
			parseRedisQueueRelatedConfigs();
					
			// add shutdown hook for releasing port resource
			HookUtils.addShutdownHook(() -> restServer.stop());
			
			ComponentUtils.startAll(taskMQAccessService, 
					jobFetcher, schedulingManager, queueingManager, restServer);
		} catch (Exception e) {
			LOG.error(e);
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
	
	protected void parseRedisQueueRelatedConfigs() {
		Context ctx = new ContextImpl(queueingConfig);
		Iterator<Object> iter = ctx.keyIterator();
		while(iter.hasNext()) {
			String queue = iter.next().toString();
			String value = ctx.get(queue);
			
			// job:1,2,3|1:1,2:1|80
			if(!Strings.isNullOrEmpty(value)) {
				String[] values = value.split("\\|");
				String jobConfig = values[0].split(":")[1];
				String taskConfig = values[1];
				String capacity = values[2];
				int[] jobTypes = ConfigUtils.stringsToInts(jobConfig.split(","));
				queueingManager.registerQueue(queue, jobTypes);
				List<Pair<TaskType, Integer>> taskTypes = ConfigUtils.parsePairStrings(taskConfig).stream()
						.map(p -> new Pair<TaskType, Integer>(TaskType.fromCode(p.getKey()).get(), p.getValue()))
						.collect(Collectors.toList());
				resourceManager.registerResource(queue, taskTypes);
				resourceManager.registerQueueCapacity(queue, Integer.parseInt(capacity));
			}
		}
	}
	
	@Override
	public void stop() {
		try {
			ComponentUtils.stopAllQuietly(
					schedulingManager, jobFetcher, queueingManager, 
					taskMQAccessService, heartbeatMQAccessService, restServer);
			ResourceUtils.closeAll();
		} catch (Exception e) {}
	}

	@Override
	public ResourceManager getResourceManager() {
		return resourceManager;
	}

	@Override
	public JedisPool getJedisPool() {
		return ResourceUtils.getResource(JedisPool.class);
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
	public JobFetcher getJobFetcher() {
		return jobFetcher;
	}
	
	@Override
	public StateManager getStateManager() {
		return stateManager;
	}
	
	@Override
	public ScheduledController getScheduledController() {
		return scheduledController;
	}
	
	public static void main(String[] args) {
		Context context = ConfigUtils.getDefaultContext();
		LifecycleAware component = new ScheduledMain(context);
		component.start();
	}

}
