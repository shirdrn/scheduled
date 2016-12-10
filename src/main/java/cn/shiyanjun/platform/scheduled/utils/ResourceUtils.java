package cn.shiyanjun.platform.scheduled.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Throwables;
import com.rabbitmq.client.ConnectionFactory;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.common.ContextImpl;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class ResourceUtils {

	private static final ConcurrentMap<Class<?>, Object> pooledInstances = new ConcurrentHashMap<>();
	private static final ConcurrentMap<Class<?>, String> pooledConfigurations = new ConcurrentHashMap<>();
	private static final Map<Class<?>, ResourceBuilder<?>> builders = new HashMap<>();
	
	static {
		// register resource builder instances
		builders.put(JedisPool.class, new JedisPoolBuilder());
		builders.put(ConnectionFactory.class, new RabbitConnectionFactoryBuilder());
	}
	
	@SuppressWarnings({ "unchecked" })
	public static synchronized <T> void registerResource(String config, Class<T> resourceClazz) {
		try {
			ResourceBuilder<?> builder = builders.get(resourceClazz);
			T resource = (T) builder.build(config);
			pooledInstances.putIfAbsent(resourceClazz, resource);
			pooledConfigurations.putIfAbsent(resourceClazz, config);
		} catch (Exception e) {
			Throwables.propagate(e);
		}
	}
	
	@SuppressWarnings({ "unchecked" })
	public static synchronized <T> T registerAndGetResource(String config, Class<T> resourceClazz) {
		try {
			ResourceBuilder<?> builder = builders.get(resourceClazz);
			T resource = (T) builder.build(config);
			pooledInstances.putIfAbsent(resourceClazz, resource);
			pooledConfigurations.putIfAbsent(resourceClazz, config);
		} catch (Exception e) {
			Throwables.propagate(e);
		}
		return (T) pooledInstances.get(resourceClazz);
	}
	
	@SuppressWarnings({ "unchecked" })
	public static <T> T getResource(Class<T> resourceClazz) {
		return (T) pooledInstances.get(resourceClazz);
	}
	
	
	interface ResourceBuilder<T> {
		T build(String config) throws Exception;
	}
	
	public static void closeAll() {
		for(Class<?> clazz : pooledInstances.keySet()) {
			try {
				Object o = pooledInstances.get(clazz);
				if(o instanceof JedisPool) {
					((JedisPool) o).close();
				}
			} catch (Exception e) {
				// ignore it
			}
		}
	}
	
	private static abstract class AbstractResourceBuilder {
		
		protected String getString(Context context, String key, String defaultValue) {
			return context.get(key, defaultValue);
		}
		
		protected int getInt(Context context, String key, int defaultValue) {
			return context.getInt(key, defaultValue);
		}
		
		protected boolean getBool(Context context, String key, boolean defaultValue) {
			return context.getBoolean(key, defaultValue);
		}
	}
	
	private  static final class RabbitConnectionFactoryBuilder extends AbstractResourceBuilder implements ResourceBuilder<ConnectionFactory> {

		private static String RABBITMQ_HOST = "rabbitmq.host";
		private static String RABBITMQ_PORT = "rabbitmq.port";
		private static String RABBITMQ_USERNAME = "rabbitmq.username";
		private static String RABBITMQ_PASSWORD = "rabbitmq.password";
		private static String RABBITMQ_CONNECT_TIMEOUT = "rabbitmq.connect.timeout";
		private static String RABBITMQ_AUTOMATIC_RECOVERY = "rabbitmq.automatic.recovery";
		
		@Override
		public ConnectionFactory build(String config) throws Exception {
			final Context context = new ContextImpl(config);
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(getString(context, RABBITMQ_HOST, "localhost"));
			factory.setPort(getInt(context, RABBITMQ_PORT, 5672));
			factory.setUsername(getString(context, RABBITMQ_USERNAME, null));
			factory.setPassword(getString(context, RABBITMQ_PASSWORD, null));
			factory.setConnectionTimeout(context.getInt(RABBITMQ_CONNECT_TIMEOUT, 30000));
			factory.setAutomaticRecoveryEnabled(context.getBoolean(RABBITMQ_AUTOMATIC_RECOVERY, true));
			return factory;
		}
		
	}
	
	private static final class JedisPoolBuilder extends AbstractResourceBuilder implements ResourceBuilder<JedisPool> {

		private static String REDIS_HOST = "redis.host";
		private static String REDIS_PORT = "redis.port";
		private static String REDIS_PASSWORD = "redis.password";
		private static String REDIS_TIMEOUT = "redis.timeout";
		private static String REDIS_POOL_MAX_TOTAL = "redis.pool.maxTotal";
		private static String REDIS_POOL_MAX_IDLE = "redis.pool.maxIdle";
		private static String REDIS_POOL_MIN_IDLE = "redis.pool.minIdle";
		private static String REDIS_POOL_MAX_WAIT_MILLIS = "redis.pool.maxWaitMillis";
		private static String REDIS_POOL_TEST_ON_BORROW = "redis.pool.testOnBorrow";
		private static String REDIS_POOL_TEST_ON_RETURN = "redis.pool.testOnReturn";

		@Override
		public JedisPool build(String config) throws Exception {
			JedisPoolConfig jpc = new JedisPoolConfig();
			final Context context = new ContextImpl(config);
			jpc.setMaxTotal(getInt(context, REDIS_POOL_MAX_TOTAL, 1));
			jpc.setMaxIdle(getInt(context, REDIS_POOL_MAX_IDLE, 1));
			jpc.setMinIdle(getInt(context, REDIS_POOL_MIN_IDLE, 1));
			jpc.setMaxWaitMillis(getInt(context, REDIS_POOL_MAX_WAIT_MILLIS, 30000));
			jpc.setTestOnBorrow(getBool(context, REDIS_POOL_TEST_ON_BORROW, true));
			jpc.setTestOnReturn(getBool(context, REDIS_POOL_TEST_ON_RETURN, false));
			
			String host = getString(context, REDIS_HOST, "localhost");
			int port = getInt(context, REDIS_PORT, 6379);
			String password = context.get(REDIS_PASSWORD);
			if(password != null) {
				int timeout = getInt(context, REDIS_TIMEOUT, 30000);
				return new JedisPool(jpc, host, port, timeout, password);
			}
			return new JedisPool(jpc, host, port);
		}
		
	}
}
