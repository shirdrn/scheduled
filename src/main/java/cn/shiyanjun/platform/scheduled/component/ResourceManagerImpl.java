package cn.shiyanjun.platform.scheduled.component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.api.utils.Pair;
import cn.shiyanjun.platform.api.utils.Time;
import cn.shiyanjun.platform.scheduled.api.ResourceManager;
import cn.shiyanjun.platform.scheduled.common.StatCounter;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;

public class ResourceManagerImpl implements ResourceManager {
	
	private static final Log LOG = LogFactory.getLog(ResourceManagerImpl.class);
	private final Map<String, Map<TaskType, Integer>> maxConcurrencies = Maps.newHashMap();
	private final Map<String, Integer> queueCapacities = Maps.newHashMap();
	private volatile Map<String, Map<TaskType, AtomicInteger>> counters = Maps.newHashMap();
	private final Map<String, Set<TaskType>> taskTypes = Maps.newHashMap();
	
	private final Map<String, JobStatCounter> jobStatCounters = Maps.newHashMap();
	private final Map<String, TaskStatCounter> taskStatCounters = Maps.newHashMap();
	private final Map<TaskType, Integer> reportedAvailableResources = Maps.newHashMap();
	
	private static final int DEFAULT_MAX_RECYCLE_RESOURCE_CACHE_CAPACITY = 10000;
	private final int maxRecycleResourceCacheCapacity;
	private final ResourceRecyclingGuard guard;
	
	public ResourceManagerImpl(Context context) {
		maxRecycleResourceCacheCapacity = context.getInt(ConfigKeys.SCHEDULED_MAX_RECYCLE_RESOURCE_CACHE_CAPACITY, DEFAULT_MAX_RECYCLE_RESOURCE_CACHE_CAPACITY);
		LOG.info("Configs: maxRecycleResourceCacheCapacity=" + maxRecycleResourceCacheCapacity);
		guard = new ResourceRecyclingGuard();
	}
	
	@Override
	public void registerResource(String queue, List<Pair<TaskType, Integer>> amounts) {
		Preconditions.checkArgument(!maxConcurrencies.containsKey(queue), "Already registered: queue=" + queue);
		maxConcurrencies.put(queue, Maps.newHashMap());
		counters.put(queue, Maps.newHashMap());
		taskTypes.put(queue, Sets.newHashSet());
		jobStatCounters.put(queue, new JobStatCounter(queue));
		taskStatCounters.put(queue, new TaskStatCounter(queue));
		amounts.stream().forEach(p -> {
			taskTypes.get(queue).add(p.getKey());
			maxConcurrencies.get(queue).put(p.getKey(), p.getValue());
			counters.get(queue).put(p.getKey(), new AtomicInteger(0));
		});
		LOG.info("Configured task types: " + taskTypes);
		LOG.info("Configured resources: " + maxConcurrencies);
	}
	
	@Override
	public synchronized void allocateResource(String queue, TaskType taskType) {
		int limit = maxConcurrencies.get(queue).get(taskType);
		final AtomicInteger occupied = counters.get(queue).get(taskType);
		if(occupied.get() < limit) {
			occupied.incrementAndGet();
		}
	}

	@Override
	public void releaseResource(String queue, int jobId, int taskId, TaskType taskType) {
		guard.recycle(queue, new ID(jobId, taskId), taskType, () -> {
			final AtomicInteger occupied = counters.get(queue).get(taskType);
			occupied.decrementAndGet();
		});
	}
	
	final class ResourceRecyclingGuard {
		
		private final Cache<ID, ResourceState> stateCache;
		
		public ResourceRecyclingGuard() {
			super();
			stateCache = CacheBuilder.newBuilder().maximumSize(maxRecycleResourceCacheCapacity).build();
		}
		
		public synchronized boolean recycle(String queue, ID id, TaskType taskType, Runnable action) {
			final ResourceState value = stateCache.getIfPresent(id);
			if(value == null) {
				stateCache.put(id, new ResourceState(id, taskType));
				action.run();
				LOG.info("Cached recycled state: jobId=" + id.jobId + ", taskId=" + id.taskId + ", taskType=" + taskType);
				return true;
			} else {
				LOG.warn("Already recycled resource: state=" + value);
				return false;
			}
		}
	}
	
	final class ID {
		
		private final int jobId;
		private final int taskId;
		
		public ID(int jobId, int taskId) {
			super();
			this.jobId = jobId;
			this.taskId = taskId;
		}
		
		@Override
		public boolean equals(Object obj) {
			ID other = (ID) obj;
			return jobId == other.jobId && taskId == other.taskId;
		}
		
		@Override
		public int hashCode() {
			return 31 * jobId + 31 * taskId;
		}
	}
	
	final class ResourceState {
		
		final ID id;
		final TaskType taskType;
		final long timestamp;
		
		public ResourceState(ID id, TaskType taskType) {
			super();
			this.id = id;
			this.taskType = taskType;
			this.timestamp = Time.now();
		}
		
		@Override
		public String toString() {
			JSONObject info = new JSONObject(true);
			info.put("jobId", id.jobId);
			info.put("taskId", id.taskId);
			info.put("taskType", taskType);
			info.put("timestamp", timestamp);
			return info.toJSONString();
		}
	}
	
	@Override
	public int availableResource(String queue, TaskType taskType) {
		int occupied = counters.get(queue).get(taskType).get();
		int maxAvailableCount = maxConcurrencies.get(queue).get(taskType);
		return maxAvailableCount - occupied;
	}
	
	@Override
	public JobStatCounter getJobStatCounter(String queue) {
		return jobStatCounters.get(queue);
	}
	
	@Override
	public TaskStatCounter getTaskStatCounter(String queue) {
		return taskStatCounters.get(queue);
	}
	
	@Override
	public int getRunningTaskCount(String queue) {
		return counters.get(queue).values().stream()
			.collect(Collectors.summingInt(AtomicInteger::get));
	}
	
	@Override
	public void updateReportedResources(Map<TaskType, Integer> resources) {
		reportedAvailableResources.clear();
		reportedAvailableResources.putAll(resources);
		LOG.info("Available resources: " + resources);
	}
	
	@Override
	public void currentResourceStatuses() {
		LOG.info("-------------------------------------------------------------------------");
		LOG.info("|     CONCURRENCY STATUSES ");
		counters.keySet().forEach(queue -> {
			Map<TaskType, AtomicInteger> cs = counters.get(queue);
			StringBuilder sb = new StringBuilder("|  queue=").append(queue).append(": ");
			cs.keySet().forEach(type -> {
				sb.append("(taskType=").append(type)
					.append(", occupied=").append(cs.get(type).get()).append("/").append(maxConcurrencies.get(queue).get(type)).append(") ");
			});
			LOG.info(sb.toString());
		});
		LOG.info("-------------------------------------------------------------------------");
	}

	@Override
	public Set<TaskType> taskTypes(String queue) {
		return Collections.unmodifiableSet(taskTypes.get(queue));
	}
	
	public static class JobStatCounter extends StatCounter {
		
		private volatile int succeededJobCount = 0;
		private volatile int failedJobCount = 0;
		private volatile int timeoutJobCount = 0;
		private volatile int cancelledJobCount = 0;
		
		public JobStatCounter(String queue) {
			super(queue);
		}
		
		public void incrementSucceededJobCount() {
			succeededJobCount++;
		}
		
		public void incrementFailedJobCount() {
			failedJobCount++;
		}
		
		public void incrementTimeoutJobCount() {
			timeoutJobCount++;
		}
		
		public void incrementCancelledJobCount() {
			cancelledJobCount++;
		}
		
		public JSONObject toJSONObject() {
			JSONObject jo = new JSONObject(true);
			jo.put("succeededJobCount", succeededJobCount);
			jo.put("failedJobCount", failedJobCount);
			jo.put("timeoutJobCount", timeoutJobCount);
			jo.put("cancelledJobCount", cancelledJobCount);
			return jo;
		}
	}

	public static class TaskStatCounter extends StatCounter {
		
		public TaskStatCounter(String queue) {
			super(queue);
		}
		
		private volatile int succeededTaskCount = 0;
		private volatile int failedTaskCount = 0;
		private volatile int timeoutTaskCount = 0;
		
		public void incrementSucceededTaskCount() {
			succeededTaskCount++;
		}
		
		public void incrementFailedTaskCount() {
			failedTaskCount++;
		}
		
		public void incrementTimeoutTaskCount() {
			timeoutTaskCount++;
		}
		
		public JSONObject toJSONObject() {
			JSONObject jo = new JSONObject(true);
			jo.put("succeededTaskCount", succeededTaskCount);
			jo.put("failedTaskCount", failedTaskCount);
			jo.put("timeoutTaskCount", timeoutTaskCount);
			return jo;
		}

	}

	@Override
	public synchronized void updateResourceAmount(String queue, TaskType taskType, int amount) {
		Map<TaskType, Integer> taskResourceLimits = maxConcurrencies.get(queue);
		if(taskResourceLimits != null) {
			Integer oldAmount = taskResourceLimits.get(taskType);
			if(oldAmount != null && amount > 0) {
				final AtomicInteger oldOccupied = counters.get(queue).get(taskType);
				if(amount - oldOccupied.get() > 0) {
					taskResourceLimits.put(taskType, amount);
					LOG.info("Resource updated via REST: queue=" + queue + ", taskType=" + taskType + 
							", oldAmount=" + oldAmount + ", newAmount=" + amount + 
							", occupied=" + oldOccupied.get());
				}
			}
		}
	}

	@Override
	public void registerQueueCapacity(String queue, int capacity) {
		queueCapacities.put(queue, capacity);		
	}

	@Override
	public Map<String, Integer> queueCapacities() {
		return Collections.unmodifiableMap(queueCapacities);
	}

}
