package cn.shiyanjun.platform.scheduled.component;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.scheduled.api.ResourceManager;
import cn.shiyanjun.platform.scheduled.common.StatCounter;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;

public class ResourceManagerImpl implements ResourceManager {
	
	private static final Log LOG = LogFactory.getLog(ResourceManagerImpl.class);
	private final Map<String, Map<TaskType, Integer>> maxConcurrencies = Maps.newHashMap();
	private volatile Map<String, Map<TaskType, AtomicInteger>> counters = Maps.newHashMap();
	private final Map<String, Set<TaskType>> taskTypes = Maps.newHashMap();
	
	private final Map<String, JobStatCounter> jobStatCounters = Maps.newHashMap();
	private final Map<String, TaskStatCounter> taskStatCounters = Maps.newHashMap();
	private final Map<TaskType, Integer> reportedAvailableResources = Maps.newHashMap();
	
	public ResourceManagerImpl(Context context) {
		String[] a = context.getStringArray(ConfigKeys.SCHEDULED_TASK_MAX_CONCURRENCIES, null);
		Preconditions.checkArgument(a != null, "Max concurrencies MUST be configured");
		for(String s : a) {
			String[] aa = s.split("\\(");
			String queue = aa[0];
			maxConcurrencies.put(queue, Maps.newHashMap());
			counters.put(queue, Maps.newHashMap());
			taskTypes.put(queue, Sets.newHashSet());
			jobStatCounters.put(queue, new JobStatCounter(queue));
			taskStatCounters.put(queue, new TaskStatCounter(queue));
			String[] rs = aa[1].substring(0, aa[1].length()-1).split(";");
			for(String t : rs) {
				String[] ts = t.split("\\:");
				int type = Integer.parseInt(ts[0]);
				int count = Integer.parseInt(ts[1]);
				Optional<TaskType> taskType = TaskType.fromCode(type);
				taskType.ifPresent(tt -> {
					taskTypes.get(queue).add(tt);
					maxConcurrencies.get(queue).put(tt, count);
					counters.get(queue).put(tt, new AtomicInteger(0));
				});
			}
		}
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
	public synchronized void releaseResource(String queue, TaskType taskType) {
		final AtomicInteger occupied = counters.get(queue).get(taskType);
		occupied.decrementAndGet();
	}
	
	@Override
	public int queryResource(String queue, TaskType taskType) {
		int occupied = counters.get(queue).get(taskType).get();
		int available = maxConcurrencies.get(queue).get(taskType);
		return available - occupied;
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

}
