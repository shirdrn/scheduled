package cn.shiyanjun.platform.scheduled.common;
//package cn.shiyanjun.ddc.scheduling.platform.common;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class ResourceMetadata {
//	
//	private static final Logger log = LoggerFactory.getLogger(ResourceMetadata.class);
//	private final TaskStatisticsCounters taskStatisticsCounters;
//	private final Map<Integer, Integer> taskConcurrencyLimits = new HashMap<>();
//	private final Map<Integer, AtomicInteger> taskCounters = new HashMap<>();
//	/** taskType -> availableResourceCount */
//	private final Map<Integer, AtomicInteger> availableTaskResources = new HashMap<>();
//	
//	public ResourceMetadata() {
//		super();
//		taskStatisticsCounters = new TaskStatisticsCounters();
//	}
//	
//	public synchronized void configureTaskConcurrencyLimits(int taskType, int limit) {
//		taskConcurrencyLimits.put(taskType, limit);
//		availableTaskResources.put(taskType, new AtomicInteger(limit));
//	}
//	
//	public int getTaskConcurrencyLimitForType(int taskType) {
//		return taskConcurrencyLimits.get(taskType);
//	}
//	
//	public void registerTaskType(int taskType) {
//		taskCounters.put(taskType, new AtomicInteger(0));
//	}
//
//	public TaskStatisticsCounters getTaskStatisticsCounters() {
//		return taskStatisticsCounters;
//	}
//	
//	public int incrementTaskCountForType(int taskType) {
//		log.debug("incrementTaskCount for taskType:[{}]", taskType);
//		return taskCounters.get(taskType).incrementAndGet();
//	}
//	
//	public int decrementTaskCountForType(int taskType) {
//		log.debug("decrementTaskCount for taskType:[{}]", taskType);
//		return taskCounters.get(taskType).addAndGet(-1);
//	}
//
//	public int runningTaskCountForType(int taskType){
//		return taskCounters.get(taskType).get();
//	}
//	
//	public int getTaskCountForType(int taskType) {
//		return taskCounters.get(taskType).get();
//	}
//	
//	public Set<Integer> getTaskTypes() {
//		return taskCounters.keySet();
//	}
//	
//	
//
//}
