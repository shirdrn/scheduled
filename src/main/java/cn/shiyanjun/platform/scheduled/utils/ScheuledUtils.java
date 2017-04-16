package cn.shiyanjun.platform.scheduled.utils;

import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import cn.shiyanjun.platform.api.utils.Pair;
import cn.shiyanjun.platform.scheduled.api.QueueSelector;
import cn.shiyanjun.platform.scheduled.api.ResourceManager;
import cn.shiyanjun.platform.scheduled.constants.QueueSelectorType;

public class ScheuledUtils {

	private static final Map<QueueSelectorType, QueueSelector> selectors = Maps.newHashMap();
	static {
		selectors.put(QueueSelectorType.RAMDOM, new RandomQueueSelector());
		selectors.put(QueueSelectorType.CAPACITY, new CapacityQueueSelector());
	}
	
	public static Optional<QueueSelector> getQueueSelector(QueueSelectorType selectorType) {
		return Optional.ofNullable(selectors.get(selectorType));
	}
	
	public static final class RandomQueueSelector implements QueueSelector {

		private ResourceManager resourceManager;
		private final Random rand = new Random();
		private Map<String, Integer> capacities;
		private String[] keys;

		public RandomQueueSelector() {
			super();
		}
		
		@Override
		public void setResourceManager(ResourceManager rm) {
			this.resourceManager = rm;
			capacities = resourceManager.queueCapacities();
			keys = new String[capacities.size()];
			capacities.keySet().toArray(keys);
		}

		@Override
		public Optional<String> selectQueue() {
			Preconditions.checkArgument(resourceManager != null, "ResourceManager not set!");
			// select a queue randomly
			int selectedIndex = rand.nextInt(capacities.size());
			return Optional.of(keys[selectedIndex]);
		}
	}

	public static final class CapacityQueueSelector implements QueueSelector {

		private ResourceManager resourceManager;
		private final Random rand = new Random();
		private Map<String, Integer> capacities;

		public CapacityQueueSelector() {
			super();
		}
		
		@Override
		public void setResourceManager(ResourceManager rm) {
			this.resourceManager = rm;
			capacities = resourceManager.queueCapacities();
		}

		@Override
		public Optional<String> selectQueue() {
			Preconditions.checkArgument(resourceManager != null, "ResourceManager not set!");
			final LinkedList<Holder> percentageAxis = Lists.newLinkedList();
			final AtomicInteger current = new AtomicInteger(0);
			capacities.keySet().forEach(queue -> {
				int start = current.get();
				int end = start + capacities.get(queue);
				percentageAxis.add(new Holder(queue, new Pair<>(start, end)));
				current.set(end);
			});

			final int selected = rand.nextInt(current.get());
			return percentageAxis.stream().filter(h -> selected >= h.range.getKey() && selected < h.range.getValue())
					.map(h -> h.queue).findFirst();
		}

		class Holder {
			final Pair<Integer, Integer> range;
			final String queue;

			public Holder(String queue, Pair<Integer, Integer> range) {
				super();
				this.queue = queue;
				this.range = range;
			}
		}
	}
}
