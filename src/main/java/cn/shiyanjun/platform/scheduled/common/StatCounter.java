package cn.shiyanjun.platform.scheduled.common;

public abstract class StatCounter {
	
	protected final String queue;
	
	public StatCounter(String queue) {
		this.queue = queue;
	}
	
	public String getQueue() {
		return queue;
	}
}