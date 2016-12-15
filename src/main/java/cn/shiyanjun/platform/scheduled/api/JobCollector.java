package cn.shiyanjun.platform.scheduled.api;

public interface JobCollector<T> {

	/**
	 * Collect a job for the queueing component.
	 * @param job
	 */
	void collect(T job);
}
