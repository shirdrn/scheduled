package cn.shiyanjun.platform.scheduled.common;

public interface Dispatcher<T> {

	/**
	 * Dispatches a job to the queueing component.
	 * @param job
	 */
	void dispatch(T job);
}
