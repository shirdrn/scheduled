package cn.shiyanjun.platform.scheduled.api;

public interface ResponseHandler<T> {

	void handle(final T response);
}
