package cn.shiyanjun.platform.scheduled.api;

import cn.shiyanjun.platform.api.LifecycleAware;

public interface JobFetcher extends LifecycleAware {

	void setSchedulingOpened(boolean isSchedulingOpened);
	boolean isSchedulingOpened();
}
