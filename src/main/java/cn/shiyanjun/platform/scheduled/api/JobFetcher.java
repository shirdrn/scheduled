package cn.shiyanjun.platform.scheduled.api;

import cn.shiyanjun.platform.api.LifecycleAware;
import cn.shiyanjun.platform.api.utils.Pair;

public interface JobFetcher extends LifecycleAware {

	void setSchedulingOpened(boolean isSchedulingOpened);
	boolean isSchedulingOpened();
	
	Pair<String, String> getMaintenanceTimeSegment();
	void updateMaintenanceTimeSegment(String startTime, String endTime);
}
