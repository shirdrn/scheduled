package cn.shiyanjun.platform.scheduled.api;

import cn.shiyanjun.platform.api.Context;

public interface StorageService {

	Context getContext();
	JobPersistenceService getJobPersistenceService();
	TaskPersistenceService getTaskPersistenceService();
}
