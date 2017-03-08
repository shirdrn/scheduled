package cn.shiyanjun.platform.scheduled.constants;

public interface ConfigKeys {

	String SCHEDULED_WEB_MANAGER_PORT = "scheduled.web.manager.port";
	String SCHEDULED_FETCH_JOB_INTERVAL_MILLIS = "scheduled.fetch.job.interval.millis";
	String SCHEDULED_QUEUEING_QUEUE_NAMES = "scheduled.queueing.queue.name";
	String SCHEDULED_STALE_JOB_CHECK_INTERVAL_SECS = "scheduled.stale.job.check.interval.secs";
	String SCHEDULED_STALE_JOB_MAX_THRESHOLD_SECS = "scheduled.stale.job.max.threshold.secs";
	String SCHEDULED_MQ_TASK_QUEUE_NAME = "scheduled.mq.task.queue.name";
	String SCHEDULED_MQ_HEARTBEAT_QUEUE_NAME = "scheduled.mq.heartbeat.queue.name";
	String SCHEDULED_MQ_RESOURCE_QUEUE_NAME = "scheduled.mq.resource.queue.name";
	String SCHEDULED_QUEUEING_REDIS_DB_INDEX = "scheduled.queueing.redis.db.index";
	String SCHEDULED_QUEUEING_SELECTOR_NAME = "scheduled.queueing.selector.name";
	String SCHEDULED_KEPT_HISTORY_JOB_MAX_COUNT = "scheduled.kept.history.job.max.count";
	String SCHEDULED_KEPT_TIMEOUT_JOB_MAX_COUNT = "scheduled.kept.timeout.job.max.count";
	String SCHEDULED_RECOVERY_FEATURE_ENABLED = "scheduled.recovery.feature.enabled";
	String SCHEDULED_MAINTENANCE_TIME_SEGMENT_START = "scheduled.maintenance.time.segment.start";
	String SCHEDULED_MAINTENANCE_TIME_SEGMENT_END = "scheduled.maintenance.time.segment.end";
	String SCHEDULED_MAX_RECYCLE_RESOURCE_CACHE_CAPACITY = "scheduled.max.recycle.resource.cache.capacity";
	
	String SERVICE_JOB_ORCHESTRATE_PROTOCOL = "service.job.orchestrate.protocol";
	String SERVICE_JOB_FETCH_PROTOCOL = "service.job.fetch.protocol";
}
