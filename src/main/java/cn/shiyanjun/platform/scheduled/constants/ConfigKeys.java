package cn.shiyanjun.platform.scheduled.constants;

public interface ConfigKeys {

	String SCHEDULED_WEB_MANAGER_PORT = "scheduled.web.manager.port";
	String SCHEDULED_FETCH_JOB_INTERVAL_MILLIS = "scheduled.fetch.job.interval.millis";
	String SCHEDULED_QUEUEING_QUEUE_NAMES = "scheduled.queueing.queue.name";
	String SCHEDULED_STALE_TASK_CHECK_INTERVAL_SECS = "scheduled.stale.task.check.interval.secs";
	String SCHEDULED_STALE_TASK_MAX_THRESHOLD_SECS = "scheduled.stale.task.max.threshold.secs";
	String SCHEDULED_MQ_TASK_QUEUE_NAME = "scheduled.mq.task.queue.name";
	String SCHEDULED_MQ_HEARTBEAT_QUEUE_NAME = "scheduled.mq.heartbeat.queue.name";
	String SCHEDULED_MQ_RESOURCE_QUEUE_NAME = "scheduled.mq.resource.queue.name";
	String SCHEDULED_QUEUEING_REDIS_DB_INDEX = "scheduled.queueing.redis.db.index";
	String SCHEDULED_TASK_MAX_CONCURRENCIES = "scheduled.task.max.concurrencies";
	String SCHEDULED_KEPT_HISTORY_JOB_MAX_COUNT = "scheduled.kept.history.job.max.count";
	String SCHEDULED_KEPT_TIMEOUT_JOB_MAX_COUNT = "scheduled.kept.timeout.job.max.count";
	String SCHEDULED_RECOVERY_FEATURE_ENABLED = "scheduled.recovery.feature.enabled";
	
	String SERVICE_JOB_ORCHESTRATE_PROTOCOL = "service.job.orchestrate.protocol";
	String SERVICE_JOB_ORCHESTRATE_URL = "service.job.orchestrate.url";
	
	String SERVICE_JOB_FETCH_PROTOCOL = "service.job.fetch.protocol";
}
