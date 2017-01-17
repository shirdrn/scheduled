CREATE DATABASE IF NOT EXISTS scheduled_db DEFAULT CHARSET utf8;

CREATE TABLE IF NOT EXISTS `job` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `job_type` int(11) NOT NULL,
  `params` varchar(20480) NOT NULL,
  `status` tinyint(4) NOT NULL DEFAULT 0 COMMENT 'Job statuses: 0-Created, 1-Queueing, 2-Scheduled, 3-Submitted, 4-Running, 5-Succeeded, 6-Failed, 7-Cancelled, 8-Timeout',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `done_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `job_id` int(11) NOT NULL,
  `seq_no` int(11) NOT NULL,
  `task_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '任务类型: 0-Unknown; 1-GreenPlum; 2-Spark; 3-JavaApp; 4-MapReduce; 5-Flink; 6-RestCallback; 7-RestPolling',
  `params` text,
  `status` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'Task statuses: 0-Created, 1-Queueing, 2-Scheduled, 3-Submitted, 4-Running, 5-Succeeded, 6-Failed, 7-Cancelled, 8-Timeout',
  `result_count` bigint(20) DEFAULT NULL,
  `start_time` timestamp NULL DEFAULT NULL,
  `done_time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_index_job_id_seq_no` (`job_id`,`seq_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;