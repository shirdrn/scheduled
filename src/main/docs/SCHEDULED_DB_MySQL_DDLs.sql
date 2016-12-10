CREATE DATABASE IF NOT EXISTS scheduled_db DEFAULT CHARSET utf8;

CREATE TABLE IF NOT EXISTS `job` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `job_type` int(11) NOT NULL,
  `params` varchar(20480) NOT NULL,
  `status` tinyint(4) NOT NULL DEFAULT 0 COMMENT '作业状态: 0-已创建; 1-已提交; 2-排队中; 3-已拉取; 4-已调度; 5-运行中; 6-已成功; 7-已失败',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `done_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `job_id` int(11) NOT NULL,
  `serial_no` int(11) NOT NULL,
  `task_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '任务类型: 0-Unknown; 1-GreenPlum; 2-Spark; 3-JavaApp; 4-MapReduce; 5-Flink; 6-RestCallback; 7-RestPolling',
  `params` text,
  `status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '任务状态: 0-已创建; 1-已提交; 2-排队中; 3-已调度; 4-运行中; 5-已成功; 6-已失败',
  `result_count` bigint(20) DEFAULT NULL,
  `start_time` timestamp NULL DEFAULT NULL,
  `done_time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_index_job_id_serial_no` (`job_id`,`serial_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;