CREATE DATABASE IF NOT EXISTS `mq` DEFAULT CHARSET utf8mb4; 

use `mq`;

-- 队列模板表
-- DROP TABLE IF EXISTS `message_queue`
CREATE TABLE IF NOT EXISTS `message_queue` (
  `id` bigint NOT NULL, -- AUTO_INCREMENT
  `ctime` int NOT NULL COMMENT '记录时间',
  `mtime` int NOT NULL COMMENT '变动时间',
  `status` tinyint DEFAULT '0' COMMENT '状态 -1失败 0未处理 1已完成 2执行中',
  `retry` tinyint DEFAULT '0' COMMENT '重试数 0不重试 最高程序限制',
  `retry_count` tinyint DEFAULT '0',
  `ack` tinyint DEFAULT '0' COMMENT '是否应答',
  `seq_id` int UNSIGNED DEFAULT '0' COMMENT '顺序id 通过此值来分配到同一队列 用于顺序处理',
  `host` varchar(50) DEFAULT '',
  `to` varchar(50) DEFAULT '' COMMENT '指定处理 空|all|g_group|u_user',
  `topic` varchar(50) NOT NULL,
  `data` text NOT NULL COMMENT '数据',
  `result` text COMMENT '执行结果',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- 重试记录表
-- DROP TABLE IF EXISTS `message_queue_retry`
CREATE TABLE IF NOT EXISTS `message_queue_retry` (
  `id` bigint NOT NULL,
  `ctime` int NOT NULL,
  `queue_str` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 记录队列名及处理状态
-- DROP TABLE IF EXISTS `mq_list`
CREATE TABLE IF NOT EXISTS `mq_list` (
  `name` varchar(10) NOT NULL COMMENT '队列名',
  `ctime` int NOT NULL COMMENT '记录时间',
  `exptime` int NOT NULL COMMENT '过期时间',
  `last_id` bigint DEFAULT '0' COMMENT '上次处理的id',
  `end_id` bigint DEFAULT '0' COMMENT '最后的id',
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;