<?php
$cfg = array(
    'db2' => [
        'dbms' => 'mysql', //数据库
        'server' => '192.168.0.219',//数据库主机
        'name' => 'mq',    //数据库名称
        'user' => 'root',    //数据库用户
        'pwd' => '123456',    //数据库密码
        'port' => 3306,     // 端口
    ],
    'db' => [
        'dbms' => 'sqlite', //数据库
        'name' => __DIR__.'/mq.db',
    ],
    'redis' => array(
        'host' => '192.168.0.246',
        'port' => 6379,
        'password' => '123456',
        'select' => 6, //选择库
        'pconnect'=>true, //长连接
    ),
    'log_dir' => __DIR__ . '/log/', //日志记录主目录名称
    'log_size' => 4194304,// 日志文件大小限制
    'log_level' => 1,// 日志记录等级
    // ----- message queue start -----
    'auth_key'=>'', // tcp认证key
    'allow_ip' => '', // 允许ip 优先于auth_key
    'queue_prefix' => '', // 前缀
    'data_expired_day' => 1, //数据过期天数
    'data_clear_on_hour' => 12, // 数据每日几时（0-23）清理
    'queue_step'=> 60, //队列存储间隔 分钟
    'retry_step' => [ // topic=>[重试间隔值,...] 未配置使用全局值
        //'cmd' => [10, 30, 60, 90, 120, 180],
    ],
    // ----- message queue end -----
);