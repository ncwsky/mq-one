<?php
$cfg = array(
    'db2' => [
        'dbms' => 'mysql', //数据库
        'server' => '127.0.0.1',//数据库主机
        'name' => 'mq',    //数据库名称
        'user' => 'root',    //数据库用户
        'pwd' => '123456',    //数据库密码
        'port' => 3306,     // 端口
    ],
    'db' => [ //默认使用sqlite
        'dbms' => 'sqlite', //数据库
        'name' => __DIR__.'/mq.db',
    ],
    'redis' => array(
        'host' => '127.0.0.1',
        'port' => 6379,
        'password' => '123456',
        'select' => 6, //选择库
        'pconnect'=>true, //长连接
    ),
    'log_dir' => __DIR__ . '/log/', //日志记录主目录名称
    'log_size' => 4194304,// 日志文件大小限制
    'log_level' => 1,// 日志记录等级
    // ----- message queue start -----
    'auth_key' => '', // tcp认证key
    'allow_ip' => '', // 允许ip 优先于auth_key
    'queue_prefix' => '', // 前缀
    'data_expired_day' => 1, //数据过期天数
    'data_clear_on_hour' => 10, // 数据每日几时（0-23）清理
    'queue_step' => 60, //队列存储间隔 分钟
    'retry_step' => [ // topic=>[重试间隔值,...] 未配置使用全局值
        //'cmd' => [10, 30, 60, 90, 120, 180],
    ],
    'delay_class' => DelayPHP::class,
    'retry_class' => RetryPHP::class,
    'alarm_interval' => 60, //重复预警间隔 秒
    'alarm_waiting' => 10000, //待处理预警值
    'alarm_retry' => 1000, //重试预警值
    'alarm_fail' => 100, //每分钟失败预警值
    'alarm_callback' => function ($type, $value) { // type:waiting|retry|fail, value:对应的触发值
        //todo
        SrvBase::safeEcho($type . ' -> ' . $value . PHP_EOL);
    },
    // ----- message queue end -----
);
/**
 * @param string $name
 * @return lib_redis
 */
function redis($name = 'redis')
{
    lib_redis::$isExRedis = false;
    return lib_redis::getInstance(GetC($name));
}