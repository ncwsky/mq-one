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
    'memory_limit'=>'512M', //内存限制 大量队列堆积会造成内存不足 需要调整限制
    'auth_key' => '', // tcp认证key
    'allow_ip' => '', // 允许ip 优先于auth_key
    'prefix' => '', // 前缀
    'max_waiting_num' => 50000, //allow_waiting_num 0不限制 超出此值新推送的消息将会丢弃并返回失败
    'data_expired' => 1440, //数据过期分钟 至少是queue_step的3倍以上 并且要大于重试合计总时长
    'queue_step' => 60, //队列存储间隔 分钟
    'topic_multi_split' => "\r", //多个同topic push/pop消息分隔符
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
        Log::write($type . ' -> ' . $value, 'alarm');
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