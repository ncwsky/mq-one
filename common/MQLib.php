<?php

/**
 * 公共函数类库
 * Class MQLib
 * @package MyMQ
 */
class MQLib
{
    use MQMsg;
    //开关字节位值
    const STATUS_ACK = 1;
    const STATUS_SYNC = 2;

    const STATUS_FAIL = -1;
    const STATUS_TODO = 0;
    const STATUS_DONE = 1;
    const STATUS_EXECUTING = 2;
    const STATUS_TIMEOUT = 3;
    const STATUS_RETRY = 4;

    const MAX_TOPIC_LENGTH = 50;
    const MAX_DATA_LENGTH = 64000; //65535;

    const MQ_LIST_TABLE = 'mq_list';
    const QUEUE_TABLE_PREFIX = 'message_queue';
    const QUEUE_RETRY_TABLE = 'message_queue_retry';
    const QUEUE_DELAY_TABLE = 'message_queue_delay';
    /**
     * 等待的缓存列队名
     * @var string
     */
    const QUEUE_WAITING = 'mq_waiting';

    /**
     * 失败的缓存列队名
     * @var string
     */
    const QUEUE_FAILED = 'mq_failed';
    /**
     * 延迟的缓存列队名
     */
    const QUEUE_DELAYED = 'mq_delayed';
    const LAST_DELAY_ID = 'last_delay_id';
    const LAST_DELAY_TIME = 'last_delay_time';

    const QUEUE_RETRY_LIST = 'mq_retry_list';
    const QUEUE_RETRY_HASH = 'mq_retry_hash';

    //实时数据缓存名称
    const REAL_RECV_NUM = 'realRecvNum';
    const REAL_POP_NUM = 'realPopNum';
    const REAL_PUSH_NUM = 'realPushNum';
    const REAL_TOPIC_NUM = 'topic';
    const REAL_QUEUE_COUNT = 'realQueueCount';
    const REAL_HANDLE_COUNT = 'realHandleCount';
    const REAL_DELAY_COUNT = 'realDelayCount';

    public static $prefix = '';
    public static $authKey = '';
    public static $allowIp = '';
    public static $isSqlite = false;

    const RETRY_GLOBAL_NAME = '__';
    /**
     * 全局队列重试间隔 最大重试数9: 10s 30s 1m 3m 5m 10m 20m 30m 1h
     * 重试间隔配置 [topic=>[10, 30, ...], ...]
     * @var array
     */
    protected static $retryGlobalStep = [
        self::RETRY_GLOBAL_NAME => [10, 30, 60, 180, 300, 600, 1200, 1800, 3600]
    ];

    /**
     * 队列存储间隔  默认值 60
     * @var int
     */
    public static $allowQueueStep = [2, 5, 10, 20, 30, 60, 120, 180, 240, 360, 480, 720, 1440];

    public static function toJson($buffer)
    {
        return json_encode($buffer, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }

    /**
     * 唯一id生成
     * @param $name
     * @return string len 20
     */
    public static function uniqId($name)
    {
        $m = microtime(true);
        $time = floor($m);
        $micro = $m - $time;
        $rand = mt_rand(0, 99);
        return (string)$time . ($name === '' ? (string)mt_rand(100, 999) : substr((string)crc32($name), -3)) . substr((string)$micro, 2, 5) . sprintf('%02d', $rand);
    }

    /**
     * id生成(每秒最多99999个id) 最多支持部署100个服务器 每个服务最多100个进程 10位时间戳+[5位$sequence+2位$worker_id+2位$p] 19位数字  //[5位$sequence+2位uname+2位rand]
     * @param int $worker_id 进程id 0-99
     * @param int $p 服务器区分值 0-99
     * @return int 8字节长整形
     */
    public static function bigId($worker_id = 0, $p = 0)
    {
        static $lastTime = 0, $sequence = 1, $uname;
        //if (!isset($uname)) $uname = crc32(php_uname('n')) % 10 * 1000;
        $time = time();
        if ($time == $lastTime) {
            $sequence++; // max 99999
        } else {
            $sequence = 1;
            $lastTime = $time;
        }
        //$uname + mt_rand(100, 999)
        return (int)((string)$time . '000000000') + (int)((string)$sequence . '0000') + $worker_id * 100 + $p;
        return (int)sprintf('%d%05d%02d%02d', $time, $sequence, $worker_id, $p);
        return $time * 1000000000 + $sequence * 10000 + $worker_id * 100 + $p;
    }

    public static function statusMap($val)
    {
        $status = [
            static::STATUS_FAIL => '失败',
            static::STATUS_TODO => '待处理',
            static::STATUS_DONE => '已处理',
            static::STATUS_EXECUTING => '处理中',
            static::STATUS_TIMEOUT => '处理超时',
            static::STATUS_RETRY => '重试',
        ];
        return $status[$val] ?? '';
    }

    /**
     * 初始配置
     */
    public static function initConf()
    {
        static::$prefix = GetC('queue_prefix', '');
        static::$allowIp = GetC('allow_ip');
        static::$authKey = GetC('auth_key');
        static::$isSqlite = GetC('db.dbms')=='sqlite';
        static::queueStep();
        static::retryStep();
        static::maxRetry();
    }

    //队列存储间隔 秒
    public static function queueStep()
    {
        static $queueStep;
        if (!$queueStep) {
            $queueStep = GetC('queue_step', 60);
            if (!in_array($queueStep, static::$allowQueueStep)) {
                $queueStep = 1440;
            }
            $queueStep *= 60;
        }
        return $queueStep;
    }

    /**
     * 重试间隔配置
     * @param null $topic
     * @return array|int[]|int[][]|mixed
     */
    public static function retryStep($topic = null)
    {
        static $retryStep;
        if (!$retryStep) {
            $retry_step = GetC('retry_step', []);
            if ($retry_step) {
                $retryStep = array_merge(static::$retryGlobalStep, $retry_step);
            } else {
                $retryStep = static::$retryGlobalStep;
            }
        }
        return $topic === null ? $retryStep : ($retryStep[$topic] ?? $retryStep[static::RETRY_GLOBAL_NAME]);
    }

    /**
     * 获取重试步进时间
     * @param $topic
     * @param int $step
     * @return int|mixed
     */
    public static function getRetryStep($topic, $step = 0)
    {
        $steps = static::retryStep($topic);
        return $steps[$step] ?? end($steps);
    }

    /**
     * 最大重试次数 [topic=>times, ...]
     * @param null $topic
     * @return array|mixed
     */
    public static function maxRetry($topic = null)
    {
        static $maxRetry;
        if (!$maxRetry) {
            foreach (static::retryStep() as $name => $steps) {
                $maxRetry[$name] = count($steps);
            }
        }
        if ($topic === null) return $maxRetry;
        return $maxRetry[$topic] ?? $maxRetry[static::RETRY_GLOBAL_NAME];
    }

    public static function remoteIp($con, $fd)
    {
        if (SrvBase::$instance->isWorkerMan) return $con->getRemoteIp();

        if (is_array($fd)) { // swoole udp 客户端信息包括address/port/server_socket等多项客户端信息数据
            return $fd['address'];
        }
        return $con->getClientInfo($fd)['remote_ip'];
    }

    /**
     * tcp 认证
     * @param $con
     * @param $fd
     * @param string $recv
     * @return bool
     */
    public static function auth($con, $fd, $recv = false)
    {
        //优先ip
        if (MQLib::$allowIp) {
            return Helper::allowIp(static::remoteIp($con, $fd), MQLib::$allowIp);
        }
        if (!MQLib::$authKey || SrvBase::$instance->server->type=='udp') return true; // udp使用ip验证 !$fd || is_array($fd)

        if ($recv) {
            if ($recv[0] == '#') {
                $key =  substr($recv, 1);
                if ($key == static::$authKey) { //通过认证
                    SrvBase::$instance->server->clearTimer(SrvBase::$instance->auth[$fd]);
                    SrvBase::$instance->auth[$fd] = true;
                } else {
                    static::err('auth fail');
                    return false;
                }
            } else {
                if (!isset(SrvBase::$instance->auth[$fd]) || SrvBase::$instance->auth[$fd] !== true) {
                    static::err('not auth');
                    return false;
                }
            }
            return true;
        }

        if (!isset(SrvBase::$instance->auth)) {
            SrvBase::$instance->auth = [];
        }
        //连接断开清除
        if ($recv === null) {
            unset(SrvBase::$instance->auth[$fd]);
            return true;
        }
        //创建定时认证
        SrvBase::$instance->auth[$fd] = SrvBase::$instance->server->after(1000, function () use ($con, $fd) {
            unset(SrvBase::$instance->auth[$fd]);
            SrvBase::safeEcho('to close ' . $fd . PHP_EOL);
            if (SrvBase::$instance->isWorkerMan) {
                //$con->send('auth timeout');
                $con->close();
            } else {
                //$con->send($fd, 'auth timeout');
                $con->close($fd);
            }
        });
        return true;
    }
}