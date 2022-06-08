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

    const STATUS_CANCEL = -2;
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

    const ALARM_FAIL = 'fail';
    const ALARM_RETRY = 'retry';
    const ALARM_WAITING = 'waiting';
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

    protected static $alarmInterval = 0;
    protected static $alarmWaiting = 0;
    protected static $alarmRetry = 0;
    protected static $alarmFail = 0;
    /**
     * @var callable|null
     */
    protected static $onAlarm = null;

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
        static::$prefix = GetC('prefix', '');
        static::$allowIp = GetC('allow_ip');
        static::$authKey = GetC('auth_key');

        static::$alarmInterval = (int)GetC('alarm_interval', 60);
        static::$alarmWaiting = GetC('alarm_waiting');
        static::$alarmRetry = GetC('alarm_retry');
        static::$alarmFail = GetC('alarm_fail');
        static::$onAlarm = GetC('alarm_callback');
        if (static::$onAlarm && !is_callable(static::$onAlarm)) {
            static::$onAlarm = null;
        } elseif (max(static::$alarmWaiting, static::$alarmRetry, static::$alarmFail) <= 0) {
            static::$onAlarm = null;
        }
        static::retryStep();
        static::maxRetry();
    }

    /**
     * 预警触发处理
     * @param string $type
     * @param int $value
     */
    public static function alarm($type, $value)
    {
        if (!static::$onAlarm) return;

        static $time_waiting = 0, $time_retry = 0, $time_fail = 0;
        $alarm = false;
        $alarmCheck = function (&$value, &$alarmValue, &$time, &$alarm) {
            if (is_int($value)) {
                if ($alarmValue > 0 && $value >= $alarmValue && (MQServer::$tickTime - $time) >= static::$alarmInterval) {
                    $time = MQServer::$tickTime;
                    $alarm = true;
                }
            } elseif ((MQServer::$tickTime - $time) >= static::$alarmInterval) {
                $time = MQServer::$tickTime;
                $alarm = true;
            }
        };
        if ($type == MQLib::ALARM_WAITING) {
            $alarmCheck($value, static::$alarmWaiting, $time_waiting, $alarm);
        } elseif ($type == MQLib::ALARM_RETRY) {
            $alarmCheck($value, static::$alarmRetry, $time_retry, $alarm);
        } elseif ($type == MQLib::ALARM_FAIL) {
            $alarmCheck($value, static::$alarmFail, $time_fail, $alarm);
        }
        if ($alarm) {
            Log::NOTICE('alarm '. $type . ' -> ' . $value);
            call_user_func(static::$onAlarm, $type, $value);
        }
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
        return $steps[$step] ?? 0;
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
     * @return bool|string
     */
    public static function auth($con, $fd, $recv = null)
    {
        //优先ip
        if (static::$allowIp) {
            return $recv === false || \Helper::allowIp(static::remoteIp($con, $fd), static::$allowIp);
        }
        //认证key
        if (!static::$authKey) return true;

        if (!isset(\SrvBase::$instance->auth)) {
            \SrvBase::$instance->auth = [];
        }

        //连接断开清除
        if ($recv === false) {
            unset(\SrvBase::$instance->auth[$fd]);
            \SrvBase::$isConsole && \SrvBase::safeEcho('clear auth '.$fd);
            return true;
        }

        if ($recv) {
            if (isset(\SrvBase::$instance->auth[$fd]) && \SrvBase::$instance->auth[$fd] === true) {
                return true;
            }
            \SrvBase::$instance->server->clearTimer(\SrvBase::$instance->auth[$fd]);
            if ($recv == static::$authKey) { //通过认证
                \SrvBase::$instance->auth[$fd] = true;
            } else {
                static::err('auth fail');
                return false;
            }
            return 'ok';
        }

        //创建定时认证
        if(!isset(\SrvBase::$instance->auth[$fd])){
            \SrvBase::$isConsole && \SrvBase::safeEcho('auth timer ' . $fd . PHP_EOL);
            \SrvBase::$instance->auth[$fd] = \SrvBase::$instance->server->after(1000, function () use ($con, $fd) {
                unset(\SrvBase::$instance->auth[$fd]);
                \SrvBase::$isConsole && \SrvBase::safeEcho('auth timeout to close ' . $fd . PHP_EOL);
                if (\SrvBase::$instance->isWorkerMan) {
                    $con->close();
                } else {
                    $con->close($fd);
                }
            });
        }
        return true;
    }
    /**
     * @param \Workerman\Connection\TcpConnection|\swoole_server $con
     * @param int $fd
     * @param string $msg
     */
    public static function toClose($con, $fd=0, $msg=null){
        if (\SrvBase::$instance->isWorkerMan) {
            $con->close($msg);
        } else {
            if ($msg) $con->send($fd, $msg);
            $con->close($fd);
        }
    }
}