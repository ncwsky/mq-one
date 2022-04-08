<?php

use Workerman\Connection\AsyncTcpConnection;

class MQServer
{
    use MQMsg;

    // 注意mysql:max_allowed_packet,innodb_log_buffer_size
    public static $maxBufferSize = 512000; //最大数据缓存大小 0.5M
    public static $maxBufferNum = 1000;
    public static $maxQueueWaitingNum = 10000; //最大队列等待数

    /**
     * 每秒实时接收数量
     * @var int
     */
    protected static $realPushNum = 0;
    /**
     * 队列数
     * @var int
     */
    protected static $queueCount = 0;
    /**
     * 处理数
     * @var int
     */
    protected static $handleCount = 0;
    /**
     * 延迟数
     * @var int
     */
    protected static $delayCount = 0;

    /**
     * 队列存储间隔
     * @var int
     */
    protected static $queueStep; //秒

    /**
     * 接收数据缓存
     * @var SplQueue[]
     */
    protected static $bufferData = [];
    /**
     * @var int
     */
    protected static $bufferSize = 0;
    protected static $bufferNum = 0;

    /**
     * 每秒实时实时出列数量
     * @var int
     */
    public static $realPopNum = 0;

    /**
     * 队列缓存
     * @var SplQueue[]
     */
    protected static $queueData = [];

    protected static $cacheMqListLastId = []; //上次处理的id
    /**
     * 队列数据更新缓存
     * @var array  [queueName=>[id=>[update data], ...], ...]
     */
    protected static $cacheQueueUpdate = [];

    /**
     * 缓存队列名 [name=>time, ...]
     * @var array
     */
    protected static $cacheQueueName = [];
    /**
     * 下次清理标识
     * @var string
     */
    protected static $nextClearFlag = '';

    /**
     * 下下次时段
     * @var int
     */
    protected static $next2StepTime = 0;
    /**
     * @var TcpClient
     */
    protected static $client;

    /**
     * 获取|生成队列存储名称
     * @param $time
     * @return string
     * @throws \Exception
     */
    protected static function queueName($time)
    {
        $name = date('mdHi', (int)floor($time / static::$queueStep) * static::$queueStep);

        //检测是否存在
        if (isset(static::$cacheQueueName[$name])) return $name;
        static::$cacheQueueName[$name] = $time;
        static::$bufferData[$name] = new SplQueue();

        $tableName = MQLib::QUEUE_TABLE_PREFIX . $name;
        if (MQLib::$isSqlite) {
            $sql = "select name,sql from sqlite_master where type='table' and name in('" . MQLib::QUEUE_TABLE_PREFIX . "','" . $tableName . "')";
            $tables = db()->idx('name')->query($sql, true);
            if (!isset($tables[$tableName])) {
                db()->execute(str_replace(MQLib::QUEUE_TABLE_PREFIX, $tableName, $tables[MQLib::QUEUE_TABLE_PREFIX]['sql']));
            }
        } else {
            db()->execute('CREATE TABLE IF NOT EXISTS ' . $tableName . ' LIKE ' . MQLib::QUEUE_TABLE_PREFIX);
        }

        try {
            $one = db()->getOne("select name from ". MQLib::MQ_LIST_TABLE ." where name='" . $name . "'");
            !$one && db()->add(['name' => $name, 'ctime' => $time, 'exptime' => $time + GetC('data_expired_day', 7) * 86400, 'last_id'=>0, 'end_id'=>0], MQLib::MQ_LIST_TABLE);
        } catch (\Exception $e) {
            //Log::write($e->getMessage());
        }

        return $name;
    }

    /**
     * 进程启动时处理
     * @param $worker
     * @param $worker_id
     * @throws Exception
     */
    public static function onWorkerStart($worker, $worker_id)
    {
        ini_set('memory_limit', '512M');
        MQLib::initConf();
        static::$queueStep = MQLib::queueStep();
        $time = time();
        $stepTime = floor($time / static::$queueStep) * static::$queueStep;
        $nextStepTime = $stepTime + static::$queueStep;
        static::$next2StepTime = $nextStepTime + static::$queueStep;
        static::$nextClearFlag = date('ymd') . (string)GetC('data_clear_on_hour', 10);
        static::queueName($time);

        // 清除redis延迟缓存
        $delayedList = (array)redis()->keys(MQLib::$prefix . MQLib::QUEUE_DELAYED . '*');
        $delayedList && redis()->del($delayedList);

        /*
        // 清除redis重试缓存
        redis()->del(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, MQLib::$prefix . MQLib::QUEUE_RETRY_HASH);
        */
        //内部通信client
        SrvBase::safeEcho("tcp://127.0.0.1:".SrvBase::$instance->port.PHP_EOL);
        if (SrvBase::$instance->isWorkerMan) {
            $srvConn = new AsyncTcpConnection("tcp://127.0.0.1:" . SrvBase::$instance->port);
            $srvConn->protocol = 'MQPackN2';
            $srvConn->onConnect = function (AsyncTcpConnection $con) {
                MQLib::$authKey && $con->send('#' . MQLib::$authKey);
            };
            $srvConn->onMessage = function (AsyncTcpConnection $con, $data) {
                SrvBase::safeEcho($data . PHP_EOL);
            };
            $srvConn->onClose = function (AsyncTcpConnection $con) {
                // 如果连接断开，则在1秒后重连
                $con->reConnect(1);
            };
            $srvConn->connect();
        } else {
            $srvConn = TcpClient::instance();
            TcpClient::$onLog = function ($msg) {
                Log::write($msg);
            };
            $srvConn->config("tcp://127.0.0.1:" . SrvBase::$instance->port);
            $srvConn->onConnect = function(TcpClient $con){
                MQLib::$authKey && $con->send('#' . MQLib::$authKey);
            };
            $srvConn->onInput = ['MQPackN2', 'input'];
            $srvConn->onEncode = ['MQPackN2', 'encode'];
            $srvConn->onDecode = ['MQPackN2', 'decode'];
        }

        // 实时统计
        $worker->tick(1000, function () use ($worker_id) {
            redis()->multi(MyRedis::PIPELINE);
            redis()->setex(MQLib::$prefix . MQLib::REAL_POP_NUM . $worker_id, 30, static::$realPopNum);
            redis()->setex(MQLib::$prefix . MQLib::REAL_PUSH_NUM . $worker_id, 30, static::$realPushNum);
            //redis()->setex(MQLib::$prefix . MQLib::REAL_QUEUE_COUNT . $worker_id, 30, static::$queueCount);
            //redis()->setex(MQLib::$prefix . MQLib::REAL_HANDLE_COUNT . $worker_id, 30, static::$handleCount);

            redis()->incrby(MQLib::$prefix . MQLib::REAL_QUEUE_COUNT, static::$queueCount);
            redis()->incrby(MQLib::$prefix . MQLib::REAL_HANDLE_COUNT, static::$handleCount);
            redis()->incrby(MQLib::$prefix . MQLib::REAL_DELAY_COUNT, static::$delayCount);
            //redis()->incrby(MQLib::$prefix . 'total_' . MQLib::REAL_QUEUE_COUNT, static::$queueCount);
            //redis()->incrby(MQLib::$prefix . 'total_' . MQLib::REAL_HANDLE_COUNT, static::$handleCount);

            static::$queueCount = 0;
            static::$handleCount = 0;
            static::$delayCount = 0;
            static::$realPopNum = 0;
            static::$realPushNum = 0;
            foreach (static::$queueData as $topic => $queue) {
                redis()->setex(MQLib::$prefix . MQLib::REAL_TOPIC_NUM . $worker_id . ':' . $topic, 30, $queue->count());
            }
            redis()->exec();
        });

        // 延迟入列|重试入列|更新mq最后使用id数据|更新队列数据的状态
        $worker->tick(1000, function () use($srvConn) {
            //延迟入列
            $now = time();
            $delayedList = (array)redis()->keys(MQLib::$prefix . MQLib::QUEUE_DELAYED . '*');
            $delayedLen = strlen(MQLib::$prefix . MQLib::QUEUE_DELAYED);
            foreach ($delayedList as $delayed) {
                //$options = ['LIMIT', 0, 128];
                $topic = substr($delayed, $delayedLen);
                $items = redis()->zrevrangebyscore($delayed, $now, '-inf');
                if ($items) {
                    foreach ($items as $package_str) {
                        list($queueName, $id, $ack, $retry, $data) = explode(',', $package_str, 5);
                        $push = [
                            'id'=>$id,
                            'topic'=>$topic,
                            'queueName'=>$queueName,
                            'ack'=>$ack,
                            'retry'=>$retry,
                            'data'=>$data
                        ];
                        $srvConn->send(toJson($push));
                        static::$delayCount--;
                    }
                    redis()->zremrangebyscore($delayed, '-inf', $now);
                }
            }

            //重试入列
            $now = time();
            $items = redis()->zrevrangebyscore(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, $now, '-inf');
            if ($items) {
                $redis = redis();
                $redis->retries = 1;
                foreach ($items as $id) {
                    //"$topic,$queueName,$id,$ack,$retry-$retry_step,$data"
                    $package_str = $redis->hget(MQLib::$prefix . MQLib::QUEUE_RETRY_HASH, $id); //
                    if (!$package_str) { //数据可能被清除
                        $redis->zRem(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, $id);
                        continue;
                    }

                    list($topic, $queueName, $id, $ack, $retry, $data) = explode(',', $package_str, 6);
                    $push = [
                        'id'=>$id,
                        'topic'=>$topic,
                        'queueName'=>$queueName,
                        'ack'=>$ack,
                        'retry'=>$retry,
                        'data'=>$data
                    ];
                    static::retryClean($id, true); #清除
                    list($retry, $retry_step) = explode('-', $retry, 2);
                    static::queueUpdate($queueName, $id, ['retry_count'=>(int)$retry_step]);
                    $srvConn->send(toJson($push));
                }
            }

            //更新mq最后使用id数据
            db()->beginTrans();
            foreach (static::$cacheMqListLastId as $queueName => $last_id) {
                db()->update(['last_id' => $last_id], MQLib::MQ_LIST_TABLE, ['name' => $queueName, 'last_id<' . $last_id]);
                unset(static::$cacheMqListLastId[$queueName]);
            }
            db()->commit();

            static $lastTime;
            if (!$lastTime) {
                $lastTime = time();
            }
            $time = time();
            $toClean = false;
            $day = 0;
            if (($lastTime - $time) > 86400) {
                $lastTime = $time;
                $toClean = true;
                $day = (int)date('j', $time);
            }
            //更新队列数据的状态
            foreach (static::$cacheQueueUpdate as $queueName => $items) {
                if ($items) {
                    try {
                        db()->beginTrans();
                        foreach ($items as $id => $item) {
                            db()->update($item, MQLib::QUEUE_TABLE_PREFIX . $queueName, 'id=' . $id);
                        }
                        db()->commit();
                    } catch (\Exception $e) {
                        Log::write($e->getMessage(), 'fail');
                    }
                    static::$cacheQueueUpdate[$queueName] = [];
                }

                if ($toClean) { //移除超过1天的状态缓存  可考虑直接重置 static::$cacheQueueUpdate = [];
                    $qDay = (int)substr($queueName, 2, 2);
                    if (abs($day - $qDay) > 1) {
                        unset(static::$cacheQueueUpdate[$queueName]);
                    }
                }
            }
        });

        //n ms实时数据落地
        $worker->tick(200, function () {
            self::writeToDisk();
        });

        //间隔时段定时 移除无用的缓存队列名|下下间隔时段的延迟数据|赛斯数据清理  todo 优化
        $worker->tick(static::$queueStep * 1000, function () use ($worker_id) {
            //延迟数据入缓存
            $nextQueueName = date('mdHi', static::$next2StepTime);
            //下下间隔时段的延迟数据
            $r = db()->find(MQLib::MQ_LIST_TABLE, "name='".$nextQueueName."'", '', 'name,last_id,end_id');
            $last_id = 0;
            while ($last_id < $r['end_id']){
                $res = db()->query('select id,ctime,topic,retry,ack,data from ' . MQLib::QUEUE_TABLE_PREFIX . $nextQueueName . ' where id>' . $last_id . ' order by id asc limit 500');
                $redis = redis();
                $redis->multi(MyRedis::PIPELINE);
                while ($item = db()->fetch_array($res)) {
                    $queue_str = $r['name'] . ',' . $item['id'] . ',' . $item['ack'] . ',' . $item['retry'] . ',' . $item['data'];
                    $redis->zAdd(MQLib::$prefix . MQLib::QUEUE_DELAYED . $item['topic'], $item['ctime'], $queue_str);
                    $last_id = $item['id'];
                }
                $redis->exec();
            }
            //更新下下次时段
            static::$next2StepTime += static::$queueStep;

            $t = (int)floor(time() / static::$queueStep) * static::$queueStep;
            foreach (static::$cacheQueueName as $name => $time) {
                if ($time < $t) {
                    unset(static::$cacheQueueName[$name], static::$bufferData[$name]);
                }
            }
            if ($worker_id == 0) {
                $t = time();
                //达到时间清理
                if (date('ymdG', $t) == static::$nextClearFlag) {
                    //更新下次清理标识
                    static::$nextClearFlag = date('ymd', $t + 86400) . (string)GetC('data_clear_on_hour', 10);

                    //清理过期数据
                    $expList = db()->query('select name from '. MQLib::MQ_LIST_TABLE .' where exptime<' . $t, true);
                    foreach ($expList as $item) {
                        db()->execute('DROP TABLE IF EXISTS ' . MQLib::QUEUE_TABLE_PREFIX . $item['name']);
                        Log::write($item['name'], 'clear');
                    }
                    db()->del(MQLib::MQ_LIST_TABLE, 'exptime<' . $t);
                }
            }
        });

        //初始缓存所有队列表名
        $tables = db()->query('select name,ctime from '. MQLib::MQ_LIST_TABLE .' where ctime>=' . $stepTime . ' order by ctime asc', true);
        foreach ($tables as $r) {
            static::$cacheQueueName[$r['name']] = $r['ctime'];
            static::$bufferData[$r['name']] = new SplQueue();
        }
        static::queueName($time);

        //初始待处理数据 3天内的+下2次时段的数据
        $tables = db()->all(MQLib::MQ_LIST_TABLE, 'ctime>=' . ($stepTime - 3 * 86400) . ' and ctime<=' . static::$next2StepTime . ' and end_id>last_id', 'ctime asc', 'name,last_id,end_id');
        // 'select name,last_id,end_id from ' . MQLib::MQ_LIST_TABLE . ' where ctime>=' . ($stepTime - 3 * 86400) . ' and ctime<=' . ($stepTime + static::$queueStep) . ' and end_id>last_id order by ctime asc';
        foreach ($tables as $r) {
            $last_id = $r['last_id'];
            $count = 0;
            while ($last_id < $r['end_id']) {
                $res = db()->query('select id,ctime,topic,retry,ack,data from ' . MQLib::QUEUE_TABLE_PREFIX . $r['name'] . ' where id>' . $last_id . ' order by id asc limit 500');
                $redis = redis();
                $redis->multi(MyRedis::PIPELINE);
                while ($item = db()->fetch_array($res)) {
                    if (!isset(static::$queueData[$item['topic']])) {
                        static::$queueData[$item['topic']] = new SplQueue();
                    }

                    $queue_str = $r['name'] . ',' . $item['id'] . ',' . $item['ack'] . ',' . $item['retry'] . ',' . $item['data'];
                    if ($item['ctime'] <= $time) {
                        static::$queueData[$item['topic']]->enqueue($queue_str);
                    } else {
                        $redis->zAdd(MQLib::$prefix . MQLib::QUEUE_DELAYED . $item['topic'], $item['ctime'], $queue_str);
                    }
                    $last_id = $item['id'];
                    $count++;
                }
                $redis->exec();
            }
            Log::write('init: ' . $r['name'] . '->' . $r['last_id'] . '<-' . $last_id . ', count:' . $count);
        }

        //重试缓存数据载入 缓存中有记录 不执行载入处理
        $retryNum = redis()->hlen(MQLib::$prefix . MQLib::QUEUE_RETRY_HASH);
        if ($retryNum == 0) {
            $redis = redis();
            $count = db()->getCount(MQLib::QUEUE_RETRY_TABLE);
            $last_id = 0;
            while ($retryNum < $count) {
                $redis->multi(MyRedis::PIPELINE);
                $res = db()->query('select id,ctime,queue_str from ' . MQLib::QUEUE_RETRY_TABLE . ' where id>' . $last_id . ' order by id asc limit 500');
                while ($item = db()->fetch_array($res)) {
                    static::retryQueue($item['id'], $item['ctime'], $item['queue_str']);
                    $last_id = $item['id'];
                }
                $redis->exec();
                $retryNum += 500;
            }
        }
        //清空重试持久缓存表
        db()->execute((MQLib::$isSqlite ? 'DELETE FROM ' : 'TRUNCATE TABLE ') . MQLib::QUEUE_RETRY_TABLE);
    }

    /**
     * 终端数据进程结束时的处理
     * @param $worker
     * @param $worker_id
     * @throws \Exception
     */
    public static function onWorkerStop($worker, $worker_id)
    {
        //进程结束时把缓存的数据写入到磁盘
        self::writeToDisk();

        if ($worker_id != 0) return;

        $time = time();
        //持久缓存重试数据
        $retryList = redis()->ZRANGEBYSCORE(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, time(), '+inf');
        if($retryList){
            db()->beginTrans();
            $retryStmt = db()->prepare('INSERT INTO '.MQLib::QUEUE_RETRY_TABLE.'(id,ctime,queue_str) VALUES (?, ?, ?)');
            $n=0;
            foreach ($retryList as $id){
                $package_str = redis()->hget(MQLib::$prefix . MQLib::QUEUE_RETRY_HASH, $id); //
                if (!$package_str) { //数据可能被清除
                    continue;
                }

                list($topic, $queueName, $id, $ack, $retry, $data) = explode(',', $package_str, 6);

                list($retry, $retry_step) = explode('-', $retry, 2);
                $ctime = $time + MQLib::getRetryStep($topic, $retry_step);

                $retryStmt->execute([$id, $ctime, $package_str]);
                $n++;
                if ($n > 1000) {
                    db()->commit();
                    db()->beginTrans();
                    $n = 0;
                }
            }
            db()->commit();
        }

        // 清除redis重试缓存
        redis()->del(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, MQLib::$prefix . MQLib::QUEUE_RETRY_HASH);
        // 清除redis延迟缓存
        $delayedList = (array)redis()->keys(MQLib::$prefix . MQLib::QUEUE_DELAYED . '*');
        $delayedList && redis()->del($delayedList);
    }

    /**
     * 处理数据
     * @param $con
     * @param string $recv
     * @param int|array $fd
     * @return bool|array
     * @throws \Exception
     */
    public static function onReceive($con, $recv, $fd=0)
    {
        //认证处理
        if (!MQLib::auth($con, $fd, $recv)) {
            static::err(MQLib::err());
            return false;
        }

        if ($recv === '') {
            static::err('nil');
            return false;
        }

        if ($recv[0] == '{') { // substr($recv, 0, 1) == '{' && substr($recv, -1) == '}'
            $data = json_decode($recv, true);
        } else { // querystring
            parse_str($recv, $data);
        }

        if (empty($data)) {
            static::err('empty data: '.$recv);
            return false;
        }
        if (!isset($data['cmd'])) $data['cmd'] = 'push';


        $ret = 'ok';
        switch ($data['cmd']) {
            case 'push': //入列 用于消息重试
                $data['host'] = SrvBase::$instance->isWorkerMan ? $con->getRemoteIp() : $con->getClientInfo($fd)['remote_ip'];
                $data['len'] = strlen($recv);
                $ret = static::push($data);
                break;
            case 'pop': //出列
                $ret = static::pop($data);
                break;
            case 'ack': //应答
                static::ack($data);
                break;
            case 'stats':
                $ret = static::stats();
                break;
            default:
                self::err('invalid cmd');
                $ret = false;
        }
        return $ret;
    }
    protected static function push($data){
        static::$realPushNum++;
        if (empty($data['topic']) || empty($data['data'])) {
            return [0, 'topic or data is empty'];
            static::err('topic or data is empty');
            return false;
        }
        $topic_length = strlen($data['topic']);
        if ($topic_length > MQLib::MAX_TOPIC_LENGTH) {
            $data['topic'] = substr($data['topic'], 0, MQLib::MAX_TOPIC_LENGTH);
            #return [0, 'topic length[' . $topic_length . '] is too long'];
        }
        $data_length = strlen($data['data']);
        if ($data_length > MQLib::MAX_DATA_LENGTH) {
            return [0, 'data length[' . $data_length . '] is too long'];
        }

        $maxRetry = MQLib::maxRetry($data['topic']);

        if (!isset(static::$queueData[$data['topic']])) {
            static::$queueData[$data['topic']] = new SplQueue();
        }

        $ack = empty($data['ack']) ? 0 : 1;
        if (isset($data['id']) && isset($data['queueName'])) { //直接投递入列数据处理
            if (isset($data['retry'])) {
                if (is_numeric($data['retry'])) {
                    if ($data['retry'] > $maxRetry) {
                        $data['retry'] = $maxRetry;
                    }
                } else {
                    list($retry, $retry_step) = explode('-', $data['retry'], 2);
                    $retry = min($maxRetry, $retry);
                    $data['retry'] = $retry . '-' . $retry_step;
                }
            } else {
                $data['retry'] = $ack ? $maxRetry : 0;
            }
            static::$queueData[$data['topic']]->enqueue($data['queueName'] . ',' . $data['id'] . ',' . $data['ack'] . ',' . $data['retry'] . ',' . $data['data']);
            return [$data['id'], $data['queueName']];
        }

        $ctime = $t = time();
        $sync = 0;
        if (isset($data['delay'])) {
            $ctime += (int)$data['delay'];
        }

        if (isset($data['to']) && strlen($data['to']) > 50) {
            $data['to'] = substr($data['to'], 0, 50);
        }

        $worker_id = SrvBase::$instance->workerId();
        $queueData = [
            'id' => MQLib::bigId($worker_id),
            'ctime' => $ctime,
            'mtime' => $t,
            'retry' => 0,
            'ack' => $ack,
            'seq_id' => 0,
            'host' => $data['host'] ?? '',
            'to' => $data['to'] ?? '',
            'topic' => $data['topic'],
            'data' => $data['data']
        ];

        if (isset($data['retry'])) {
            $queueData['retry'] = (int)$data['retry'];
            if ($queueData['retry'] > $maxRetry) {
                $queueData['retry'] = $maxRetry;
            } elseif ($queueData['retry'] < 0) {
                $queueData['retry'] = 0;
            }
        } elseif ($ack) {
            $queueData['retry'] = $maxRetry;
        }
        if (!empty($data['sync'])) {
            $sync = MQLib::STATUS_SYNC; //同步落地
        }
/*
        $data['status'] = $data['status'] ?? 0;
        if ($data['status'] > 0) {
            $queueData['ack'] = $data['status'] & MQLib::STATUS_ACK;
            $sync = $data['status'] & MQLib::STATUS_SYNC; //同步落地
        }*/

        if (isset($data['seq_id'])) { // todo seq_id相同的只能由同一个消费者处理
            $queueData['seq_id'] = (int)$data['seq_id'];
        }

        $queueName = static::queueName($ctime);
        static::$bufferNum++;
        static::$bufferSize += $data['len'];
        static::$queueCount++;
        static::$bufferData[$queueName]->enqueue($queueData);
        if ($sync && static::$bufferSize > static::$maxBufferSize || static::$bufferNum > static::$maxBufferNum) {
            static::writeToDisk();
        }
        return [$queueData['id'], $queueName];
    }
    /**
     * 出列数据
     * @param array $data ['topic'=>'cmd'|[cmd, ...],'num'=>1]
     * @return array|string
     */
    protected static function pop($data)
    {
        $topic = $data['topic'] ?? '';
        $num = $data['num'] ?? 1;
        if ($topic === '') return [];
        $ret = '';//[];
        if (is_string($topic)) {
            static::checkTopic($topic);
            static::dequeue($ret, $topic, $num);
        } else {
            foreach ($topic as $name) {
                static::checkTopic($name);
                static::dequeue($ret, $name, $num);
            }
        }
        return $ret;
    }

    protected static function dequeue(&$ret, $topic, $num)
    {
        if (!isset(static::$queueData[$topic])) return;
        $time = time();
        while (!static::$queueData[$topic]->isEmpty()) {
            $package_str = static::$queueData[$topic]->dequeue();
            list($queueName, $id, $ack, $retry, $data) = explode(',', $package_str, 5); //[$queueName, $id, $ack, $retry, $data]
            static::$realPopNum++;
            //记录最后出列id
            if (!isset(static::$cacheMqListLastId[$queueName]) || static::$cacheMqListLastId[$queueName] < $id) {
                static::$cacheMqListLastId[$queueName] = $id;
            }
            $ret .= $package_str . "\r";

            //加入重试集合
            $retry_step = 0;
            if(strpos($retry,'-')){
                list($retry, $retry_step) = explode('-', $retry);
                $retry_step = (int)$retry_step;
            }
            $retry = (int)$retry;
            if ($retry > $retry_step) { //进程结束会记录到持久缓存表 意外关机
                static::retryQueue($id, $time + MQLib::getRetryStep($topic, $retry_step), $topic . ',' . $queueName . ',' . $id . ',' . $ack . ',' . $retry.'-'.($retry_step+1) . ',' . $data);
            }

            if ($retry_step == 0) {
                static::$handleCount++;
            }

            //更新状态
            static::queueUpdate($queueName, $id, ['status' => $ack || $retry ? MQLib::STATUS_EXECUTING : MQLib::STATUS_DONE]);

            $num--;
            if ($num <= 0) break;
        }
        return;
    }
    protected static function ack($data)
    {
        $queueName = $data['queueName'] ?? '';
        $id = $data['id'] ?? 0;
        $status = $data['status'] ?? 1;
        if (strlen((string)$id) != 19) return; //id固定长度19位

        if ($queueName == '') { //延迟的取name不正确
            $time = (int)substr((string)$id, 0, 10);
            $step = MQLib::queueStep();
            $queueName = date('mdHi', (int)floor($time / $step) * $step);;
        }
        static::retryClean($id);
        //static::setStatus($queueName, $id, $status, $result);
        $result = $data['result'] ?? '';
        $data = ['status' => $status];
        if ($result!=='') $data['result'] = $result;
        static::queueUpdate($queueName, $id, $data);
    }

    protected static function stats()
    {
        $stats = [
            'queue_count' => (int)redis()->get(MQLib::$prefix . MQLib::REAL_QUEUE_COUNT),
            'handle_count' => (int)redis()->get(MQLib::$prefix . MQLib::REAL_HANDLE_COUNT),
            'retry_count' => (int)redis()->zcard(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST),
            'delay_count' => (int)redis()->get(MQLib::$prefix . MQLib::REAL_DELAY_COUNT),
            'real_pop_num' => 0,
            'real_push_num' => 0,
            'waiting_num' => 0,
            'topic_count' => 0,
            'topic_list' => [],
        ];

        $popNumList = (array)redis()->keys(MQLib::$prefix . MQLib::REAL_POP_NUM . '*');
        foreach ($popNumList as $key) {
            $stats['real_pop_num'] += (int)redis()->get($key);
        }

        $pushNumList = (array)redis()->keys(MQLib::$prefix . MQLib::REAL_PUSH_NUM . '*');
        foreach ($pushNumList as $key) {
            $stats['real_push_num'] += (int)redis()->get($key);
        }

        //待处理数量
        $topicNumList = (array)redis()->keys(MQLib::$prefix . MQLib::REAL_TOPIC_NUM . '*');
        foreach ($topicNumList as $topicNum) {
            list(, $topic) = explode(':', $topicNum, 2);
            $num = (int)redis()->get($topicNum);
            $stats['waiting_num'] += $num;
            if (isset($stats['topic_queue'][$topic])) {
                $stats['topic_list'][$topic] += $num;
            } else {
                $stats['topic_list'][$topic] = $num;
            }
        }
        $stats['topic_count'] = count($stats['topic_list']);

        return $stats;
    }

    protected static function checkTopic(&$topic)
    {
        if (isset($topic) && strlen($topic) > MQLib::MAX_TOPIC_LENGTH) {
            $topic = substr($topic, 0, MQLib::MAX_TOPIC_LENGTH);
        }
    }

    /**
     * 更新
     * @param string $queueName
     * @param int $id
     * @param array $data
     */
    protected static function queueUpdate($queueName, $id, $data)
    {
        if (!isset(static::$cacheQueueUpdate[$queueName])) {
            static::$cacheQueueUpdate[$queueName] = [];
        }
        if (!isset(static::$cacheQueueUpdate[$queueName][$id])) {
            static::$cacheQueueUpdate[$queueName][$id] = [];
        }
        static::$cacheQueueUpdate[$queueName][$id] = array_merge(static::$cacheQueueUpdate[$queueName][$id], $data);
        static::$cacheQueueUpdate[$queueName][$id]['mtime'] = time();
    }
    /**
     * 重试集合
     * @param $id
     * @param $time
     * @param $data "$topic,$queueName,$id,$ack,($retry-1),$data"
     */
    protected static function retryQueue($id, $time, $data)
    {
        redis()->zAdd(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, $time, $id);
        redis()->hset(MQLib::$prefix . MQLib::QUEUE_RETRY_HASH, $id, $data);
    }

    /**
     * 清除重试
     * @param $id
     * @param bool $retry 是否重试清除
     */
    protected static function retryClean($id, $retry = false)
    {
        Log::DEBUG("<- " . ($retry ? 'Retry' : 'Recv') . " PUBACK package, id:$id");
        if (SrvBase::$isConsole) {
            SrvBase::safeEcho(date("Y-m-d H:i:s")." <- " . ($retry ? 'Retry' : 'Recv') . " PUBACK package, id:$id" . PHP_EOL);
        }
        $redis = redis();
        if ($redis->hExists(MQLib::$prefix . MQLib::QUEUE_RETRY_HASH, $id)) {
            $redis->zRem(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, $id);
            $redis->hdel(MQLib::$prefix . MQLib::QUEUE_RETRY_HASH, $id);
        }
    }

    /**
     * 将数据写入磁盘
     * @throws \Exception
     */
    public static function writeToDisk()
    {
        if (static::$bufferSize == 0) return;
        $time = time();
        foreach (static::$bufferData as $queueName => $queue) {
            try {
                $n = 0;
                $data = [];
                $redis = redis();
                $endId = 0;
                $tableName = MQLib::QUEUE_TABLE_PREFIX . $queueName;
                db()->beginTrans();
                $redis->multi(MyRedis::PIPELINE);
                while(!$queue->isEmpty()){
                    $item = $queue->dequeue();
                    $n++;
                    $redis->retries = 1;
                    $queue_str = $queueName . ',' . $item['id'] . ',' . $item['ack'] . ',' . $item['retry'] . ',' . $item['data'];
                    //推送数据
                    if ($item['ctime'] <= $time) {
                        static::$queueData[$item['topic']]->enqueue($queue_str);
                    } else {
                        static::$delayCount++;
                        if ($item['ctime'] <= static::$next2StepTime) { //延时在一间隔时间段内的 直接加入缓存 超过定时定时读入缓存
                            $redis->zAdd(MQLib::$prefix . MQLib::QUEUE_DELAYED . $item['topic'], $item['ctime'], $queue_str);
                        }
                    }

                    $data[] = $item;
                    $endId = $item['id'];
                    if ($n > 9) {
                        db()->add($data, $tableName);
                        $n = 0;
                        $data = [];
                    }
                }
                if ($n > 0) {
                    db()->add($data, $tableName);
                }
                $endId>0 && db()->update(['end_id' => $endId], MQLib::MQ_LIST_TABLE, ['name' => $queueName]); //记录最新id
                db()->commit();
                $redis->exec();
            } catch (\Exception $e) {
                Log::write($e->getMessage(), 'fail');
            }
        }
        static::$bufferSize = 0;
        static::$bufferNum = 0;
    }
}