<?php

class MQServer
{
    use MQMsg;

    // 注意mysql:max_allowed_packet,innodb_log_buffer_size
    public static $maxBufferSize = 1024000; //最大数据缓存大小 1M

    /**
     * 信息统计
     * @var array
     */
    protected static $infoStats = [];

    /**
     * 每秒实时接收数量
     * @var int
     */
    protected static $realRecvNum = 0;

    /**
     * 每秒实时推送数量
     * @var int
     */
    protected static $realPushNum = 0;

    /**
     * 每分钟失败数
     * @var int
     */
    protected static $perFailNum = 0;
    /**
     * 队列数
     * @var int
     */
    protected static $queueCount = 0;
    protected static $waitingCount = 0;

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
     * 失败数
     * @var int
     */
    protected static $failCount = 0;

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

    /**
     * @var DelayPHP|DelayInterface
     */
    protected static $delay;

    /**
     *  DelayInterface Class
     * @var string
     */
    public static $delayClass = '';

    /**
     * @var RetryRedis|RetryInterface
     */
    protected static $retry;

    /**
     *  RetryInterface Class
     * @var string
     */
    public static $retryClass = '';

    /**
     * 每秒实时实时出列数量
     * @var int
     */
    protected static $realPopNum = 0;

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
    protected static $nextClearFlag = 0;

    /**
     * 下下次时段
     * @var int
     */
    protected static $next2StepTime = 0;

    protected static $isMem = false;
    protected static $isSqlite = false;
    protected static $allowTopicList;
    protected static $multiSplit = "\r";
    protected static $maxWaitingNum = 0;
    protected static $dataExpired = 0;

    /**
     * 待处理队列已满
     * @var bool
     */
    protected static $isWaitingFull = false;
    /**
     * 用于记录队列满待处理的起始id, 截止id
     * static::$infoStats['full_info'] = [startTime, startId, endTime, endId]
     */

    /**
     * @var int 定时每秒的时间
     */
    public static $tickTime;

    protected static function stepTime($time)
    {
        return (int)floor($time / static::$queueStep) * static::$queueStep;
    }
    /**
     * 获取|生成队列存储名称
     * @param $time
     * @return string
     * @throws \Exception
     */
    protected static function queueName($time)
    {
        $time = static::stepTime($time);
        $name = date('mdHi', $time);

        //检测是否存在
        if (isset(static::$cacheQueueName[$name])) return $name;
        static::$cacheQueueName[$name] = $time;
        static::$bufferData[$name] = new SplQueue();
        if (static::$isMem) return $name;

        $tableName = MQLib::QUEUE_TABLE_PREFIX . $name;
        if (static::$isSqlite) {
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
            !$one && db()->add(['name' => $name, 'ctime' => $time, 'exptime' => $time + static::$dataExpired, 'last_id'=>0, 'end_id'=>0], MQLib::MQ_LIST_TABLE);
        } catch (\Exception $e) {
            //Log::write($e->getMessage());
        }

        return $name;
    }

    //更新队列数据的状态
    protected static function toQueueUpdate(){
        if (static::$isMem) {
            static::$cacheQueueUpdate = [];
            return;
        }
        foreach (static::$cacheQueueUpdate as $queueName => $items) {
            if ($items) {
                db()->beginTrans();
                try {
                    foreach ($items as $id => $item) {
                        db()->update($item, MQLib::QUEUE_TABLE_PREFIX . $queueName, 'id=' . $id);
                    }
                    db()->commit();
                    static::$cacheQueueUpdate[$queueName] = [];
                } catch (\Exception $e) {
                    db()->rollBack();
                    //Log::write((string)$e, 'toQueueUpdate');
                    MQLib::alarm(MQLib::ALARM_FAIL, '队列数据的状态失败:'.$e->getMessage());
                }
            }
        }
    }
    //更新mq最后使用id数据
    protected static function toMqListLastId($isStop=false){
        if (static::$isMem){
            static::$cacheMqListLastId = [];
            return;
        }
        //更新mq最后使用id数据
        db()->beginTrans();
        try {
            foreach (static::$cacheMqListLastId as $queueName => $last_id) {
                db()->update(['last_id' => $last_id], MQLib::MQ_LIST_TABLE, ['name' => $queueName, 'last_id<' . $last_id]);
            }
            if ($isStop) {
                list($minId, $minQueueName) = static::$delay->stop2minId();
                $currQueueName = date('mdHi', static::stepTime(time()));
                if ($minQueueName == $currQueueName && $minId > 0) {
                    Log::write($minId . ' - ' . $minQueueName, 'delayMinId');
                    db()->update(['last_id' => $minId], MQLib::MQ_LIST_TABLE, ['name' => $minQueueName]);
                }
            }
            db()->commit();
            static::$cacheMqListLastId = [];
        } catch (Exception $e) {
            db()->rollBack();
            //Log::write((string)$e,'toMqListLastId');
            MQLib::alarm(MQLib::ALARM_FAIL, '更新mq最后使用id数据失败:'.$e->getMessage());
        }
    }
    //下下间隔时段的延迟数据
    protected static function toDelayLoad(){
        if (static::$isMem) return;
        //延迟数据入缓存
        $nextQueueName = date('mdHi', static::$next2StepTime);
        //下下间隔时段的延迟数据  调整了$queueStep的值  旧的延迟数据将无法读取
        $r = db()->find(MQLib::MQ_LIST_TABLE, "name='".$nextQueueName."'", '', 'name,last_id,end_id');
        Log::write('延迟数据载入 nextQueueName:' . $nextQueueName . ', end_id:' . ($r ? $r['end_id'] : 'none'));
        if ($r) {
            try{
                $last_id = 0;
                while ($last_id < $r['end_id']) {
                    $res = db()->query('select id,ctime,topic,retry,ack,data from ' . MQLib::QUEUE_TABLE_PREFIX . $nextQueueName . ' where id>' . $last_id . ' and status=0 order by id asc limit 100');
                    static::$delay->beforeAdd();
                    while ($item = db()->fetch_array($res)) {
                        $queue_str = $r['name'] . ',' . $item['id'] . ',' . $item['ack'] . ',' . $item['retry'] . ',' . $item['data'];
                        static::$delay->add($item['topic'], $item['ctime'], $queue_str);
                        $last_id = $item['id'];
                    }
                    static::$delay->afterAdd();
                }
            } catch (Exception $e){
                //Log::write((string)$e, 'toDelayLoad');
                MQLib::alarm(MQLib::ALARM_FAIL, '延迟数据载入失败:'.$e->getMessage());
            }
        }
        //更新下下次时段
        static::$next2StepTime += static::$queueStep;
    }
    /**
     * 间隔时段定时 移除无用的缓存队列名|过期数据清理  todo 优化
     * @throws Exception
     */
    protected static function toQueueStep(){
        Log::write('toQueueStep tickTime:'.static::$tickTime);
        Log::write('cache queue name:'. json_encode(array_keys(static::$cacheQueueName)));
        Log::write('buffer keys:'. json_encode(array_keys(static::$bufferData)));

        $now = static::$tickTime; //time();
        $currStepTime = static::stepTime($now);
        foreach (static::$cacheQueueName as $name => $time) {
            if ($time < $currStepTime) {
                unset(static::$cacheQueueName[$name], static::$bufferData[$name]);
            }
        }
        if (static::$isMem) return;

        //达到时间清理
        if ($now >= static::$nextClearFlag) {
            Log::write('clear , nextClearFlag '.static::$nextClearFlag.' : exptime<' . $now);
            //更新下次清理标识
            static::$nextClearFlag = $now + static::$dataExpired;
            //清理过期数据
            $expList = db()->query('select name from '. MQLib::MQ_LIST_TABLE .' where exptime<' . $now, true);
            foreach ($expList as $item) {
                db()->execute('DROP TABLE IF EXISTS ' . MQLib::QUEUE_TABLE_PREFIX . $item['name']);
            }
            db()->del(MQLib::MQ_LIST_TABLE, 'exptime<' . $now);
            Log::write('clear count:'. count($expList));
        }
    }
    //初始队列
    protected static function initQueue(){
        if (static::$isMem) return;
        $time = time();
        $stepTime = static::stepTime($time);
        //初始缓存所有队列表名
        $tables = db()->query('select name,ctime from '. MQLib::MQ_LIST_TABLE .' where ctime>=' . $stepTime . ' order by ctime asc', true);
        foreach ($tables as $r) {
            static::$cacheQueueName[$r['name']] = $r['ctime'];
            static::$bufferData[$r['name']] = new SplQueue();
        }
        static::queueName($time);

        //初始待处理数据 3天内的+下1次时段的数据
        static::toInitQueue($stepTime - 3 * 86400);

        //初始统计延迟数
        $tables = db()->all(MQLib::MQ_LIST_TABLE, 'ctime>=' . static::$next2StepTime . ' and last_id=0', 'ctime asc', 'name');
        foreach ($tables as $r) {
            static::$delayCount += db()->getCount(MQLib::QUEUE_TABLE_PREFIX . $r['name']);
        }
        Log::write('init: delayCount ' . static::$delayCount);
    }

    protected static function toInitQueue($startTime, $custom_id=0){
        $time = time();
        //初始待处理数据 x天内的+下1次时段的数据
        $tables = db()->all(MQLib::MQ_LIST_TABLE, 'ctime>=' . $startTime . ' and ctime<' . static::$next2StepTime . ' and end_id>last_id', 'ctime asc', 'name,last_id,end_id');
        Log::write('toInitQueue: custom_id '.$custom_id.', ctime>=' . $startTime . ' and ctime<' . static::$next2StepTime);
        foreach ($tables as $k=>$r) {
            $last_id = $r['last_id'];
            if($k==0 && $custom_id!=0) $last_id = $custom_id;
            $up_last_id = 0; //因延迟数据 last_id 更正
            $count = 0;
            while ($last_id < $r['end_id']) {
                $res = db()->query('select id,ctime,mtime,status,topic,retry,ack,data from ' . MQLib::QUEUE_TABLE_PREFIX . $r['name'] . ' where id>' . $last_id . ' order by id asc limit 200');
                $hasRecord = false;
                static::$delay->beforeAdd();
                while ($item = db()->fetch_array($res)) {
                    $hasRecord = true;
                    $last_id = $item['id'];
                    if (!empty($item['status'])) {
                        $up_last_id = $item['id'];
                        continue;
                    }
                    if (!isset(static::$queueData[$item['topic']])) static::$queueData[$item['topic']] = new SplQueue();
                    $item['ctime'] = (int)$item['ctime'];
                    $queue_str = $r['name'] . ',' . $item['id'] . ',' . $item['ack'] . ',' . $item['retry'] . ',' . $item['data'];
                    if ($item['ctime'] <= $time) {
                        static::$queueData[$item['topic']]->enqueue($queue_str);
                        static::$waitingCount++;
                    } else {
                        static::$delayCount++;
                        static::$delay->add($item['topic'], $item['ctime'], $queue_str);
                    }
                    $count++;
                }
                static::$delay->afterAdd();
                if (!$hasRecord) break;
            }
            if ($up_last_id>$r['last_id']) {
                db()->update(['last_id' => $up_last_id], MQLib::MQ_LIST_TABLE, ['name' => $r['name']]);
            }
            Log::write('init: ' . $r['name'] . '->' . $r['last_id'] . '<-' . $last_id . ', count:' . $count);
        }
    }

    //初始重试数据
    protected static function initRetry(){
        if (static::$isMem) return;
        $retryNum = static::$retry->getCount(); //缓存中有记录 不执行载入处理
        if ($retryNum == 0) {
            $count = db()->getCount(MQLib::QUEUE_RETRY_TABLE);
            $last_id = 0;
            while ($retryNum < $count) {
                static::$retry->beforeAdd();
                $res = db()->query('select id,ctime,queue_str from ' . MQLib::QUEUE_RETRY_TABLE . ' where id>' . $last_id . ' order by id asc limit 100');
                while ($item = db()->fetch_array($res)) {
                    static::$retry->add($item['id'], $item['ctime'], $item['queue_str']);
                    $last_id = $item['id'];
                    $retryNum ++;
                }
                static::$retry->afterAdd();
            }
            Log::write('init-retry: load ' . $retryNum . ', retry_count ' . static::$retry->getCount() . ', table_count ' . $count);
        }
        //Log::write((string)static::$retry);
        //清空重试持久缓存表
        db()->execute((static::$isSqlite ? 'DELETE FROM ' : 'TRUNCATE TABLE ') . MQLib::QUEUE_RETRY_TABLE);
    }
    //重试缓存数据落盘存储便于下次启动使用
    protected static function toRetrySave(){
        if (static::$isMem) return;
        $retryList = static::$retry->getIdList();
        $retryCount = count($retryList);
        Log::write('toRetrySave count:' . $retryCount);

        if(!$retryList) return; //无重试缓存

        //$time = time();
        db()->beginTrans();
        try {
            $retryStmt = db()->prepare('INSERT INTO '.MQLib::QUEUE_RETRY_TABLE.'(id,ctime,queue_str) VALUES (?, ?, ?)');
            $n=0;
            foreach ($retryList as $id){
                $package_str = static::$retry->getData($id);
                if (!$package_str) { //数据可能被清除
                    $retryCount--;
                    continue;
                }
                //$retryTime, $topic, $queueName, $id, $ack, $retry, $data
                list($retryTime, $topic, , $id, , $retry, ) = explode(',', $package_str, 7);
                //list(, $retry_step) = explode('-', $retry, 2); //$retry, $retry_step
                //$ctime = $time + MQLib::getRetryStep($topic, $retry_step);

                $retryStmt->execute([$id, $retryTime, $package_str]);
                $n++;
                if ($n > 1000) {
                    db()->commit();
                    db()->beginTrans();
                    $n = 0;
                }
            }
            db()->commit();
            Log::write('toRetrySave write count:'.$retryCount);
        } catch (Exception $e) {
            db()->rollBack();
            //Log::write((string)$e, 'toRetrySave');
            MQLib::alarm(MQLib::ALARM_FAIL, '重试数据落盘存储失败:'.$e->getMessage());
        }
    }
    //统计信息 初始
    protected static function initInfo($reset=false)
    {
        $file = SrvBase::$instance->runDir . '/' . SrvBase::$instance->serverName() . '.info';
        if ($reset || !is_file($file)) {
            static::$infoStats = [
                'date' => date("Y-m-d H:i:s"),
                'queue_count' => 0,
                'handle_count' => 0,
                'fail_count' => 0,
                'delay_count' => 0,
            ];
        } else {
            static::$infoStats = (array)json_decode(file_get_contents($file), true);
        }
        static::$infoStats = array_merge(static::$infoStats, [
            'retry_count' => 0,
            'per_fail_num' => 0,
            'real_recv_num' => 0,
            'real_pop_num' => 0,
            'real_push_num' => 0,
            'waiting_delay_num'=>0,
            'waiting_num' => 0,
            'topic_count' => 0,
            'topic_list' => []
        ]);
    }

    /**
     * 统计信息 存储
     * @param bool $flush
     */
    public static function infoTick($flush = false){
        static $last_time = 0;
        if ((static::$tickTime - $last_time) >= 60) { //每60秒统计
            static::$perFailNum = 0;
            $last_time = static::$tickTime;
        }
        static::$perFailNum += static::$failCount;
        static::$infoStats['date'] = date("Y-m-d H:i:s", static::$tickTime);
        //static::$infoStats['waiting_full'] = static::$isWaitingFull;
        static::$infoStats['queue_count'] += static::$queueCount;
        static::$infoStats['handle_count'] += static::$handleCount;
        static::$infoStats['fail_count'] += static::$failCount;
        static::$infoStats['delay_count'] = static::$delayCount; //延迟总数
        static::$infoStats['retry_count'] = static::$retry->getCount();
        static::$infoStats['per_fail_num'] = static::$perFailNum;
        static::$infoStats['real_recv_num'] = static::$realRecvNum;
        static::$infoStats['real_pop_num'] = static::$realPopNum;
        static::$infoStats['real_push_num'] = static::$realPushNum;
        static::$infoStats['waiting_delay_num'] = static::$delay->waitingCount(); //最近时段待处理的延迟
        static::$infoStats['waiting_num'] = static::$waitingCount;
        static::$infoStats['topic_count'] = 0;
        static::$infoStats['topic_list'] = [];

        //待处理数量
        static::$infoStats['topic_count'] = count(static::$queueData);
        foreach (static::$queueData as $topic => $queue) {
            static::$infoStats['topic_list'][$topic] = $queue->count();
        }
        //预警
        MQLib::alarm(MQLib::ALARM_WAITING, static::$infoStats['waiting_num']);
        MQLib::alarm(MQLib::ALARM_RETRY, static::$infoStats['retry_count']);
        MQLib::alarm(MQLib::ALARM_FAIL, static::$infoStats['per_fail_num']);

        static::$queueCount = 0;
        static::$handleCount = 0;
        //static::$delayCount = 0;
        static::$failCount = 0;
        static::$realPopNum = 0;
        static::$realPushNum = 0;
        static::$realRecvNum = 0;

        $flush && file_put_contents(SrvBase::$instance->runDir . '/' . SrvBase::$instance->serverName() . '.info', json_encode(static::$infoStats), LOCK_EX);
    }

    /**
     * 进程启动时处理
     * @param $worker
     * @param $worker_id
     * @throws Exception
     */
    public static function onWorkerStart($worker, $worker_id)
    {
        ini_set('memory_limit', GetC('memory_limit', '512M'));
        MQLib::initConf();
        static::$queueStep = GetC('queue_step', 60) * 60;
        $time = time();
        $stepTime = static::stepTime($time);
        $nextStepTime = $stepTime + static::$queueStep;
        static::$next2StepTime = $nextStepTime + static::$queueStep;
        static::$nextClearFlag = $time;
        static::$tickTime = $time;
        static::$isMem = !GetC('db.name');
        static::$isSqlite = GetC('db.dbms') == 'sqlite';
        static::$maxWaitingNum = GetC('max_waiting_num', 0);
        static::$dataExpired = intval(GetC('data_expired', 1440) * 60);
        if (GetC('allow_topic_list')) {
            static::$allowTopicList = array_fill_keys(explode(',', GetC('allow_topic_list')), 1);
        }
        static::$multiSplit = GetC('multi_split', "\r");
        static::$delayClass = GetC('delay_class', DelayPHP::class);
        if (empty(static::$delayClass) || !class_exists(static::$delayClass)) {
            static::$delayClass = DelayPHP::class;
        }
        static::$retryClass = GetC('retry_class', RetryRedis::class);
        if (empty(static::$retryClass) || !class_exists(static::$retryClass)) {
            static::$retryClass = RetryRedis::class;
        }
        static::$delay = new static::$delayClass();
        static::$retry = new static::$retryClass();
        // 清除redis延迟缓存
        static::$delay->clear();
        //间隔时段 移除无用的缓存队列名|下下间隔时段的延迟数据|过期数据清理
        static::toQueueStep();

        //初始统计信息
        static::initInfo(static::$isMem);
        //初始队列
        static::initQueue();
        //初始重试数据
        static::initRetry();

        // 延迟入列|重试入列|更新mq最后使用id数据|更新队列数据的状态
        $worker->tick(1000, function () {
            static $lastTime = 0;
            static::$tickTime = time();
            //延迟入列
            $delayCount = static::$delay->tick();
            static::$delayCount -= $delayCount;

            //重试入列
            $retryCount = static::$retry->tick();

            //信息统计
            static::infoTick(date("s") == '59');

            SrvBase::$isConsole && SrvBase::safeEcho(sprintf('tickTime:%s, delayCount:%s, retryCount:%s', static::$tickTime, $delayCount, $retryCount).PHP_EOL);

            //更新队列数据的状态
            static::toQueueUpdate();

            //重置|更新last_id
            if (!$lastTime) {
                $lastTime = static::$tickTime;
            } else {
                $time = static::$tickTime;
                if (($time - $lastTime) >= static::$queueStep) {
                    $lastTime = $time;
                    static::$cacheQueueUpdate = [];
                    //更新mq最后使用id数据
                    static::toMqListLastId();
                    //延迟数据载入
                    static::toDelayLoad();
                    //间隔时段 移除无用的缓存队列名|下下间隔时段的延迟数据|过期数据清理  todo 优化
                    static::toQueueStep();
                }
            }
        });
        //n ms实时数据落地
        $worker->tick(200, function () {
            static::writeToDisk();
        });
    }

    /**
     * 终端数据进程结束时的处理
     * @param $worker
     * @param $worker_id
     * @throws \Exception
     */
    public static function onWorkerStop($worker, $worker_id)
    {
        static::$tickTime = time();
        //1 进程结束时把缓存的数据写入到磁盘
        static::writeToDisk();
        //2 更新队列数据的状态
        static::toQueueUpdate();
        //3 更新mq最后使用id数据
        static::toMqListLastId(true);
        //4 信息统计
        static::infoTick(true);
        //5 持久缓存重试数据
        static::toRetrySave();
        // 清除redis重试缓存
        static::$retry->clear();
        // 清除redis延迟缓存
        static::$delay->clear();
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
        static::$realRecvNum++;
        \SrvBase::$isConsole && \SrvBase::safeEcho($recv . PHP_EOL);
        Log::trace($recv);

        //认证处理
        $authRet = MQLib::auth($con, $fd, $recv);
        if (!$authRet) {
            //static::err(MQLib::err());
            MQLib::toClose($con, $fd, MQLib::err());
            return '';
        }
        if($authRet==='ok'){
            return 'ok';
        }

        // 批量消息 开头$标识 数据使用指定分隔符分隔
        if ($recv[0] == '$') {
            $ret = [];
            $offset = 1;
            $len = strlen(static::$multiSplit);
            while ($pos = strpos($recv, static::$multiSplit, $offset)) {
                $_recv = substr($recv, $offset, $pos-$offset);
                $ret[] = static::handle($con, $_recv, $fd);
                $offset = $pos + $len;
            }
            return $ret;
        }
        return static::handle($con, $recv, $fd);
    }

    protected static function handle($con, $recv, $fd=0){
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

        $ret = 'ok'; //默认返回信息
        switch ($data['cmd']) {
            case 'push': //入列 用于消息重试
                $data['host'] = MQLib::remoteIp($con, $fd);
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
                $ret = static::$infoStats;
                break;
            case 'delay_cancel': //取消指定延迟队列  queue_name=xx&id=xxx
                static::delayCancel($data);
                break;
            case 'stats_reset': //统计重置
                static::initInfo(true);
                break;
            case 'retry_clear': //清空重试缓存
                static::$retry->clear();
                break;
            default:
                self::err('invalid cmd');
                $ret = false;
        }
        return $ret;
    }

    /**
     * @param $data
     * @return array|bool
     * @throws Exception
     */
    public static function push($data){
        static::$realPushNum++;
        if (empty($data['topic']) || empty($data['data'])) {
            static::err('topic or data is empty');
            return false;
        }
        $topic_length = strlen($data['topic']);
        if ($topic_length > MQLib::MAX_TOPIC_LENGTH) {
            $data['topic'] = substr($data['topic'], 0, MQLib::MAX_TOPIC_LENGTH);
        }
        $data_length = strlen($data['data']);
        if ($data_length > MQLib::MAX_DATA_LENGTH) {
            static::err('data length[' . $data_length . '] is too long');
            return false;
        }

        if (static::$allowTopicList && !isset(static::$allowTopicList[$data['topic']])) {
            static::err('Invalid topic');
            return false;
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
            static::$waitingCount++;
            return [$data['id'], $data['queueName']];
        }

        //允许等待队列数
        if (static::$maxWaitingNum > 0 && static::$maxWaitingNum < static::$waitingCount) {
            static::err('waiting mq is full');
            return false;
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
            'status'=> 0,
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

        if (isset($data['seq_id'])) { // todo seq_id相同的只能由同一个消费者处理
            $queueData['seq_id'] = (int)$data['seq_id'];
        }

        $queueName = static::queueName($ctime);
        static::$bufferSize += $data['len'];
        static::$queueCount++;
        static::$bufferData[$queueName]->enqueue($queueData);

        if ($sync && static::$bufferSize > static::$maxBufferSize) {
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
            static::$waitingCount--;
            list($queueName, $id, $ack, $retry, $data) = explode(',', $package_str, 5); //[$queueName, $id, $ack, $retry, $data]
            //todo 延迟取消 判断处理
            static::$realPopNum++;
            //记录最后出列id
            if (!isset(static::$cacheMqListLastId[$queueName]) || static::$cacheMqListLastId[$queueName] < $id) {
                static::$cacheMqListLastId[$queueName] = $id;
            }
            if ($ret === '') $ret = $package_str;
            else $ret .= static::$multiSplit . $package_str;

            //加入重试集合
            $retry_step = 0;
            if(strpos($retry,'-')){
                list($retry, $retry_step) = explode('-', $retry);
                $retry_step = (int)$retry_step;
            }
            $retry = (int)$retry;
            if ($retry > $retry_step) { //进程结束会记录到持久缓存表 意外关机除外
                $retryTime = $time + MQLib::getRetryStep($topic, $retry_step);
                static::$retry->add($id, $retryTime, $retryTime .','. $topic . ',' . $queueName . ',' . $id . ',' . $ack . ',' . $retry.'-'.($retry_step+1) . ',' . $data, $retry_step+1);
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
        $status = isset($data['status']) ? (int)$data['status'] : 1;
        if (strlen((string)$id) != 19) return; //id固定长度19位
        if ($status === MQLib::STATUS_TODO) $status = MQLib::STATUS_FAIL;
        if ($status === MQLib::STATUS_FAIL) {
            static::$failCount++;
        }

        if ($queueName == '') { //延迟的取name不正确
            $time = (int)substr((string)$id, 0, 10);
            $step = static::$queueStep;
            $queueName = date('mdHi', (int)floor($time / $step) * $step);;
        }

        static::$retry->clean($id);
        $result = $data['result'] ?? '';
        $data = ['status' => $status];
        if ($result!=='') $data['result'] = $result;
        static::queueUpdate($queueName, $id, $data);
    }

    protected static function delayCancel($data){
        $id = $data['id']??0;
        $queueName = $data['queue_name']??'';
        $id && $queueName && static::queueUpdate($queueName, $id,  ['status' => MQLib::STATUS_CANCEL]);
        //todo 在缓存队列的 打下标记方便在出列时判断
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
    public static function queueUpdate($queueName, $id, $data)
    {
        if (static::$isMem) return;
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
     * 将数据写入磁盘
     * @throws \Exception
     */
    protected static function writeToDisk()
    {
        if (static::$bufferSize == 0) return;
        $time = time();
        if(static::$isMem){
            foreach (static::$bufferData as $queueName => $queue) {
                try {
                    static::$delay->beforeAdd();
                    while(!$queue->isEmpty()){
                        $item = $queue->dequeue();
                        $queue_str = $queueName . ',' . $item['id'] . ',' . $item['ack'] . ',' . $item['retry'] . ',' . $item['data'];
                        //推送数据
                        if ($item['ctime'] <= $time) {
                            static::$queueData[$item['topic']]->enqueue($queue_str);
                            static::$waitingCount++;
                        } else {
                            static::$delayCount++;
                            static::$delay->add($item['topic'], $item['ctime'], $queue_str);
                        }
                    }
                    static::$delay->afterAdd();
                } catch (\Exception $e) {
                    MQLib::alarm(MQLib::ALARM_FAIL, '数据入列失败:'.$e->getMessage());
                }
            }
            return;
        }
        foreach (static::$bufferData as $queueName => $queue) {
            db()->beginTrans();
            try {
                $n = 0;
                $data = [];
                $endId = 0;
                $tableName = MQLib::QUEUE_TABLE_PREFIX . $queueName;
                static::$delay->beforeAdd();
                while(!$queue->isEmpty()){
                    $item = $queue->dequeue();
                    $n++;
                    $queue_str = $queueName . ',' . $item['id'] . ',' . $item['ack'] . ',' . $item['retry'] . ',' . $item['data'];
                    //推送数据
                    if ($item['ctime'] <= $time) {
                        static::$queueData[$item['topic']]->enqueue($queue_str);
                        static::$waitingCount++;
                    } else {
                        static::$delayCount++;
                        if ($item['ctime'] < static::$next2StepTime) { //延时在一间隔时间段内的直接加入缓存 超过定时读入缓存
                            static::$delay->add($item['topic'], $item['ctime'], $queue_str);
                        }
                    }

                    $data[] = $item;
                    $endId = $item['id'];
                    if ($n > 49) {
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
                static::$delay->afterAdd();
            } catch (\Exception $e) {
                db()->rollBack();
                //Log::write((string)$e, 'writeToDisk');
                MQLib::alarm(MQLib::ALARM_FAIL, '数据写入磁盘失败:'.$e->getMessage());
            }
        }
        static::$bufferSize = 0;
    }
}