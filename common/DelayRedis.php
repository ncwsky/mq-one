<?php

class DelayRedis implements DelayInterface
{
    /**
     * @var MyRedis|lib_redis
     */
    private $redis;
    private $waiting_count = 0;

    public function __construct()
    {
        $this->redis = redis();
    }

    public function waitingCount(){
        return $this->waiting_count;
    }

    //取重试最小id 用于服务结束重启的载入处理
    public function stop2minId(){
        $minId = 0;
        $minQueueName = '';
        $delayedList = (array)$this->redis->keys(MQLib::$prefix . MQLib::QUEUE_DELAYED . '*');
        foreach ($delayedList as $delayed) {
            $items = $this->redis->zRange($delayed, 0, -1); //, 'WITHSCORES'
            if($items){
                foreach ($items as $item){
                    list($queueName, $id, $ack, $retry, $data) = explode(',', $item, 5);
                    if($minId==0 || $minId>$id) {
                        $minId = $id;
                        $minQueueName = $queueName;
                    }
                }
            }
            /*
            $items = $this->redis->ZRANGEBYSCORE($delayed, '-inf', '+inf');
            if ($items) {
                foreach ($items as $item){
                    list($queueName, $id, $ack, $retry, $data) = explode(',', $item, 5);
                    if($minId==0 || $minId>$id) {
                        $minId = $id;
                        $minQueueName = $queueName;
                    }
                }
            }
            */
        }
        return [$minId, $minQueueName];
    }

    public function tick()
    {
        //延迟入列
        $now = time();
        $delayedList = (array)$this->redis->keys(MQLib::$prefix . MQLib::QUEUE_DELAYED . '*');
        $delayedLen = strlen(MQLib::$prefix . MQLib::QUEUE_DELAYED);
        $count = 0;
        foreach ($delayedList as $delayed) {
            //$options = ['LIMIT', 0, 128];
            $topic = substr($delayed, $delayedLen);
            $items = $this->redis->zrevrangebyscore($delayed, $now, '-inf');
            if ($items) {
                $_count = count($items);
                $count += $_count;
                $this->waiting_count -= $_count;
                foreach ($items as $package_str) {
                    list($queueName, $id, $ack, $retry, $data) = explode(',', $package_str, 5);
                    $push = [
                        'id' => $id,
                        'topic' => $topic,
                        'queueName' => $queueName,
                        'ack' => $ack,
                        'retry' => $retry,
                        'data' => $data
                    ];
                    MQServer::push($push);
                }
                $this->redis->zremrangebyscore($delayed, '-inf', $now);
            }
        }
        return $count;
    }

    public function beforeAdd(){
        $this->redis->multi(MyRedis::PIPELINE);
        $this->redis->retries = 1;
    }

    public function add($topic, $time, $queue_str)
    {
        $this->waiting_count++;
        return $this->redis->zAdd(MQLib::$prefix . MQLib::QUEUE_DELAYED . $topic, $time, $queue_str);
    }

    public function afterAdd(){
        $this->redis->exec();
    }

    public function clear(){
        $this->waiting_count = 0;
        $delayedList = (array)redis()->keys(MQLib::$prefix . MQLib::QUEUE_DELAYED . '*');
        $delayedList && redis()->del($delayedList);
    }
}