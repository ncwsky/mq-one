<?php

class DelayRedis implements DelayInterface
{
    /**
     * @var MyRedis|lib_redis
     */
    private $redis;
    private $count = 0;
    private $waiting_count = 0;

    public function __construct()
    {
        $this->redis = redis();
    }

    public function getCount(){
        return $this->count;
    }

    public function waitingCount(){
        return $this->waiting_count;
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
                $this->count -= $_count;
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
        $this->count++;
        $this->waiting_count++;
        return $this->redis->zAdd(MQLib::$prefix . MQLib::QUEUE_DELAYED . $topic, $time, $queue_str);
    }

    public function afterAdd(){
        $this->redis->exec();
    }

    public function clear(){
        $this->count = 0;
        $this->waiting_count = 0;
        $delayedList = (array)redis()->keys(MQLib::$prefix . MQLib::QUEUE_DELAYED . '*');
        $delayedList && redis()->del($delayedList);
    }
}