<?php

class DelayPHP implements DelayInterface
{
    private $count = 0;
    private $waiting_count = 0;

    /**
     * 接收数据缓存
     * @var SplPriorityQueue[]
     */
    protected static $delayData = [];

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
        $count = 0;
        foreach (static::$delayData as $topic => $queue) {
            while ($queue->valid()) {
                $data = $queue->top();
                if (-$data['priority'] > $now) {
                    break;
                }
                $queue->extract();
                $this->count--;
                $this->waiting_count--;

                list($queueName, $id, $ack, $retry, $data) = explode(',', $data['data'], 5);
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
            /*
            if ($queue->isCorrupted()) {
                $queue->recoverFromCorruption();
            }*/
        }
        return $count;
    }

    public function beforeAdd(){}

    public function add($topic, $time, $queue_str)
    {
        $this->count++;
        $this->waiting_count++;
        if (!isset(static::$delayData[$topic])) {
            static::$delayData[$topic] = new SplPriorityQueue();
            static::$delayData[$topic]->setExtractFlags(SplPriorityQueue::EXTR_BOTH);
        }

        return static::$delayData[$topic]->insert($queue_str, -$time);
    }

    public function afterAdd(){}

    public function clear(){
        $this->count = 0;
        $this->waiting_count = 0;
        static::$delayData = [];
    }
}