<?php

class DelayPHP implements DelayInterface
{
    /**
     * 接收数据缓存
     * @var SplPriorityQueue[]
     */
    protected static $delayData = [];

    public function tick($srvConn)
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
                ++$count;

                list($queueName, $id, $ack, $retry, $data) = explode(',', $data['data'], 5);
                $push = [
                    'id' => $id,
                    'topic' => $topic,
                    'queueName' => $queueName,
                    'ack' => $ack,
                    'retry' => $retry,
                    'data' => $data
                ];
                $srvConn->send(toJson($push));
            }
        }
        return $count;
    }

    public function beforeAdd(){}

    public function add($topic, $time, $queue_str)
    {
        if (!isset(static::$delayData[$topic])) {
            static::$delayData[$topic] = new SplPriorityQueue();
            static::$delayData[$topic]->setExtractFlags(SplPriorityQueue::EXTR_BOTH);
        }

        return static::$delayData[$topic]->insert($queue_str, -$time);
    }

    public function afterAdd(){}

    public function clear(){
        static::$delayData = [];
    }
}