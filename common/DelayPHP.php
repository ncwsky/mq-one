<?php

class DelayPHP implements DelayInterface
{
    /**
     * 接收数据缓存
     * @var SplPriorityQueue[]
     */
    protected static $delayData = [];

    public function waitingCount(){
        $waiting_count = 0;
        foreach (static::$delayData as $topic => $queue){
            $waiting_count += $queue->count();
        }
        return $waiting_count;
    }

    //取重试最小id 用于服务结束重启的载入处理
    public function stop2minId(){
        $minId = 0;
        $minQueueName = '';
        foreach (static::$delayData as $topic => $queue) {
            if ($queue->valid()) {
                //$queue->setExtractFlags(SplPriorityQueue::EXTR_BOTH);
                foreach ($queue as $item){
                    //$item['priority']; $queueName, $id, $ack, $retry, $data
                    list($queueName, $id, , , ) = explode(',', $item['data'], 5);
                    if($minId==0 || $minId>$id) {
                        $minId = $id;
                        $minQueueName = $queueName;
                    }
                }
            }
        }
        return [$minId, $minQueueName];
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