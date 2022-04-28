<?php

class RetryRedis implements RetryInterface
{
    /**
     * @var MyRedis|lib_redis
     */
    private $redis;

    public function __construct()
    {
        $this->redis = redis();
    }
    //重试数
    public function getCount(){
        return (int)$this->redis->hlen(MQLib::$prefix . MQLib::QUEUE_RETRY_HASH);
        //return (int)$this->redis->zcard(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST);
    }

    /**
     * 定时延时入列数据
     * @return int 入列数
     */
    public function tick(){
        $count = 0;
        //重试入列
        $now = time();
        $items = $this->redis->zrevrangebyscore(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, $now, '-inf');
        if(!$items) return $count;

        $count = count($items);
        $this->redis->retries = 1;
        foreach ($items as $id) {
            //"$topic,$queueName,$id,$ack,$retry-$retry_step,$data"
            $package_str = $this->getData($id); //
            if (!$package_str) { //数据可能被清除
                $this->redis->zRem(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, $id);
                $this->redis->hdel(MQLib::$prefix . MQLib::QUEUE_RETRY_HASH, $id);
                --$count;
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
            $this->clean($id, true); #清除
            list($retry, $retry_step) = explode('-', $retry, 2);
            MQServer::queueUpdate($queueName, $id, ['retry_count'=>(int)$retry_step]);
            MQServer::push($push);
        }
        return $count;
    }

    public function beforeAdd(){
        $this->redis->multi(MyRedis::PIPELINE);
        $this->redis->retries = 1;
    }

    public function add($id, $time, $data, $retry_step=null){
        $this->redis->zAdd(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, $time, $id);
        $this->redis->hset(MQLib::$prefix . MQLib::QUEUE_RETRY_HASH, $id, $data);
    }

    public function afterAdd(){
        $this->redis->exec();
    }

    public function getIdList(){
        return $this->redis->ZRANGEBYSCORE(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, '-inf', '+inf'); //time()-10
    }

    public function getData($id){
        return $this->redis->hget(MQLib::$prefix . MQLib::QUEUE_RETRY_HASH, $id);
    }

    /**
     * 清除重试
     * @param $id
     * @param bool $retry 是否重试清除
     */
    public function clean($id, $retry = false){
        Log::DEBUG("<- " . ($retry ? 'Retry' : 'Recv') . " PUBACK package, id:$id");
        if (SrvBase::$isConsole) {
            SrvBase::safeEcho(date("Y-m-d H:i:s")." <- " . ($retry ? 'Retry' : 'Recv') . " PUBACK package, id:$id" . PHP_EOL);
        }
        if ($this->redis->hExists(MQLib::$prefix . MQLib::QUEUE_RETRY_HASH, $id)) {
            $this->redis->zRem(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, $id);
            $this->redis->hdel(MQLib::$prefix . MQLib::QUEUE_RETRY_HASH, $id);
        }
    }

    public function clear()
    {
        $this->redis->del(MQLib::$prefix . MQLib::QUEUE_RETRY_LIST, MQLib::$prefix . MQLib::QUEUE_RETRY_HASH);
    }
}