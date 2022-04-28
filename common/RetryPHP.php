<?php

class RetryPHP implements RetryInterface
{
    private $hash = []; // [step,data],...
    private $list = []; //id->time,...
    /**
     * @var array 所有重试间隔
     */
    private $timeStep = [];
    private $timeStepNum = 0;

    public function __construct()
    {
        $timeStep = [];
        foreach (MQLib::retryStep() as $name => $steps) {
            foreach ($steps as $step) {
                $timeStep[$step] = 1;
            }
        }
        $this->timeStep = array_keys($timeStep);
        $this->timeStepNum = count($this->timeStep);
        sort($this->timeStep);
        Log::write($this->timeStep, 'retry-php');
    }

    public function __toString()
    {
        return json_encode(['hash' => $this->hash, 'list' => $this->list]);
    }

    public function getCount(){
        return count($this->hash);
    }

    /**
     * 定时延时入列数据
     * @return int 入列数
     */
    public function tick(){
        $count = 0;
        //重试入列
        $now = time();
        for ($retry_step = 1; $retry_step <= $this->timeStepNum; $retry_step++) {
            if(empty($this->list[$retry_step])) continue;
            foreach ($this->list[$retry_step] as $id => $time) {
                if ($time > $now) break;
                $count++;

                //"$topic,$queueName,$id,$ack,$retry-$retry_step,$data"
                $package_str = $this->getData($id); //
                if (!$package_str) { //数据可能被清除
                    unset($this->hash[$id], $this->list[$retry_step][$id]);
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
                list(, $retry_step) = explode('-', $retry, 2);
                MQServer::queueUpdate($queueName, $id, ['retry_count'=>(int)$retry_step]);
                MQServer::push($push);
            }
        }
        return $count;
    }

    public function beforeAdd(){}

    public function add($id, $time, $data, $retry_step=null){
        if($retry_step===null){
            list(, , , , $retry,) = explode(',', $data, 6);
            list(, $retry_step) = explode('-', $retry, 2);
        }
        $this->hash[$id] = [$retry_step, $data];
        if (!isset($this->list[$retry_step])) {
            $this->list[$retry_step] = [];
        }
        $this->list[$retry_step][$id] = $time;
    }

    public function afterAdd(){}

    public function getIdList(){
        $idList = new SplFixedArray();
        $i = 0;
        foreach ($this->list as $stepId2times){
            $idList->setSize($i+count($stepId2times));
            foreach ($stepId2times as $id=>$time){
                $idList[$i] = $id;
                $i++;
            }
        }
        return $idList;
    }

    public function getData($id){
        return $this->hash[$id][1] ?? null;
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
        if (isset($this->hash[$id])) {
            $retry_step = $this->hash[$id][0];
            unset($this->hash[$id], $this->list[$retry_step][$id]);
        }
    }

    public function clear()
    {
        $this->hash = [];
        $this->list = [];
    }
}