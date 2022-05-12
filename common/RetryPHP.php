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
        //初始重试列表
        for ($i = 0; $i <= $this->timeStepNum; $i++) {
            $this->list[$i] = [];
        }

        Log::write($this->timeStepNum.' -> '.json_encode($this->timeStep).'|'.json_encode($this->list), 'retry-php');
    }

    public function __toString()
    {
        if (count($this->hash) <= 200) {
            return json_encode(['hash' => $this->hash, 'list' => $this->list]);
        }
        return json_encode(['hash' => count($this->hash), 'list' => count($this->list)]);
    }

    public function getCount(){
        return count($this->hash);
    }

    public function stats(){
        $counts = [];
        foreach ($this->list as $step=>$items){
            $counts[$step] = count($items);
        }
        return ['count'=>array_sum($counts), 'list'=>$counts, 'hash_count'=>count($this->hash)];
    }

    /**
     * 定时延时入列数据
     * @return int 入列数
     */
    public function tick(){
        $count = 0;
        //重试入列
        //$now = time();
        for ($retry_step = 1; $retry_step <= $this->timeStepNum; $retry_step++) {
            SrvBase::$isConsole && SrvBase::safeEcho('retry tick -> retry_step:'.$retry_step.', count:'.(isset($this->list[$retry_step])? count($this->list[$retry_step]):'null') .PHP_EOL);
            //file_put_contents(SrvBase::$instance->runDir . '/' . SrvBase::$instance->serverName() . '.retry', (string)$this);
            if(empty($this->list[$retry_step])) continue;
            foreach ($this->list[$retry_step] as $id => $time) {
                if ($time > MQServer::$tickTime) break;
                $count++;

                //"$retryTime,$topic,$queueName,$id,$ack,$retry-$retry_step,$data"
                $package_str = $this->getData($id); //
                if (!$package_str) { //数据可能被清除
                    unset($this->hash[$id], $this->list[$retry_step][$id]);
                    --$count;
                    continue;
                }

                list(, $topic, $queueName, $id, $ack, $retry, $data) = explode(',', $package_str, 7);
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

    /**
     * 添加重试数据
     * @param int $id
     * @param int $time
     * @param string $data
     * @param int|null $retry_step
     * @return mixed
     */
    public function add($id, $time, $data, $retry_step=null){
        if($retry_step===null){
            list(, , , , , $retry,) = explode(',', $data, 7);
            list(, $retry_step) = explode('-', $retry, 2);
            $retry_step = (int)$retry_step;
        }
        $this->hash[$id] = [$retry_step, $data];
/*        if (!isset($this->list[$retry_step])) {
            $this->list[$retry_step] = [];
            SrvBase::safeEcho('retry_step : '.$retry_step.PHP_EOL);
        }*/
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

    /**
     * @param int $id
     * @return mixed|string|null
     */
    public function getData($id){
        return $this->hash[$id][1] ?? null;
    }

    /**
     * 清除重试
     * @param int $id
     * @param bool $retry 是否重试清除
     */
    public function clean($id, $retry = false){
        //Log::DEBUG("<- " . ($retry ? 'Retry' : 'Recv') . " PUBACK package, id:$id");
        if (SrvBase::$isConsole) {
            //SrvBase::safeEcho(date("Y-m-d H:i:s")." <- " . ($retry ? 'Retry' : 'Recv') . " PUBACK package, id:$id" . PHP_EOL);
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