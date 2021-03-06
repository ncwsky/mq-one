#!/usr/bin/env php
<?php
declare(strict_types=1);

require __DIR__ . '/conf.php';
require __DIR__ . '/../myphp/base.php';
require __DIR__ . '/../myphp/GetOpt.php';

while(1){
    var_dump(redis()->publish('abc', time()));
    usleep(10000);
    var_dump(redis()->publish('ab', date("Y-m-d H:i:s")));
    sleep(6);
}

die();
//解析命令参数
GetOpt::parse('h:n:', ['host:', 'num:']);
$testCount = (int)GetOpt::val('n', 'num', 0);
$host = GetOpt::val('h', 'host', '192.168.0.245:55011');
if ($testCount <= 0) $testCount = 0;

/*
$minId = 0;
$minQueueName = '';
$delayedList = (array)redis()->keys(MQLib::$prefix . MQLib::QUEUE_DELAYED . '*');
foreach ($delayedList as $delayed) {
    $arr = redis()->zRange($delayed, 0, -1); //, 'WITHSCORES'
    if($arr){
        foreach ($arr as $item){
            list($queueName, $id, $ack, $retry, $data) = explode(',', $item, 5);
            if($minId==0 || $minId>$id) {
                $minId = $id;
                $minQueueName = $queueName;
            }
        }
    }

    $items = redis()->ZRANGEBYSCORE($delayed, '-inf', '+inf');
    if ($items) {
        var_dump($items);
        foreach ($items as $item){
            list($queueName, $id, $ack, $retry, $data) = explode(',', $item, 5);
            if($minId==0 || $minId>$id) {
                $minId = $id;
                $minQueueName = $queueName;
            }
        }
    }
}
var_dump([$minId, $minQueueName]);die();
*/




$client = TcpClient::instance();
$client->config('192.168.0.245:55011');
$client->onInput = function ($buffer) {
    return MQPackN2::input($buffer);
};
$client->onEncode = function ($buffer) {
    return MQPackN2::encode($buffer);
};
$client->onDecode = function ($buffer) {
    return MQPackN2::decode($buffer);
};
$count = 0;
while (1) {
    try {
        $client->send('cmd=pop&topic=cmd');
        $ret = $client->recv();
        echo date("Y-m-d H:i:s") . ' recv: ' . $ret, PHP_EOL;

        if (!$ret) {
            sleep(1);
            continue;
        }
        $count++;

        $mqList = explode("\r", $ret);
        foreach ($mqList as $mq) {
            if (strlen($mq)> 32 && substr_count($mq, ',', 0, 32)==3) {
                list($queueName, $id, $ack, $retry, $data) = explode(',', $mq, 5);
                $output = [];
                //usleep(mt_rand(100000, 1000000)); //模拟处理时间
                //usleep(mt_rand(100, 10000)); //模拟处理时间
                //exec($data . ' 2>&1', $output, $code); //将标准错误输出重定向到标准输出
                $result = '';//implode(PHP_EOL, $output);
                echo date("Y-m-d H:i:s") . ' handle: ' . $result, PHP_EOL;
                if (intval($ack) > 0 || intval($retry) > 0) {
                    $ack_str = 'cmd=ack&id=' . $id . '&queueName=' . $queueName . '&status=' . (mt_rand(0, 9) ? 1 : 0) . '&result=' . $result;
                    if (mt_rand(0, 9) <= 6) {
                        $client->send($ack_str);
                        $ok = $client->recv();
                        echo date("Y-m-d H:i:s") . ' ack: ' . $ack_str . ' -> ' . $ok, PHP_EOL;
                    }
                } else {
                    echo date("Y-m-d H:i:s") . ' ack: no', PHP_EOL;
                }
            } else{
                echo date("Y-m-d H:i:s") . ' mq: invalid', PHP_EOL;
            }
        }
    } catch (Exception $e) {
        echo date("Y-m-d H:i:s") . ' err: ' . $e->getMessage(), PHP_EOL;
        sleep(2);
    }
    if ($testCount && $count >= $testCount) {
        break;
    }
}

