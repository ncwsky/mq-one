#!/usr/bin/env php
<?php
declare(strict_types=1);

require __DIR__ . '/conf.php';
require __DIR__ . '/../myphp/base.php';

/*
$queueName = 'message_queue03141800';
$time = time();
#lib_redis::$isExRedis = false;# redis()->multi();
redis()->del(MQLib::QUEUE_DELAYED_PREFIX);
redis()->multi(MyRedis::PIPELINE);
for($i=0;$i<1280;$i++){
    $item = ['topic'=>'cmd','ctime'=>$time+(mt_rand(0, $i)), 'id'=>$i,'ack'=>1];
    redis()->zAdd(MQLib::QUEUE_DELAYED_PREFIX, $item['ctime'], $queueName . ',' . $item['id'] . ',' . $item['ack'] . ',' . $item['topic']);
}
redis()->exec();

var_dump(redis()->ZCARD(MQLib::QUEUE_DELAYED_PREFIX));
//延迟数据入列
$now = time();
$options = ['LIMIT', 0, 128];
$items = redis()->zrevrangebyscore(MQLib::QUEUE_DELAYED_PREFIX, $now, '-inf');

if($items){
    foreach ($items as $package_str) {
        echo $package_str,PHP_EOL;
    }
    $del = redis()->zRemRangeByScore(MQLib::QUEUE_DELAYED_PREFIX, '-inf', $now);
    var_dump($del);
}
var_dump(redis()->ZCARD(MQLib::QUEUE_DELAYED_PREFIX));
*/
/*
$bigId = [];
$hasRepeat = [];

for ($i = 0; $i < 99998; $i++) {
    $val = MQLib::bigId();
    if (isset($bigId[$val])) {
        if (isset($hasRepeat[$val])) $hasRepeat[$val]++;
        else $hasRepeat[$val] = 2;
    } else {
        $bigId[$val] = 1;
    }
}
var_dump($hasRepeat);*/
$testCount = empty($argv[1]) ? 0 : (int)$argv[1];
if ($testCount <= 0) $testCount = 10000;

$topic = 'cmd';
$data = 'php yii mch-order/tm-ymd-report -1';

$rawData = MQPackN2::toEncode('topic=cmd&data='.$data);
//file_put_contents(__DIR__.'/tcp', $rawData);

$client = TcpClient::instance();
$client->config('192.168.0.245:55011');
$client->onDecode = function ($buffer) {
    $buffer = rtrim($buffer, "\n");
    return substr($buffer, 6);
};
$count = 0;
while(1){
    $topic = 'cmd';
    $data = 'php yii mch-order/tm-ymd-report -1';
    $delay = mt_rand(0, 9) ? 0 : mt_rand(10, 600);
    $retry = mt_rand(0, 5) ? 0 : mt_rand(1, 7);
    $ack = mt_rand(0, 9) ? 0 : 1;
    $rawData = MQPackN2::toEncode('topic=cmd&data=' . $data . '&delay=' . $delay . '&retry=' . $retry . '&ack=' . $ack);
    try {
        if ($client->send($rawData)) {
            $count++;
        }
        $ret = $client->recv();
        //echo $ret, PHP_EOL;
    } catch (Exception $e) {
        echo $e->getMessage(), PHP_EOL;
        sleep(1);
    }
    if($count>=$testCount) break;
}
echo $count,' use time:', run_time(),PHP_EOL;

