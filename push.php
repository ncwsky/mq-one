#!/usr/bin/env php
<?php
declare(strict_types=1);

require __DIR__ . '/conf.php';
require __DIR__ . '/../myphp/base.php';
require __DIR__ . '/../myphp/GetOpt.php';

//解析命令参数
GetOpt::parse('h:n:', ['host:', 'num:']);
$testCount = (int)GetOpt::val('n', 'num', 0);
$host = GetOpt::val('h', 'host', '192.168.0.245:55011');
if ($testCount <= 0) $testCount = 10000;
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


$topic = 'cmd';
$data = 'php yii mch-order/tm-ymd-report -1';

$rawData = MQPackN2::toEncode('cmd=retry_clear');
//file_put_contents(__DIR__.'/tcp', $rawData);

$client = TcpClient::instance();
$client->config($host);
$client->onInput = function ($buffer) {
    //return MQPackN2::toEncode($buffer) . "\n";
    return MQPackN2::input($buffer);
};
$client->onEncode = function ($buffer) {
    //return MQPackN2::toEncode($buffer) . "\n";
    return MQPackN2::toEncode($buffer);
};
$client->onDecode = function ($buffer) {
    //$buffer = rtrim($buffer, "\n");
    return substr($buffer, 6);
};
//认证
$client->onConnect = function ($client){
    //$client->send('123456');
    //$client->recv();
};

$rand = '~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789~!@#$%^&*_-abcdefghijkmnpqrstuvwxyzABCDEFGHIJKLMNPRSTUVWXYZ0123456789';
$count = 0;
while(1){
    $topic = 'cmd';
    $data = 'php yii mch-order/tm-ymd-report -1 ' . mt_rand(0, 1000000) . ' ' . $rand;
    $delay = mt_rand(0, 9) ? 0 : mt_rand(10, 480);
    $retry = mt_rand(0, 9) ? 0 : mt_rand(1, 7);
    $ack = mt_rand(0, 2) ? 0 : 1;
    $rawData = 'topic=cmd&data=' . urlencode($data) . '&delay=' . $delay . '&retry=' . $retry . '&ack=' . $ack;
    try {
        if ($client->send($rawData)) {
            $count++;
        }
        $ret = $client->recv();
        echo $ret, PHP_EOL;
        usleep(mt_rand(100, 10000)); //模拟处理时间
    } catch (Exception $e) {
        echo $e->getMessage(), PHP_EOL;
        sleep(1);
    }
    if($count>=$testCount) break;
}
echo $count,' use time:', run_time(),PHP_EOL;

