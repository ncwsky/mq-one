#!/usr/bin/env php
<?php
declare(strict_types=1);

require __DIR__ . '/conf.php';
require __DIR__ . '/../myphp/base.php';
require __DIR__ . '/../myphp/GetOpt.php';

var_dump(redis()->subscribe('abc','ab'));die();
while(1){
    var_dump(redis()->parseResponse());
    //sleep(1);
}
die();
//解析命令参数
GetOpt::parse('h:c:', ['host:', 'cmd:']);
$cmd =GetOpt::val('c', 'cmd');
$host = GetOpt::val('h', 'host', '192.168.0.245:55011');

$client = TcpClient::instance('', $host);
$client->onInput = function ($buffer) {
    return MQPackN2::input($buffer);
};
$client->onEncode = function ($buffer) {
    return MQPackN2::encode($buffer);
};
$client->onDecode = function ($buffer) {
    return MQPackN2::decode($buffer);
};
$cmd && $client->send('cmd='.$cmd);
while (1) {
    try {
        $client->send('cmd=stats');
        $ret = $client->recv();
        echo $ret, PHP_EOL;
    } catch (Exception $e) {
        echo $e->getMessage().PHP_EOL;
        sleep(1);
    }
    sleep(1);
}

