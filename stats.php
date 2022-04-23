#!/usr/bin/env php
<?php
declare(strict_types=1);

require __DIR__ . '/conf.php';
require __DIR__ . '/../myphp/base.php';

$client = TcpClient::instance('', '127.0.0.1:55011');
//$client->type = 'udp';
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

