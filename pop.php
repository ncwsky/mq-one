#!/usr/bin/env php
<?php
declare(strict_types=1);

require __DIR__ . '/conf.php';
require __DIR__ . '/../myphp/base.php';

$client = TcpClient::instance();
$client->config('192.168.0.245:55011');
$client->onEncode = function ($buffer) {
    return MQPackN2::toEncode($buffer) . "\n";
};
$client->onDecode = function ($buffer) {
    $buffer = rtrim($buffer, "\n");
    return substr($buffer, 6);
};
while (1) {
    try {
        $client->send('cmd=pop&topic=cmd');
        $ret = $client->recv();
        echo date("Y-m-d H:i:s") . ' recv: ' . $ret, PHP_EOL;

        if (!$ret) continue;

        $mqList = explode("\r", $ret);
        foreach ($mqList as $mq) {
            if (strlen($mq)> 32 && substr_count($mq, ',', 0, 32)==3) {
                list($queueName, $id, $ack, $retry, $data) = explode(',', $mq, 5);
                $output = [];
                //exec($data . ' 2>&1', $output, $code); //将标准错误输出重定向到标准输出
                $result = '';//implode(PHP_EOL, $output);
                echo date("Y-m-d H:i:s") . ' handle: ' . $result, PHP_EOL;
                if (intval($ack) > 0 || intval($retry) > 0) {
                    $ack_str = 'cmd=ack&id=' . $id . '&queueName=' . $queueName . '&status=1&result=' . $result;
                    if (mt_rand(0, 9) <= 7) {
                        $client->send($ack_str);
                        $ok = $client->recv();
                        echo date("Y-m-d H:i:s") . ' ack: ' . $ack_str . ' -> ' . $ok, PHP_EOL;
                    }
                } else {
                    echo date("Y-m-d H:i:s") . ' ack: no', PHP_EOL;
                }
            }
        }
    } catch (Exception $e) {
        echo date("Y-m-d H:i:s") . ' ' . $e->getMessage(), PHP_EOL;
        sleep(2);
    }
}

