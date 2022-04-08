<?php

interface DelayInterface
{
    /**
     * 定时延时入列数据
     * @param TcpClient|\Workerman\Connection\AsyncTcpConnection $srvConn
     * @return int 入列数
     */
    public function tick($srvConn);

    public function beforeAdd();

    /**
     * 添加延迟数据
     * @param string $topic
     * @param int $time
     * @param string $queue_str
     * @return mixed
     */
    public function add($topic, $time, $queue_str);

    public function afterAdd();

    public function clear();
}