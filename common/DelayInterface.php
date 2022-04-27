<?php

interface DelayInterface
{
    /**
     * 当前实际待处理延迟数
     * @return mixed
     */
    public function waitingCount();
    /**
     * 定时延时入列数据
     * @return int 入列数
     */
    public function tick();

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

    /**
     * 清除所有
     * @return void
     */
    public function clear();
}