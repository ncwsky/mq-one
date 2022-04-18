<?php

interface DelayInterface
{
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

    public function clear();
}