<?php

interface RetryInterface
{
    public function getCount();
    /**
     * 定时重试入列数据
     * @return int 入列数
     */
    public function tick();

    /**
     * 进程结束存储重试数据
     * @return void
     */
    public function stop2save();

    public function beforeAdd();

    /**
     * 添加重试数据
     * @param $id
     * @param $time
     * @param $data
     * @return mixed
     */
    public function add($id, $time, $data);

    public function afterAdd();

    /**
     * 清除重试
     * @param $id
     * @param bool $retry 是否重试清除
     */
    public function clean($id, $retry = false);

    /**
     * 清除缓存
     */
    public function clear();

}