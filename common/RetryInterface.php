<?php

interface RetryInterface
{
    public function getCount();
    /**
     * 定时重试入列数据
     * @return int 入列数
     */
    public function tick();

    public function beforeAdd();

    /**
     * 添加重试数据
     * @param $id
     * @param $time
     * @param $data
     * @param null $retry_step
     * @return mixed
     */
    public function add($id, $time, $data, $retry_step);

    public function afterAdd();

    /**
     * 获取重试id列表
     * @return array [id, ...]
     */
    public function getIdList();

    /**
     * 获取重试的数据
     * @param $id
     * @return string
     */
    public function getData($id);

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