<?php
/**
 * @param string $name
 * @return lib_redis
 */
function redis($name = 'redis')
{
    lib_redis::$isExRedis = false;
    return lib_redis::getInstance(GetC($name));
}