#!/usr/bin/env php
<?php
define('RUN_DIR', __DIR__);
define('VENDOR_DIR', __DIR__ . '/vendor');
//define('MY_PHP_DIR', __DIR__ . '/vendor/myphps/myphp');
//define('MY_PHP_SRV_DIR', __DIR__ . '/vendor/myphps/my-php-srv');
define('MQ_NAME', 'MyMQ');
define('MQ_LISTEN', '0.0.0.0');
define('MQ_PORT', 55011);
define('IS_SWOOLE', 0);
//define('STOP_TIMEOUT', 10);

require __DIR__. '/vendor/myphps/mq-one/MyMQ.php';