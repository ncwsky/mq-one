#!/usr/bin/env php
<?php
define('VENDOR_DIR', __DIR__ . '/vendor');
//define('MY_PHP_DIR', __DIR__ . '/vendor/myphps/myphp');
//define('MY_PHP_SRV_DIR', __DIR__ . '/vendor/myphps/my-php-srv');
define('CONF_FILE', __DIR__ . '/conf.php');
define('MQ_NAME', 'MyMQ');
define('MQ_LISTEN', '0.0.0.0');
define('MQ_PORT', 55011);
define('IS_SWOOLE', 0);

require __DIR__. '/MyMQ.php';