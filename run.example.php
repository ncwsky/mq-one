#!/usr/bin/env php
<?php
define('RUN_DIR', __DIR__);
define('VENDOR_DIR', RUN_DIR . '/vendor');
//define('MY_PHP_DIR', RUN_DIR . '/vendor/myphps/myphp');
//define('MY_PHP_SRV_DIR', RUN_DIR . '/vendor/myphps/my-php-srv');
define('CONF_FILE', RUN_DIR . '/conf.php');
define('MQ_NAME', 'MyMQ');
define('MQ_LISTEN', '0.0.0.0');
define('MQ_PORT', 55011);
define('IS_SWOOLE', 0);

require RUN_DIR. '/vendor/myphps/mq-one/MyMQ.php';