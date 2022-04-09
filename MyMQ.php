#!/usr/bin/env php
<?php
//declare(strict_types=1);
error_reporting(E_ALL);
ini_set('display_errors', 'On');// 有些环境关闭了错误显示
if (!defined('VENDOR_DIR')) {
    if (is_dir(__DIR__ . '/vendor')) {
        define('VENDOR_DIR', __DIR__ . '/vendor');
    } elseif (is_dir(__DIR__ . '/../vendor')) {
        define('VENDOR_DIR', __DIR__ . '/../vendor');
    } elseif (is_dir(__DIR__ . '/../../../vendor')) {
        define('VENDOR_DIR', __DIR__ . '/../../../vendor');
    }
}
defined('MY_PHP_DIR') || define('MY_PHP_DIR', VENDOR_DIR . '/myphps/myphp');
//defined('MY_PHP_SRV_DIR') || define('MY_PHP_SRV_DIR', VENDOR_DIR . '/myphps/my-php-srv');

defined('CONF_FILE') || define('CONF_FILE', __DIR__ . '/conf.php');
defined('MQ_NAME') || define('MQ_NAME', 'MyMQ');
defined('MQ_LISTEN') || define('MQ_LISTEN', '0.0.0.0');
defined('MQ_PORT') || define('MQ_PORT', 55011);
defined('IS_SWOOLE') || define('IS_SWOOLE', 0);

require VENDOR_DIR . '/autoload.php';
require MY_PHP_DIR . '/GetOpt.php';
if (defined('MY_PHP_SRV_DIR')) require MY_PHP_SRV_DIR . '/Load.php';

//解析命令参数
GetOpt::parse('sp:l:', ['help', 'swoole', 'port:', 'listen:']);
//处理命令参数
$isSwoole = GetOpt::has('s', 'swoole') || IS_SWOOLE;
$port = (int)GetOpt::val('p', 'port', MQ_PORT);
$listen = GetOpt::val('l', 'listen', MQ_LISTEN);
//自动检测
if (!$isSwoole && !SrvBase::workermanCheck() && defined('SWOOLE_VERSION')) {
    $isSwoole = true;
}

if (GetOpt::has('h', 'help')) {
    echo 'Usage: php MyMQ.php OPTION [restart|reload|stop]
   or: MyMQ.php OPTION [restart|reload|stop]

   --help
   -l --listen    监听地址 默认 0.0.0.0
   -p --port      tcp|udp 端口
   -s --swoole    swolle运行', PHP_EOL;
    exit(0);
}
if(!is_file(CONF_FILE)){
    echo CONF_FILE.' file does not exist';
    exit(0);
}
// $_SERVER['SCRIPT_FILENAME'] = __FILE__; //重置运行 不设置此项使用相对路径运行时 会加载了不相应的引入文件

$conf = [
    'name' => MQ_NAME, //服务名
    'ip' => $listen,
    'port' => $port,
    'type' => 'tcp', //类型[tcp udp]
    'setting' => [ //swooleSrv有兼容处理
        'protocol' => 'MQPackN2',
        'stdoutFile' => __DIR__ . '/log.log', //终端输出
        'pidFile' => __DIR__ . '/mq.pid',  //pid_file
        'logFile' => __DIR__ . '/log.log', //日志文件 log_file
        'log_level' => 0,
        //swoole
        /*'open_eof_check' => true, //打开EOF检测
        'package_eof' => "\n", //设置EOF
        */
        'open_length_check' => true,
        'package_length_func' => function ($buffer) { //自定义解析长度
            $pos = strpos($buffer, "\n");
            if ($pos === false) {
                return 0;
            }
            return $pos + strlen("\n");
        }
    ],
    'event' => [
        'onWorkerStart' => function ($worker, $worker_id) {
            MQServer::onWorkerStart($worker, $worker_id);
        },
        'onWorkerStop' => function ($worker, $worker_id) {
            MQServer::onWorkerStop($worker, $worker_id);
        },
        'onConnect' => function ($con, $fd = 0) use ($isSwoole) {
            if (!$isSwoole) {
                $fd = $con->id;
            }
            MQLib::auth($con, $fd);
        },
        'onClose' => function ($con, $fd = 0) {
            MQLib::auth($con, $fd, null);
        },
        'onReceive' => function (swoole_server $server, int $fd, int $reactor_id, string $data) { //swoole tcp
            $data = MQPackN2::decode($data);
            $ret = MQServer::onReceive($server, $data, $fd);
            $server->send($fd, MQPackN2::encode($ret !== false ? $ret : MQServer::err()));
        },
        'onPacket' => function (swoole_server $server, $data, $client_info) { //swoole tcp
            $data = MQPackN2::decode($data);
            $ret = MQServer::onReceive($server, $data, $client_info);
            $server->sendto($client_info['address'], $client_info['port'], MQPackN2::encode($ret !== false ? $ret : MQServer::err()));
            SrvBase::$instance->send($client_info, MQPackN2::encode($ret !== false ? $ret : MQServer::err()));
        },
        'onMessage' => function (\Workerman\Connection\ConnectionInterface $connection, $data) { //workerman
            $fd = $connection->id;
            $ret = MQServer::onReceive($connection, $data, $fd);
            $connection->send($ret !== false ? $ret : MQServer::err());
        },
    ],
    // 进程内加载的文件
    'worker_load' => [
        CONF_FILE,
        MY_PHP_DIR . '/base.php',
    ],
];

if ($isSwoole) {
    $srv = new SwooleSrv($conf);
} else {
    $srv = new WorkerManSrv($conf);
}

$srv->run($argv);