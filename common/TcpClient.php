<?php
/**
 * tcp客户端
 */
class TcpClient
{
    /**
     * Read buffer size.
     *
     * @var int
     */
    const READ_BUFFER_SIZE = 65535;

    /**
     * 发送数据和接收数据的超时时间  单位S
     * @var int
     */
    public $timeout = 5;
    public $async = false;
    public $isConnect = false;
    /**
     * Heartbeat interval.
     * @var int
     */
    public $pingInterval = 25;

    /**
     * 类型
     * @var string
     */
    public $type = 'tcp';

    /**
     * 异步调用发送数据前缀
     * @var string
     */
    const ASYNC_SEND_PREFIX = 'asend_';

    /**
     * 异步调用接收数据
     * @var string
     */
    const ASYNC_RECV_PREFIX = 'arecv_';

    /**
     * 异步调用实例
     * @var string
     */
    protected static $asyncInstances = array();

    /**
     * 同步调用实例
     * @var self[]
     */
    protected static $instances = array();

    public static $logFile = '';
    /**
     * 自定义日志
     * @var callable
     */
    public static $onLog = null;

    /**
     * 服务端地址
     * @var array
     */
    protected $addressArray = array();

    /**
     * 到服务端的socket连接
     * @var resource
     */
    public $socket = null;

    /**
     * 实例的服务名
     * @var string
     */
    protected $serviceName = '';

    /**
     * Receive buffer.
     *
     * @var string
     */
    protected $recvBuffer = '';

    /**
     * Current package length.
     *
     * @var int
     */
    protected $currentPackageLength = 0;

    /**
     * Sets the maximum acceptable packet size for the current connection.
     *
     * @var int
     */
    public $maxPackageSize = 1048576;

    /**
     * 包EOF检测
     * @var bool
     */
    public $openEofCheck = true; //使用EOF检测包
    public $packageEof = "\n"; //设置EOF
    /**
     * 包长检测
     * @var string
     */
    public $packageLenType = 'n'; //包长类型
    public $isTotalPackageLen = true; //包长含了整个包（包头+包体）
    public $packageLenOffset = 0; //包长计算偏移

    /**
     * 自定义包长获取
     * @var callable
     */
    public $onInput = null;
    /**
     * 自定义封包
     * @var callable
     */
    public $onEncode = null;
    /**
     * 自定义解包
     * @var callable
     */
    public $onDecode = null;
    /**
     * 自定义连接事件
     * @var callable
     */
    public $onConnect = null;

    public $onClose = null;

    //支付以下长度值
    public static $packageLenTypeSizeMap = [
        'c'=>1, //有符号、1字节
        'C'=>1, //无符号、1字节
        's'=>2, //有符号、主机字节序、2字节
        'S'=>2, //无符号、主机字节序、2字节
        'n'=>2, //无符号、网络字节序、2字节
        'N'=>4, //无符号、网络字节序、4字节
        'l'=>4, //有符号、主机字节序、4字节（小写L）
        'L'=>4, //无符号、主机字节序、4字节（大写L）
        'v'=>2, //无符号、小端字节序、2字节
        'V'=>4, //无符号、小端字节序、4字节
    ];

    /**
     * 获取一个实例
     * @param string $service_name
     * @param array|string $address_array
     * @return self
     */
    public static function instance($service_name = '_default', $address_array = null)
    {
        if (!isset(self::$instances[$service_name])) {
            self::$instances[$service_name] = new self($service_name);
        }
        if ($address_array) self::$instances[$service_name]->config($address_array);
        return self::$instances[$service_name];
    }

    public static function log($msg)
    {
        if (static::$onLog) {
            call_user_func(static::$onLog, $msg);
            return;
        }
        if (static::$logFile === '') {
            static::$logFile = __DIR__ . '/../tcp-client.log';
        }

        if (is_file(static::$logFile) && 4194304 <= filesize(static::$logFile)) { // 4M
            file_put_contents(static::$logFile, '', LOCK_EX);
            clearstatcache(true, static::$logFile);
        }

        file_put_contents(static::$logFile, \date('Y-m-d H:i:s') . ' ' . $msg . "\n", FILE_APPEND | LOCK_EX);
    }

    /**
     * 设置/获取服务端地址
     * @param array|string $address_array
     * @return array|void
     */
    public function config($address_array = null)
    {
        if (!empty($address_array)) {
            $this->addressArray = (array)$address_array;
        }
        return $this->addressArray;
    }

    /**
     * EOF
     * @param string $buffer
     * @return int
     */
    public function inputEof($buffer)
    {
        if (isset($this->maxPackageSize) && strlen($buffer) >= $this->maxPackageSize) {
            $this->close();
            return 0;
        }
        $pos = strpos($buffer, $this->packageEof);
        if ($pos === false) {
            return 0;
        }
        return $pos + strlen($this->packageEof);
    }

    /**
     * 获取包长
     * @param $recv_buffer
     * @return int
     */
    public function input($buffer)
    {
        if ($this->onInput) return call_user_func($this->onInput, $buffer);

        if ($this->openEofCheck) return $this->inputEof($buffer);

        $packageLenTypeSize = static::$packageLenTypeSizeMap[$this->packageLenType];
        $len = strlen($buffer);
        if ($len === 0 || $len < ($this->packageLenOffset + $packageLenTypeSize)) return 0;

        $data = unpack($this->packageLenType, substr($buffer, $this->packageLenOffset, $packageLenTypeSize));
        return $this->packageLenOffset + $data[1] + ($this->isTotalPackageLen ? 0 : $packageLenTypeSize);
    }

    /**
     * EOF
     * @param string $buffer
     * @return string
     */
    public function encodeEof($buffer)
    {
        return $buffer . $this->packageEof;
    }

    /**
     * 封包 无符号短整型 大端字节序 包头+包体 length的值不包含包头
     * @param $buffer
     * @return string
     */
    public function encode($buffer)
    {
        if ($this->onEncode) return call_user_func($this->onEncode, $buffer);

        if (!is_scalar($buffer)) $buffer = json_encode($buffer, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

        if ($this->openEofCheck) return $this->encodeEof($buffer);

        $packageLenTypeSize = static::$packageLenTypeSizeMap[$this->packageLenType];
        return ($this->packageLenOffset ? str_repeat("\0", $this->packageLenOffset) : '') . pack($this->packageLenType, ($this->isTotalPackageLen ? $packageLenTypeSize : 0) + strlen($buffer)) . $buffer;
    }

    /**
     * EOF
     * @param string $buffer
     * @return string
     */
    public function decodeEof($buffer)
    {
        return rtrim($buffer, $this->packageEof);
    }

    /**
     * 解包
     * @param $buffer
     * @return false|string
     */
    public function decode($buffer)
    {
        if ($this->onDecode) return call_user_func($this->onDecode, $buffer);

        if ($this->openEofCheck) return $this->decodeEof($buffer);

        $packageLenTypeSize = static::$packageLenTypeSizeMap[$this->packageLenType];
        //$recv = unpack($this->packageLenType, substr($buffer, $this->packageLenOffset, $packageLenTypeSize));
        return substr($buffer, $this->packageLenOffset + $packageLenTypeSize);
    }

    /**
     * TcpClient constructor.
     * @param string $service_name
     */
    public function __construct($service_name='_default')
    {
        $this->serviceName = $service_name;
    }

    /**
     * 打开到服务端的连接
     * @throws \Exception
     */
    protected function open()
    {
        if ($this->socket) { // && is_resource($this->socket) && !feof($this->socket)
            return;
        }

        $address = $this->type . '://' . $this->addressArray[array_rand($this->addressArray)];
        set_error_handler(function(){});
        if ($this->async) {
            $this->socket = stream_socket_client($address, $err_no, $err_msg, 0, STREAM_CLIENT_ASYNC_CONNECT);
        } else {
            $this->socket = stream_socket_client($address, $err_no, $err_msg);
        }
        restore_error_handler();
        if (!$this->socket || !is_resource($this->socket)) {
            $this->toOnClose();
            throw new \Exception("can not connect to $address , $err_no:$err_msg");
        }

        if ($this->async) {
            $read = [];
            $write  = [$this->socket];
            $except = [];
            // For windows.
            if(\DIRECTORY_SEPARATOR === '\\') {
                $except  = [$this->socket];
            }
            try {
                $ret = @stream_select($read, $write, $except, 0, 100000);
            } catch (\Exception $e) {} catch (\Error $e) {}

            if ($ret && $this->type=='tcp') {
                set_error_handler(function(){});
                //打开tcp的keepalive并禁用Nagle算法
                $raw_socket = socket_import_stream($this->socket);
                socket_set_option($raw_socket, SOL_SOCKET, SO_KEEPALIVE, 1);
                socket_set_option($raw_socket, SOL_TCP, TCP_NODELAY, 1);
                restore_error_handler();
            } else {
                $this->toOnClose();
                throw new \Exception("can not connect to $address");
            }
            stream_set_blocking($this->socket, false);
        }

        if ($this->timeout) {
            stream_set_timeout($this->socket, $this->timeout);
        }
/*
        //服务端在同一机器不用心跳
        if (strpos($address, '127.0.0.1')===false && class_exists('\Workerman\Lib\Timer') && PHP_SAPI === 'cli') {
            //todo 测试定时器
            $timer_id = \Workerman\Lib\Timer::add($this->pingInterval, function ($socket) use (&$timer_id) {
                $buffer = $this->encode('ping');
                if (strlen($buffer) !== @fwrite($socket, $buffer)) {
                    @fclose($socket);
                    \Workerman\Lib\Timer::del($timer_id);
                }
            }, array($this->socket));
        }*/
        $this->isConnect = true;

        if ($this->onConnect) {
            try {
                call_user_func($this->onConnect, $this);
            } catch (\Exception $e) {
                static::log($e);
            } catch (\Error $e) {
                static::log($e);
            }
        }
    }

    /**
     * 关闭到服务端的连接
     * @return void
     */
    protected function close()
    {
        $this->toOnClose();
        if ($this->socket) {
            fclose($this->socket);
            $this->socket = null;
            $this->currentPackageLength = 0;
            $this->recvBuffer = '';
            $this->isConnect = false;
        }
    }
    protected function toOnClose(){
        if ($this->onClose) {
            try {
                \call_user_func($this->onClose, $this);
            } catch (\Exception $e) {
                static::log($e);
            } catch (\Error $e) {
                static::log($e);
            }
        }
    }

    /**
     * @param string $read_buffer
     * @throws \Exception
     */
    protected function baseRead(&$read_buffer)
    {
        $read_buffer = '';
        $time_start = microtime(true);
        while (1) {
            // 超时了
            if ($this->timeout>0 && (microtime(true) - $time_start) > $this->timeout) {
                //$this->close();
                throw new \Exception('recv timeout '.date("Y-m-d H:i:s"));
            }

            try {
                $buffer = @fread($this->socket, self::READ_BUFFER_SIZE);
            } catch (\Exception $e) {} catch (\Error $e) {}

            if ($buffer === '' || $buffer === false) {
                if ($buffer === false || !is_resource($this->socket) || feof($this->socket)) {
                    $this->close();
                    return;
                }
                usleep(200000); //0.2
                continue;
            } else {
                $this->recvBuffer .= $buffer;
            }

            if ($this->currentPackageLength == 0) {
                try {
                    $this->currentPackageLength = $this->input($this->recvBuffer);
                } catch (\Exception $e) {} catch (\Error $e) {}
            }

            if ($this->currentPackageLength > 0) {
                $len = strlen($this->recvBuffer);
                if ($this->currentPackageLength > $len) continue;

                if ($this->currentPackageLength <= $len) {
                    if ($len === $this->currentPackageLength) {
                        $read_buffer = $this->recvBuffer;
                        $this->recvBuffer = '';
                    } else {
                        $read_buffer = substr($this->recvBuffer, 0, $this->currentPackageLength);
                        $this->recvBuffer = substr($this->recvBuffer, $this->currentPackageLength);
                    }
                    $this->currentPackageLength = 0;
                    // 接收完毕
                    break;
                }
            }
        }
    }

    /**
     * 发送数据给服务端
     * @param string $data
     * @param bool $raw
     * @return bool
     * @throws \Exception
     */
    public function send($data, $raw = false)
    {
        try {
            $this->open();
            if (!$this->isConnect) return false;
            if (!$raw) $data = $this->encode($data);

            if ($this->type == 'udp') {
                return \strlen($data) === \stream_socket_sendto($this->socket, $data);
            }

            //set_error_handler(function(){});
            $written = @fwrite($this->socket, $data);
            //restore_error_handler();

            if ($written === false) {
                throw new \Exception("Failed to write to socket.");
            }
            if ($written !== ($len = strlen($data))) {
                throw new \Exception("Failed to write to socket. $written of $len bytes written.");
            }
        } catch (\Exception $e) {
            if (!is_resource($this->socket) || feof($this->socket)) {
                //$this->close();
            }
            $this->close();
            static::log($e);
            return false;
        } catch (\Error $e) {
            if (!is_resource($this->socket) || feof($this->socket)) {
                //$this->close();
            }
            $this->close();
            static::log($e);
            return false;
        }
        return true;
    }

    /**
     * 从服务端接收数据
     * @param bool $raw
     * @return false|string
     * @throws \Exception
     */
    public function recv($raw = false)
    {
        $this->open();
        if (!$this->isConnect) return false;
        $this->baseRead($read_buffer);

        if ($read_buffer === '') {
            //return false;
            throw new \Exception("recv empty ".date("Y-m-d H:i:s"));
        }

        try {
            return $raw ? $read_buffer : $this->decode($read_buffer);
        } catch (\Exception $e) {
            static::log($e);
            return false;
        } catch (\Error $e) {
            static::log($e);
            return false;
        }
    }

    /**
     * @param string $method
     * @param array $params
     * @return string
     */
    protected function jsonRpc($method, $params)
    {
        return json_encode([
            'class' => $this->serviceName,
            'method' => $method,
            'params' => $params,
        ]);
    }

    /**
     * 调用
     * @param $method
     * @param array $arguments
     * @return mixed
     * @throws \Exception
     */
    public function __call($method, $arguments)
    {
        // 判断是否是异步发送
        if (0 === strpos($method, self::ASYNC_SEND_PREFIX)) {
            $real_method = substr($method, strlen(self::ASYNC_SEND_PREFIX));
            $instance_key = $real_method . serialize($arguments);
            if (isset(self::$asyncInstances[$instance_key])) {
                throw new \Exception($this->serviceName . "->$method(" . implode(',', $arguments) . ") have already been called");
            }
            self::$asyncInstances[$instance_key] = new self($this->serviceName);
            return self::$asyncInstances[$instance_key]->send($this->jsonRpc($real_method, $arguments));
        }
        // 如果是异步接受数据
        if (0 === strpos($method, self::ASYNC_RECV_PREFIX)) {
            $real_method = substr($method, strlen(self::ASYNC_RECV_PREFIX));
            $instance_key = $real_method . serialize($arguments);
            if (!isset(self::$asyncInstances[$instance_key])) {
                throw new \Exception($this->serviceName . "->asend_$real_method(" . implode(',', $arguments) . ") have not been called");
            }
            $tmp = self::$asyncInstances[$instance_key];
            unset(self::$asyncInstances[$instance_key]);
            return $tmp->recv();
        }
        // 同步发送接收
        $this->send($this->jsonRpc($method, $arguments));
        return $this->recv();
    }
}

/*
// ==以下调用示例==
if (PHP_SAPI == 'cli' && isset($argv[0]) && $argv[0] == basename(__FILE__)) {
    // 服务端列表
    $address_array = array(
        'tcp://127.0.0.1:2015',
        'tcp://127.0.0.1:2015'
    );
    // 配置服务端列表
    TcpClient::config($address_array);

    $uid = 567;
    $user_client = TcpClient::instance('User');
    // ==同步调用==
    $ret_sync = $user_client->getInfoByUid($uid);

    // ==异步调用==
    // 异步发送数据
    $user_client->asend_getInfoByUid($uid);
    $user_client->asend_getEmail($uid);

    //这里是其它的业务代码
    // todo .........................................

    // 异步接收数据
    $ret_async1 = $user_client->arecv_getEmail($uid);
    $ret_async2 = $user_client->arecv_getInfoByUid($uid);

    // 打印结果
    var_dump($ret_sync, $ret_async1, $ret_async2);
}
*/