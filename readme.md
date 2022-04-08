todo
    使用redis协议 伪装成redis服务端 方便接入
    多进程模式 - 分布式 主【主、从】模式
    事务时数据落地使用批量与预处理效率测试
    http reset操作支持 web管理 

//发送消息
send([
    'topic'    => 'cmd', //消息主题 必填 不允许空格,
    'data'     => $data  //消息内容 必填
    'seq_id'   => 0, //顺序id 通过此值来分配到同一队列 用于顺序处理
    'delay'    => 0, //延时秒数
    'retry' => 0, //重试次数
    'ack' => 0, //是否应答 不应答 数据出列状态设为1 应答需要消费端回应状态
    'sync' => 0, //是否同步
    'to' => '',
]);

NNNCC to topic data
12+2

ack,sync开关共用一个字节  1:ack 2:sync 4 8

//'timeout' => 0, //消息处理超时时间 0不限制 有指定retry时未设置此值  使用全局默认值 超时就放入重试队列 

udp发送时包长度限制为8k

id:1, topic:1, data:4, delay:4, retry:1, retry_step:2

1+1+4+4+1+2: 13

DATA_WRITE_ASYNC 默认为异步 设置为0会同步落盘数据  相同步异步都支持 可开启两个服务来实现

server 实时记录投递数量 [定时每秒落盘, 当前缓存数量>10【可配置】 落盘] 最后记录id  同步追加到Redis缓存  实时记录缓存数量  缓存数量>xxx 时启用定时每秒任务 判断是否消耗记录缓存数量降低 记录最后消耗记录id 通过最后记录id继续缓存 



pop: queueName, id, ack, retry, data 有ack表示要应答

push:topic, data, seq_id, delay, retry, ack, sync  有sync要同步返回落地结果

