消息队列服务 php+(mysql|sqlite)+redis 默认使用的Sqlite  
sqlite.db sqlite数据库模板适合小量使用  
mysql.sql mysql数据库初始sql

###安装
    
    mkdir mq
    cd mq
    
    composer require myphps/mq-one
    
    cp vendor/myphps/mq-one/run.example.php run.php
    cp vendor/myphps/mq-one/conf.example.php conf.php
    cp vendor/myphps/mq-one/sqlite.db mq.db
    chmod +x run.php
    
    修改 run.php conf.php配置
    运行 ./run.php 
    
###TODO
>推模式    
>追加模拟redis协议 lpop、rpush、llen|lsize、lrange?、subscribe、publish    