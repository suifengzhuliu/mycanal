canal是阿里巴巴开源的mysql binlog日志解析工具，目前使用的是基于1.0.24版本。
我改了其中的部分代码，对解析出来的数据，添加数据库的host、port等信息。

自己写了消费的客户端代码