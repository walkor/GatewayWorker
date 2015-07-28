GatewayWorker
=================

GatewayWorker基于[Workerman](https://github.com/walkor/Workerman)开发的一个项目框架，用于快速开发长连接应用，例如app推送服务端、即时IM服务端、游戏服务端、物联网、智能家居等等。

GatewayWorker使用经典的Gateway和Worker进程模型。Gateway进程负责维持客户端连接，并转发客户端的数据给Worker进程处理；Worker进程负责处理实际的业务逻辑，并将结果推送给对应的客户端。Gateway服务和Worker服务可以分开部署在不同的服务器上，实现分布式集群。

GatewayWorker提供非常方便的API，可以全局广播数据、可以向某个群体广播数据、也可以向某个特定客户端推送数据。配合Workerman的定时器，也可以定时推送数据。

启动
=======
以debug方式启动

```php start.php start```

以daemon方式启动

```php start.php start -d```

停止

```php start.php stop```

平滑重启

```php start.php reload```

查看运行状态

```php start.php status```

Applications\YourApp测试方法
======
使用telnet命令测试（不要使用windows再带的telnet）
```shell
 telnet 127.0.0.1 8282
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
Hello 3
3 login
haha
3 said haha
```

手册
=======
http://www.workerman.net/gatewaydoc/

使用GatewayWorker开发的项目
=======
## [tadpole](http://kedou.workerman.net/)  
[Live demo](http://kedou.workerman.net/)  
[Source code](https://github.com/walkor/workerman)  
![workerman todpole](http://www.workerman.net/img/workerman-todpole.png)   

## [chat room](http://chat.workerman.net/)  
[Live demo](http://chat.workerman.net/)  
[Source code](https://github.com/walkor/workerman-chat)  
![workerman-chat](http://www.workerman.net/img/workerman-chat.png)  

## [web-msg-sender](https://github.com/walkor/web-msg-sender)  
[Live demo send page](http://workerman.net:3333/)  
[Live demo receive page](http://workerman.net/web-msg-sender.html)  
[Source code](https://github.com/walkor/web-msg-sender)  
![web-msg-sender](http://www.workerman.net/img/web-msg-sender.png)   
