<?php 
/**
 * This file is part of workerman.
 *
 * Licensed under The MIT License
 * For full copyright and license information, please see the MIT-LICENSE.txt
 * Redistributions of files must retain the above copyright notice.
 *
 * @author walkor<walkor@workerman.net>
 * @copyright walkor<walkor@workerman.net>
 * @link http://www.workerman.net/
 * @license http://www.opensource.org/licenses/mit-license.php MIT License
 */
namespace GatewayWorker;

use Workerman\Connection\TcpConnection;

use \Workerman\Worker;
use \Workerman\Lib\Timer;
use \Workerman\Autoloader;
use \GatewayWorker\Protocols\GatewayProtocol;
use \GatewayWorker\Lib\Lock;
use \GatewayWorker\Lib\Store;

/**
 * 
 * 注册中心，用于注册Gateway和BusinessWorker 
 * 
 * @author walkor<walkor@workerman.net>
 *
 */
class Register extends Worker
{
    
    /**
     * 是否可以平滑重启，Register不平滑重启
     * @var bool
     */
    public $reloadable = false;
    
    protected $_gatewayConnections = array();
    protected $_workerConnections = array();   
    public function run()
    {
        $this->onConnect = array($this, 'onConnect');
        
        // onMessage禁止用户设置回调
        $this->onMessage = array($this, 'onMessage');
        
        // 保存用户的回调，当对应的事件发生时触发
        $this->onClose = array($this, 'onClose');
        
        // 记录进程启动的时间
        $this->_startTime = time();
        // 运行父方法
        parent::run();
    }

    public function onConnect($connection)
    {
         $connection->timeout_timerid = Timer::add(10, function()use($connection){
             echo "timeout\n";
             $connection->close();
         }, null, false);
    }

    public function onMessage($connection, $data)
    {
        Timer::del($connection->timeout_timerid);
        $data = json_decode($data, true);
        $event = $data['event'];
        switch($event)
        {
            case 'gateway_connect':
                if(empty($data['address']))
                {
                    echo "address not found\n";
                    return $connection->close();
                }
                $this->_gatewayConnections[$connection->id] = $data['address'];
                $this->broadcastAddresses();
                break;
           case 'worker_connect':
                $this->_workerConnections[$connection->id] = $connection;
                $this->broadcastAddresses($connection);
                break;
           default:
                echo "unknown event\n";
                $connection->close();
        }
    }

    public function onClose($connection)
    {
        if(isset($this->_gatewayConnections[$connection->id]))
        {
            unset($this->_gatewayConnections[$connection->id]);
            $this->broadcastAddresses();
        }
        if(isset($this->_workerConnections[$connection->id]))
        {
            unset($this->_workerConnections[$connection->id]);
        }
    }

    public function broadcastAddresses($connection = null)
    {
        $data = array(
            'event' => 'broadcast_addresses',
            'addresses' => array_unique(array_values($this->_gatewayConnections)),
        );
        $buffer = json_encode($data);
        if($connection)
        {
            return $connection->send($buffer);
        }
        foreach($this->_workerConnections as $con)
        {
            $con->send($buffer);
        }
    }
}



















