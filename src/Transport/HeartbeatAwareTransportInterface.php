<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Transport;

interface HeartbeatAwareTransportInterface
{
    public function setHeartbeatInterval(int $seconds): void;
}
