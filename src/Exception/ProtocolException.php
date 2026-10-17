<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Exception;

class ProtocolException extends AMQPException
{
    public function __construct(string $message, public readonly int $replyCode = 0, public readonly int $classId = 0, public readonly int $methodId = 0)
    {
        parent::__construct($message, $replyCode);
    }
}
