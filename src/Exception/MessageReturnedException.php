<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Exception;

use EasySwoole\AMQP\ReturnedMessage;

final class MessageReturnedException extends AMQPException
{
    public function __construct(public readonly ReturnedMessage $returnedMessage)
    {
        parent::__construct(
            "Message was returned by RabbitMQ: {$returnedMessage->replyCode} {$returnedMessage->replyText}",
            $returnedMessage->replyCode,
        );
    }
}
