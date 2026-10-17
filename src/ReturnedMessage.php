<?php
declare(strict_types=1);

namespace EasySwoole\AMQP;

final class ReturnedMessage
{
    public function __construct(
        public readonly int $replyCode,
        public readonly string $replyText,
        public readonly string $exchange,
        public readonly string $routingKey,
        public readonly Message $message,
    ) {}
}
