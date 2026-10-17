<?php
declare(strict_types=1);

namespace EasySwoole\AMQP;

final class Message
{
    /** @param array<string, mixed> $properties */
    public function __construct(
        public readonly string $body,
        public readonly array $properties = [],
        public readonly int $deliveryTag = 0,
        public readonly bool $redelivered = false,
        public readonly string $exchange = '',
        public readonly string $routingKey = '',
        public readonly ?string $consumerTag = null,
        public readonly ?int $messageCount = null,
    ) {}
}
