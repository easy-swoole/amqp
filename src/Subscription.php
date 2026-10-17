<?php
declare(strict_types=1);

namespace EasySwoole\AMQP;

final class Subscription
{
    private bool $active = true;
    public function __construct(
        private ?Channel $channel,
        public readonly string $consumerTag,
        private ?\Closure $handler,
        public readonly bool $autoAck,
    ) {}

    public function cancel(): void
    {
        if ($this->active) $this->channel?->cancel($this->consumerTag);
    }
    public function isActive(): bool { return $this->active; }
    public function dispatch(Message $message): mixed
    {
        if (!$this->active || $this->handler === null || $this->channel === null) throw new \LogicException('Subscription is inactive.');
        return ($this->handler)($message, $this->channel, $this);
    }
    public function markCancelled(): void
    {
        $this->active = false;
        $this->handler = null;
        $this->channel = null;
    }
}
