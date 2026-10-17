<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Internal;

use EasySwoole\AMQP\Exception\ProtocolException;

/** Mutable protocol lifecycle state kept separate from the public Channel facade. */
final class ChannelState
{
    public bool $open = false;
    public bool $busy = false;
    public bool $transactional = false;
    public bool $legacyConsumerActive = false;
    public bool $subscriptionWaitActive = false;
    public ?ProtocolException $asynchronousCloseError = null;
    private ?int $callbackDeliveryTag = null;
    private bool $callbackSettled = false;

    public function beginDelivery(int $tag): void { $this->callbackDeliveryTag = $tag; $this->callbackSettled = false; }
    public function endDelivery(): void { $this->callbackDeliveryTag = null; $this->callbackSettled = false; }
    public function callbackSettled(): bool { return $this->callbackSettled; }
    public function markSettled(int $tag, bool $multiple): void
    {
        if ($this->callbackDeliveryTag === null) return;
        if ($multiple ? ($tag === 0 || $this->callbackDeliveryTag <= $tag) : $this->callbackDeliveryTag === $tag) $this->callbackSettled = true;
    }
    public function reset(): void
    {
        $this->open = false;
        $this->busy = false;
        $this->transactional = false;
        $this->legacyConsumerActive = false;
        $this->subscriptionWaitActive = false;
        $this->asynchronousCloseError = null;
        $this->endDelivery();
    }
}
