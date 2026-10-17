<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Internal;

use EasySwoole\AMQP\Exception\ProtocolException;
use EasySwoole\AMQP\Wire\Reader;

final class PublisherConfirmTracker
{
    private bool $enabled = false;
    private int $nextSequence = 1;
    /** @var array<int, true> */
    private array $pending = [];

    public function enabled(): bool { return $this->enabled; }
    public function enable(): void { $this->enabled = true; $this->nextSequence = 1; $this->pending = []; }
    public function reset(): void { $this->enabled = false; $this->nextSequence = 1; $this->pending = []; }
    public function published(): int
    {
        if (!$this->enabled) return 0;
        $sequence = $this->nextSequence++;
        $this->pending[$sequence] = true;
        return $sequence;
    }
    public function hasPending(): bool { return $this->pending !== []; }
    public function highestPending(): int { return $this->pending === [] ? 0 : max(array_keys($this->pending)); }
    public function hasPendingThrough(int $target): bool
    {
        foreach ($this->pending as $sequence => $_) if ($sequence <= $target) return true;
        return false;
    }
    public function apply(Reader $reader): void
    {
        $deliveryTag = $reader->longlong();
        $multiple = $reader->bit();
        if ($deliveryTag > 0 && $deliveryTag >= $this->nextSequence) {
            throw new ProtocolException("Publisher confirmation references unknown delivery tag {$deliveryTag}.");
        }
        $matched = false;
        foreach (array_keys($this->pending) as $sequence) {
            if (($multiple && ($deliveryTag === 0 || $sequence <= $deliveryTag)) || (!$multiple && $sequence === $deliveryTag)) {
                unset($this->pending[$sequence]);
                $matched = true;
            }
        }
        if (!$matched) throw new ProtocolException("Publisher confirmation references settled delivery tag {$deliveryTag}.");
    }
}
