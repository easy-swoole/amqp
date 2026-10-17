<?php
declare(strict_types=1);

namespace EasySwoole\AMQP;

final class PublisherBatch
{
    /** @var list<array{body:string,exchange:string,routingKey:string,properties:array<string,mixed>,mandatory:bool}> */
    private array $messages = [];
    private bool $completed = false;

    /** @param \Closure(array, float):PublisherConfirmResult $waiter */
    public function __construct(private ?\Closure $waiter) {}

    /** @param array<string, mixed> $properties */
    public function publish(string $body, string $exchange = '', string $routingKey = '', array $properties = [], bool $mandatory = false): self
    {
        if ($this->completed) throw new \LogicException('Publisher batch is already completed.');
        $this->messages[] = compact('body', 'exchange', 'routingKey', 'properties', 'mandatory');
        return $this;
    }

    public function wait(float $timeout = 3.0): PublisherConfirmResult
    {
        if ($this->completed) throw new \LogicException('Publisher batch is already completed.');
        if ($this->messages === []) throw new \LogicException('Cannot confirm an empty publisher batch.');
        $this->completed = true;
        $messages = $this->messages;
        $waiter = $this->waiter;
        $this->messages = [];
        $this->waiter = null;
        if ($waiter === null) throw new \LogicException('Publisher batch has no active waiter.');
        return $waiter($messages, $timeout);
    }
}
