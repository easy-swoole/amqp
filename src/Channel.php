<?php
declare(strict_types=1);

namespace EasySwoole\AMQP;

use EasySwoole\AMQP\Exception\MessageReturnedException;
use EasySwoole\AMQP\Exception\ProtocolException;
use EasySwoole\AMQP\Exception\TimeoutException;
use EasySwoole\AMQP\Internal\ChannelDispatcher;
use EasySwoole\AMQP\Internal\ContentReader;
use EasySwoole\AMQP\Internal\PublisherConfirmTracker;
use EasySwoole\AMQP\Wire\Frame;
use EasySwoole\AMQP\Wire\BasicProperties;
use EasySwoole\AMQP\Wire\Reader;
use EasySwoole\AMQP\Wire\Writer;

final class Channel
{
    private bool $open = false;
    private bool $busy = false;
    private ?int $callbackDeliveryTag = null;
    private bool $callbackSettled = false;
    /** @var array<string, Subscription> */
    private array $subscriptions = [];
    private bool $legacyConsumerActive = false;
    private bool $subscriptionWaitActive = false;
    private bool $cancelRequested = false;
    /** @var null|callable(ReturnedMessage, Channel):void */
    private $returnHandler = null;
    private readonly ContentReader $contentReader;
    private readonly PublisherConfirmTracker $confirms;
    private readonly ChannelDispatcher $dispatcher;
    public function __construct(private readonly Connection $connection, public readonly int $id)
    {
        $this->contentReader = new ContentReader($connection, $id);
        $this->confirms = new PublisherConfirmTracker();
        $this->dispatcher = new ChannelDispatcher($connection, $id, $this->contentReader, $this->confirms);
    }
    public function open(): void
    {
        $this->operation(function (): void {
            $this->rpc(20, 10, (new Writer())->shortstr('')->bytes(), 20, 11); $this->open = true;
        }, false);
    }
    public function exchangeDeclare(string $exchange, string $type = 'direct', bool $passive = false, bool $durable = false, bool $autoDelete = false, bool $internal = false, array $arguments = []): void
    {
        $this->operation(function () use ($exchange, $type, $passive, $durable, $autoDelete, $internal, $arguments): void {
            $args = (new Writer())->short(0)->shortstr($exchange)->shortstr($type)->bit($passive)->bit($durable)->bit($autoDelete)->bit($internal)->bit(false)->table($arguments)->bytes();
            $this->rpc(40, 10, $args, 40, 11);
        });
    }
    /** @return array{queue:string,messageCount:int,consumerCount:int} */
    public function queueDeclare(string $queue = '', bool $passive = false, bool $durable = false, bool $exclusive = false, bool $autoDelete = false, array $arguments = []): array
    {
        return $this->operation(function () use ($queue, $passive, $durable, $exclusive, $autoDelete, $arguments): array {
            $args = (new Writer())->short(0)->shortstr($queue)->bit($passive)->bit($durable)->bit($exclusive)->bit($autoDelete)->bit(false)->table($arguments)->bytes();
            $reader = $this->rpc(50, 10, $args, 50, 11);
            return ['queue' => $reader->shortstr(), 'messageCount' => $reader->long(), 'consumerCount' => $reader->long()];
        });
    }
    public function queueBind(string $queue, string $exchange, string $routingKey = '', array $arguments = []): void
    {
        $this->operation(function () use ($queue, $exchange, $routingKey, $arguments): void {
            $args = (new Writer())->short(0)->shortstr($queue)->shortstr($exchange)->shortstr($routingKey)->bit(false)->table($arguments)->bytes();
            $this->rpc(50, 20, $args, 50, 21);
        });
    }
    public function exchangeBind(string $destination, string $source, string $routingKey = '', array $arguments = []): void
    {
        $this->operation(function () use ($destination, $source, $routingKey, $arguments): void {
            $args = (new Writer())->short(0)->shortstr($destination)->shortstr($source)->shortstr($routingKey)->bit(false)->table($arguments)->bytes();
            $this->rpc(Protocol::EXCHANGE, 30, $args, Protocol::EXCHANGE, 31);
        });
    }
    public function exchangeUnbind(string $destination, string $source, string $routingKey = '', array $arguments = []): void
    {
        $this->operation(function () use ($destination, $source, $routingKey, $arguments): void {
            $args = (new Writer())->short(0)->shortstr($destination)->shortstr($source)->shortstr($routingKey)->bit(false)->table($arguments)->bytes();
            $this->rpc(Protocol::EXCHANGE, 40, $args, Protocol::EXCHANGE, 51);
        });
    }
    public function queueUnbind(string $queue, string $exchange, string $routingKey = '', array $arguments = []): void
    {
        $this->operation(function () use ($queue, $exchange, $routingKey, $arguments): void {
            $args = (new Writer())->short(0)->shortstr($queue)->shortstr($exchange)->shortstr($routingKey)->table($arguments)->bytes();
            $this->rpc(Protocol::QUEUE, 50, $args, Protocol::QUEUE, 51);
        });
    }
    public function queuePurge(string $queue): int
    {
        return $this->operation(function () use ($queue): int {
            $args = (new Writer())->short(0)->shortstr($queue)->bit(false)->bytes();
            return $this->rpc(Protocol::QUEUE, 30, $args, Protocol::QUEUE, 31)->long();
        });
    }
    public function exchangeDelete(string $exchange, bool $ifUnused = false): void
    {
        $this->operation(function () use ($exchange, $ifUnused): void {
            $args = (new Writer())->short(0)->shortstr($exchange)->bit($ifUnused)->bit(false)->bytes();
            $this->rpc(40, 20, $args, 40, 21);
        });
    }
    /** @return int Number of messages removed with the queue. */
    public function queueDelete(string $queue, bool $ifUnused = false, bool $ifEmpty = false): int
    {
        return $this->operation(function () use ($queue, $ifUnused, $ifEmpty): int {
            $args = (new Writer())->short(0)->shortstr($queue)->bit($ifUnused)->bit($ifEmpty)->bit(false)->bytes();
            return $this->rpc(50, 40, $args, 50, 41)->long();
        });
    }
    public function qos(int $prefetchCount, int $prefetchSize = 0, bool $global = false): void
    {
        if ($prefetchCount < 0 || $prefetchCount > 65535 || $prefetchSize < 0 || $prefetchSize > 0xffffffff) throw new \InvalidArgumentException('Invalid QoS values.');
        $this->operation(fn () => $this->rpc(60, 10, (new Writer())->long($prefetchSize)->short($prefetchCount)->bit($global)->bytes(), 60, 11));
    }
    /** @param array<string, mixed> $properties */
    public function publish(string $body, string $exchange = '', string $routingKey = '', array $properties = [], bool $mandatory = false): void
    {
        $this->operation(fn (): int => $this->writePublish($body, $exchange, $routingKey, $properties, $mandatory));
    }
    /** @param null|callable(ReturnedMessage, Channel):void $handler */
    public function setReturnHandler(?callable $handler): void
    {
        $this->returnHandler = $handler;
        if ($handler !== null) $this->dispatchReturnedMessages();
    }
    public function popReturnedMessage(): ?ReturnedMessage { return $this->dispatcher->popReturn(); }
    public function enablePublisherConfirms(): void
    {
        $this->operation(function (): void {
            if ($this->confirms->enabled()) return;
            $this->rpc(Protocol::CONFIRM, 10, (new Writer())->bit(false)->bytes(), Protocol::CONFIRM, 11);
            $this->confirms->enable();
        });
    }
    /** Publish and wait until RabbitMQ confirms that it accepted the message. */
    public function publishConfirmed(string $body, string $exchange = '', string $routingKey = '', array $properties = [], bool $mandatory = false, float $timeout = 3.0): void
    {
        $returned = $this->operation(function () use ($body, $exchange, $routingKey, $properties, $mandatory): ?ReturnedMessage {
            if (!$this->confirms->enabled()) {
                $this->rpc(Protocol::CONFIRM, 10, (new Writer())->bit(false)->bytes(), Protocol::CONFIRM, 11);
                $this->confirms->enable();
            }
            // Plain publish() calls made after confirm.select also receive confirms. Drain them
            // before sending this message so an earlier return/ACK cannot be attributed to it.
            if ($this->confirms->hasPending()) {
                $this->dispatcher->waitForConfirms($this->confirms->highestPending(), false);
            }
            $sequence = $this->writePublish($body, $exchange, $routingKey, $properties, $mandatory);
            return $this->dispatcher->waitForConfirms($sequence, true);
        }, true, $timeout);
        if ($returned !== null) throw new MessageReturnedException($returned);
    }
    public function publisherBatch(): PublisherBatch
    {
        return new PublisherBatch(fn (array $messages, float $timeout): PublisherConfirmResult => $this->publishBatchMessages($messages, $timeout));
    }
    public function get(string $queue, bool $autoAck = false, float $timeout = 3.0): ?Message
    {
        return $this->operation(function () use ($queue, $autoAck): ?Message {
            $args = (new Writer())->short(0)->shortstr($queue)->bit($autoAck)->bytes();
            $this->connection->send(Frame::method($this->id, 60, 70, $args));
            [$class, $method, $reader] = $this->dispatcher->readMethod();
            if ($class !== 60) throw new ProtocolException('Unexpected basic.get response.');
            if ($method === 72) { $reader->shortstr(); return null; }
            if ($method !== 71) throw new ProtocolException('Expected basic.get-ok or basic.get-empty.');
            $tag = $reader->longlong(); $redelivered = $reader->bit(); $exchange = $reader->shortstr(); $routingKey = $reader->shortstr(); $count = $reader->long();
            return $this->contentReader->read($tag, $redelivered, $exchange, $routingKey, null, $count);
        }, true, $timeout);
    }
    public function consume(string $queue, callable $handler, string $consumerTag = '', bool $autoAck = false, bool $exclusive = false, array $arguments = []): string
    {
        if ($this->legacyConsumerActive || $this->subscriptions !== []) throw new \LogicException('This channel already has an active consumer API.');
        $this->legacyConsumerActive = true;
        try {
        $tag = $this->operation(function () use ($queue, $consumerTag, $autoAck, $exclusive, $arguments): string {
            $args = (new Writer())->short(0)->shortstr($queue)->shortstr($consumerTag)->bit(false)->bit($autoAck)->bit($exclusive)->bit(false)->table($arguments)->bytes();
            return $this->rpc(60, 20, $args, 60, 21)->shortstr();
        });
        // 每次只让 I/O worker 读取一条投递，回调仍在调用方协程执行，允许安全 ack/nack。
        while ($this->open) {
            try {
                $message = $this->operation(function () use ($tag): ?Message {
                    $event = $this->dispatcher->nextConsumerEvent();
                    $eventTag = $event['type'] === 'delivery' ? $event['value']->consumerTag : $event['value'];
                    if ($eventTag !== $tag) {
                        $this->connection->abort();
                        throw new ProtocolException("Consumer event references unexpected tag {$eventTag}; expected {$tag}.");
                    }
                    if ($event['type'] === 'cancel') { $this->cancelRequested = false; return null; }
                    return $event['value'];
                });
            } catch (TimeoutException $error) {
                if (!$this->open || !$this->connection->isConnected()) throw $error;
                continue;
            }
            if ($message === null) break;
            $deliveryTag = $message->deliveryTag;
            $this->beginDeliveryCallback($deliveryTag);
            try { $result = $handler($message, $this); }
            catch (\Throwable $error) {
                if (!$autoAck && $this->open && !$this->callbackSettled) {
                    try { $this->nack($deliveryTag, false, true); }
                    catch (\Throwable $nackError) { throw new \RuntimeException('Consumer callback and NACK both failed: ' . $nackError->getMessage(), 0, $error); }
                }
                throw $error;
            }
            finally { $settled = $this->callbackSettled; $this->endDeliveryCallback(); }
            if (!$autoAck && $this->open && !$settled) $result === false ? $this->nack($deliveryTag, false, true) : $this->ack($deliveryTag);
        }
        return $tag;
        } finally { $this->legacyConsumerActive = false; }
    }
    public function subscribe(string $queue, callable $handler, string $consumerTag = '', bool $autoAck = false, bool $exclusive = false, array $arguments = []): Subscription
    {
        if ($this->legacyConsumerActive) throw new \LogicException('Cannot subscribe while blocking consume() is active on this channel.');
        $tag = $this->operation(function () use ($queue, $consumerTag, $autoAck, $exclusive, $arguments): string {
            $args = (new Writer())->short(0)->shortstr($queue)->shortstr($consumerTag)->bit(false)->bit($autoAck)->bit($exclusive)->bit(false)->table($arguments)->bytes();
            return $this->rpc(Protocol::BASIC, 20, $args, Protocol::BASIC, 21)->shortstr();
        });
        if (isset($this->subscriptions[$tag])) {
            $this->connection->abort();
            throw new ProtocolException("Duplicate consumer tag {$tag}.");
        }
        return $this->subscriptions[$tag] = new Subscription($this, $tag, \Closure::fromCallable($handler), $autoAck);
    }
    public function wait(float $timeout = -1.0): void
    {
        if (!is_finite($timeout) || ($timeout <= 0 && $timeout !== -1.0)) throw new \InvalidArgumentException('Wait timeout must be positive seconds or -1.');
        if ($this->subscriptionWaitActive) throw new \LogicException('A subscription wait loop is already active on this channel.');
        $this->subscriptionWaitActive = true;
        try {
        $deadline = $timeout === -1.0 ? null : microtime(true) + $timeout;
        while ($this->subscriptions !== []) {
            $remaining = $deadline === null ? -1.0 : $deadline - microtime(true);
            if ($remaining !== -1.0 && $remaining <= 0) throw new TimeoutException('Consumer wait deadline exceeded.');
            try { $event = $this->operation(fn (): array => $this->dispatcher->nextConsumerEvent(), true, $remaining); }
            catch (TimeoutException $error) {
                if (!$this->open || !$this->connection->isConnected() || $deadline !== null) throw $error;
                continue;
            }
            $tag = $event['type'] === 'delivery' ? $event['value']->consumerTag : $event['value'];
            if (!is_string($tag) || !isset($this->subscriptions[$tag])) {
                $this->connection->abort();
                throw new ProtocolException("Consumer event references unknown tag {$tag}.");
            }
            $subscription = $this->subscriptions[$tag];
            if ($event['type'] === 'cancel') {
                $subscription->markCancelled(); unset($this->subscriptions[$tag]); continue;
            }
            /** @var Message $message */
            $message = $event['value'];
            $this->beginDeliveryCallback($message->deliveryTag);
            try { $result = $subscription->dispatch($message); }
            catch (\Throwable $error) {
                if (!$subscription->autoAck && $this->open && !$this->callbackSettled) {
                    try { $this->nack($message->deliveryTag, false, true); }
                    catch (\Throwable $nackError) { throw new \RuntimeException('Consumer callback and NACK both failed: ' . $nackError->getMessage(), 0, $error); }
                }
                throw $error;
            }
            finally { $settled = $this->callbackSettled; $this->endDeliveryCallback(); }
            if (!$subscription->autoAck && $this->open && !$settled) {
                $result === false ? $this->nack($message->deliveryTag, false, true) : $this->ack($message->deliveryTag);
            }
        }
        } finally { $this->subscriptionWaitActive = false; }
    }
    public function cancel(string $consumerTag): void
    {
        $this->operation(function () use ($consumerTag): void {
            $this->connection->send(Frame::method($this->id, Protocol::BASIC, 30, (new Writer())->shortstr($consumerTag)->bit(false)->bytes()));
            $this->cancelRequested = true;
        });
    }
    public function ack(int $deliveryTag, bool $multiple = false): void { $this->sendAck(80, $deliveryTag, $multiple, null); }
    public function nack(int $deliveryTag, bool $multiple = false, bool $requeue = true): void { $this->sendAck(120, $deliveryTag, $multiple, $requeue); }
    public function reject(int $deliveryTag, bool $requeue = true): void
    {
        if ($deliveryTag <= 0) throw new \InvalidArgumentException('Reject delivery-tag must be positive.');
        $this->operation(function () use ($deliveryTag, $requeue): void {
            $this->connection->send(Frame::method($this->id, 60, 90, (new Writer())->longlong($deliveryTag)->bit($requeue)->bytes()));
            $this->markCallbackSettled($deliveryTag, false);
        });
    }
    public function recover(bool $requeue = false): void
    {
        $this->operation(fn () => $this->rpc(Protocol::BASIC, 110, (new Writer())->bit($requeue)->bytes(), Protocol::BASIC, 111));
    }
    public function recoverAsync(bool $requeue = false): void
    {
        $this->operation(fn () => $this->connection->send(Frame::method($this->id, Protocol::BASIC, 100, (new Writer())->bit($requeue)->bytes())));
    }
    public function close(): void
    {
        if (!$this->open) return;
        try {
            $this->operation(function (): void {
                $args = (new Writer())->short(200)->shortstr('Goodbye')->short(0)->short(0)->bytes(); $this->rpc(20, 40, $args, 20, 41);
            });
        } finally {
            $this->connection->execute(function (): void {
                $this->invalidate();
                $this->connection->releaseChannel($this->id);
            });
        }
    }
    public function invalidate(): void
    {
        foreach ($this->subscriptions as $subscription) $subscription->markCancelled();
        $this->subscriptions = [];
        $this->legacyConsumerActive = false; $this->subscriptionWaitActive = false;
        $this->open = false; $this->cancelRequested = false; $this->endDeliveryCallback();
        $this->returnHandler = null;
        $this->confirms->reset(); $this->dispatcher->clear();
    }
    private function sendAck(int $method, int $tag, bool $multiple, ?bool $requeue): void
    {
        if ($tag < 0 || ($tag === 0 && !$multiple)) throw new \InvalidArgumentException('Delivery-tag must be positive, or zero with multiple=true.');
        $this->operation(function () use ($method, $tag, $multiple, $requeue): void {
            $writer = (new Writer())->longlong($tag)->bit($multiple); if ($requeue !== null) $writer->bit($requeue);
            $this->connection->send(Frame::method($this->id, 60, $method, $writer->bytes()));
            $this->markCallbackSettled($tag, $multiple);
        });
    }
    private function beginDeliveryCallback(int $deliveryTag): void
    {
        $this->callbackDeliveryTag = $deliveryTag;
        $this->callbackSettled = false;
    }
    private function endDeliveryCallback(): void
    {
        $this->callbackDeliveryTag = null;
        $this->callbackSettled = false;
    }
    private function markCallbackSettled(int $deliveryTag, bool $multiple): void
    {
        if ($this->callbackDeliveryTag === null) return;
        if ($multiple ? ($deliveryTag === 0 || $this->callbackDeliveryTag <= $deliveryTag) : $this->callbackDeliveryTag === $deliveryTag) {
            $this->callbackSettled = true;
        }
    }
    private function rpc(int $class, int $method, string $args, int $expectedClass, int $expectedMethod): Reader
    {
        $this->connection->send(Frame::method($this->id, $class, $method, $args)); [$actualClass, $actualMethod, $reader] = $this->dispatcher->readMethod();
        if ($actualClass !== $expectedClass || $actualMethod !== $expectedMethod) throw new ProtocolException("Unexpected method {$actualClass}.{$actualMethod}.");
        return $reader;
    }
    /** @param array<string, mixed> $properties */
    private function writePublish(string $body, string $exchange, string $routingKey, array $properties, bool $mandatory): int
    {
        $args = (new Writer())->short(0)->shortstr($exchange)->shortstr($routingKey)->bit($mandatory)->bit(false)->bytes();
        $this->connection->send(Frame::method($this->id, Protocol::BASIC, 40, $args));
        $propertyBytes = BasicProperties::encode($properties);
        $header = pack('n2', Protocol::BASIC, 0) . (new Writer())->longlong(strlen($body))->raw($propertyBytes)->bytes();
        $this->connection->send(new Frame(Frame::HEADER, $this->id, $header));
        $chunk = max(1, ($this->connection->frameMax() ?: 131072) - 8);
        for ($offset = 0, $length = strlen($body); $offset < $length; $offset += $chunk) {
            $this->connection->send(new Frame(Frame::BODY, $this->id, substr($body, $offset, $chunk)));
        }
        return $this->confirms->published();
    }
    /**
     * @param list<array{body:string,exchange:string,routingKey:string,properties:array<string,mixed>,mandatory:bool}> $messages
     */
    private function publishBatchMessages(array $messages, float $timeout): PublisherConfirmResult
    {
        return $this->operation(function () use ($messages): PublisherConfirmResult {
            if (!$this->confirms->enabled()) {
                $this->rpc(Protocol::CONFIRM, 10, (new Writer())->bit(false)->bytes(), Protocol::CONFIRM, 11);
                $this->confirms->enable();
            }
            if ($this->confirms->hasPending()) {
                $this->dispatcher->waitForConfirms($this->confirms->highestPending(), false);
            }
            $sequences = [];
            foreach ($messages as $message) {
                $sequences[] = $this->writePublish(
                    $message['body'], $message['exchange'], $message['routingKey'], $message['properties'], $message['mandatory'],
                );
            }
            return $this->dispatcher->waitForBatch($sequences);
        }, true, $timeout);
    }
    private function operation(callable $callback, bool $requireOpen = true, ?float $timeout = null): mixed
    {
        $result = $this->connection->execute(function () use ($callback, $requireOpen): mixed {
            if ($this->busy) throw new \LogicException('Reentrant AMQP channel operation.');
            if ($requireOpen && !$this->open) throw new \LogicException('AMQP channel is closed.');
            $this->busy = true;
            try { return $callback(); }
            catch (ProtocolException $error) {
                if ($error->replyCode !== 0 || !$this->connection->isConnected()) {
                    $this->open = false; $this->connection->releaseChannel($this->id);
                }
                throw $error;
            } finally { $this->busy = false; }
        }, $timeout);
        $this->dispatchReturnedMessages();
        return $result;
    }
    private function dispatchReturnedMessages(): void
    {
        if ($this->returnHandler === null) return;
        while (($returned = $this->dispatcher->popReturn()) !== null) ($this->returnHandler)($returned, $this);
    }
}
