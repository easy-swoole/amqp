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
    /** @var array<int, true> */
    private array $settled = [];
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
        if ($prefetchCount < 0 || $prefetchCount > 65535 || $prefetchSize < 0) throw new \InvalidArgumentException('Invalid QoS values.');
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
        $tag = $this->operation(function () use ($queue, $consumerTag, $autoAck, $exclusive, $arguments): string {
            $args = (new Writer())->short(0)->shortstr($queue)->shortstr($consumerTag)->bit(false)->bit($autoAck)->bit($exclusive)->bit(false)->table($arguments)->bytes();
            return $this->rpc(60, 20, $args, 60, 21)->shortstr();
        });
        // 每次只让 I/O worker 读取一条投递，回调仍在调用方协程执行，允许安全 ack/nack。
        while ($this->open) {
            try {
                $message = $this->operation(function (): ?Message {
                $queued = $this->dispatcher->popDelivery();
                if ($queued !== null) return $queued;
                [$class, $method, $delivery] = $this->dispatcher->readMethod(false, false);
                if ($class === 60 && $method === 30) {
                    $cancelledTag = $delivery->shortstr();
                    $this->connection->send(Frame::method($this->id, 60, 31, (new Writer())->shortstr($cancelledTag)->bytes()));
                    return null;
                }
                if ($class === 60 && $method === 31 && $this->cancelRequested) { $this->cancelRequested = false; return null; }
                if ($class !== 60 || $method !== 60) throw new ProtocolException('Expected basic.deliver.');
                $consumer = $delivery->shortstr(); $deliveryTag = $delivery->longlong(); $redelivered = $delivery->bit(); $exchange = $delivery->shortstr(); $routingKey = $delivery->shortstr();
                return $this->contentReader->read($deliveryTag, $redelivered, $exchange, $routingKey, $consumer);
                });
            } catch (TimeoutException $error) {
                if (!$this->open || !$this->connection->isConnected()) throw $error;
                continue;
            }
            if ($message === null) break;
            $deliveryTag = $message->deliveryTag;
            try { $result = $handler($message, $this); }
            catch (\Throwable $error) {
                if (!$autoAck && $this->open && !isset($this->settled[$deliveryTag])) {
                    try { $this->nack($deliveryTag, false, true); }
                    catch (\Throwable $nackError) { throw new \RuntimeException('Consumer callback and NACK both failed: ' . $nackError->getMessage(), 0, $error); }
                }
                throw $error;
            }
            if (!$autoAck && $this->open && !isset($this->settled[$deliveryTag])) $result === false ? $this->nack($deliveryTag, false, true) : $this->ack($deliveryTag);
            unset($this->settled[$deliveryTag]);
        }
        return $tag;
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
        $this->operation(function () use ($deliveryTag, $requeue): void {
            $this->connection->send(Frame::method($this->id, 60, 90, (new Writer())->longlong($deliveryTag)->bit($requeue)->bytes()));
            $this->settled[$deliveryTag] = true;
        });
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
                $this->open = false;
                $this->connection->releaseChannel($this->id);
            });
        }
    }
    public function invalidate(): void
    {
        $this->open = false; $this->cancelRequested = false; $this->settled = [];
        $this->confirms->reset(); $this->dispatcher->clear();
    }
    private function sendAck(int $method, int $tag, bool $multiple, ?bool $requeue): void
    {
        $this->operation(function () use ($method, $tag, $multiple, $requeue): void {
            $writer = (new Writer())->longlong($tag)->bit($multiple); if ($requeue !== null) $writer->bit($requeue);
            $this->connection->send(Frame::method($this->id, 60, $method, $writer->bytes()));
            $this->settled[$tag] = true;
        });
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
