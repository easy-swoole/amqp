<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Internal;

use EasySwoole\AMQP\Connection;
use EasySwoole\AMQP\Exception\ProtocolException;
use EasySwoole\AMQP\Message;
use EasySwoole\AMQP\Protocol;
use EasySwoole\AMQP\ReturnedMessage;
use EasySwoole\AMQP\Wire\Reader;

final class ChannelDispatcher
{
    /** @var list<ReturnedMessage> */
    private array $returns = [];
    /** @var list<Message> */
    private array $deliveries = [];

    public function __construct(
        private readonly Connection $connection,
        private readonly int $channelId,
        private readonly ContentReader $contentReader,
        private readonly PublisherConfirmTracker $confirms,
    ) {}

    /** @return array{int, int, Reader} */
    public function readMethod(bool $closeOnTimeout = true, bool $bufferDeliveries = true): array
    {
        while (true) {
            [$class, $method, $reader] = $this->connection->readMethod($this->channelId, $closeOnTimeout);
            if ($class === Protocol::BASIC && $method === Protocol::BASIC_RETURN) {
                $this->returns[] = $this->readReturn($reader);
                continue;
            }
            if ($this->confirms->enabled() && $class === Protocol::BASIC && in_array($method, [Protocol::BASIC_ACK, Protocol::BASIC_NACK], true)) {
                $this->applyConfirmation($reader);
                if ($method === Protocol::BASIC_NACK) throw new ProtocolException('RabbitMQ negatively acknowledged a published message.');
                continue;
            }
            if ($bufferDeliveries && $class === Protocol::BASIC && $method === Protocol::BASIC_DELIVER) {
                $this->deliveries[] = $this->readDelivery($reader);
                continue;
            }
            return [$class, $method, $reader];
        }
    }

    public function waitForConfirms(int $targetSequence, bool $captureReturn): ?ReturnedMessage
    {
        $returned = null;
        while ($this->confirms->hasPendingThrough($targetSequence)) {
            [$class, $method, $reader] = $this->connection->readMethod($this->channelId);
            if ($class === Protocol::BASIC && $method === Protocol::BASIC_RETURN) {
                $message = $this->readReturn($reader);
                $this->returns[] = $message;
                if ($captureReturn) $returned = $message;
                continue;
            }
            if ($class === Protocol::BASIC && $method === Protocol::BASIC_DELIVER) {
                $this->deliveries[] = $this->readDelivery($reader);
                continue;
            }
            if ($class !== Protocol::BASIC || !in_array($method, [Protocol::BASIC_ACK, Protocol::BASIC_NACK], true)) {
                $this->protocolFailure("Expected publisher confirmation, received {$class}.{$method}.");
            }
            $this->applyConfirmation($reader);
            if ($method === Protocol::BASIC_NACK) throw new ProtocolException('RabbitMQ negatively acknowledged a published message.');
        }
        return $returned;
    }

    public function popReturn(): ?ReturnedMessage { return array_shift($this->returns); }
    public function popDelivery(): ?Message { return array_shift($this->deliveries); }
    public function clear(): void { $this->returns = []; $this->deliveries = []; }

    private function applyConfirmation(Reader $reader): void
    {
        try { $this->confirms->apply($reader); }
        catch (ProtocolException $error) { $this->connection->abort(); throw $error; }
    }
    private function readReturn(Reader $reader): ReturnedMessage
    {
        $replyCode = $reader->short();
        $replyText = $reader->shortstr();
        $exchange = $reader->shortstr();
        $routingKey = $reader->shortstr();
        return new ReturnedMessage(
            $replyCode,
            $replyText,
            $exchange,
            $routingKey,
            $this->contentReader->read(0, false, $exchange, $routingKey),
        );
    }
    private function readDelivery(Reader $reader): Message
    {
        $consumerTag = $reader->shortstr();
        $deliveryTag = $reader->longlong();
        $redelivered = $reader->bit();
        $exchange = $reader->shortstr();
        $routingKey = $reader->shortstr();
        return $this->contentReader->read($deliveryTag, $redelivered, $exchange, $routingKey, $consumerTag);
    }
    private function protocolFailure(string $message): never
    {
        $this->connection->abort();
        throw new ProtocolException($message);
    }
}
