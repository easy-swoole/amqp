<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Internal;

use EasySwoole\AMQP\Connection;
use EasySwoole\AMQP\Exception\ProtocolException;
use EasySwoole\AMQP\Message;
use EasySwoole\AMQP\Protocol;
use EasySwoole\AMQP\ReturnedMessage;
use EasySwoole\AMQP\Wire\Frame;
use EasySwoole\AMQP\Wire\Reader;
use EasySwoole\AMQP\Wire\Writer;

/** Converts every channel method into one normalized internal event. */
final class ChannelEventPump
{
    private bool $publishingActive = true;

    public function __construct(
        private readonly Connection $connection,
        private readonly int $channelId,
        private readonly ContentReader $contentReader,
        private readonly PublisherConfirmTracker $confirms,
    ) {}

    public function next(bool $closeOnTimeout = true): InboundEvent
    {
        [$class, $method, $reader] = $this->connection->readMethod($this->channelId, $closeOnTimeout);
        if ($class === Protocol::CHANNEL && $method === Protocol::CHANNEL_FLOW) {
            $this->acceptFlow($reader);
            return new InboundEvent(InboundEvent::FLOW, $class, $method);
        }
        if ($class === Protocol::BASIC && $method === Protocol::BASIC_RETURN) {
            return new InboundEvent(InboundEvent::RETURNED, $class, $method, value: $this->readReturn($reader));
        }
        if ($this->confirms->enabled() && $class === Protocol::BASIC && in_array($method, [Protocol::BASIC_ACK, Protocol::BASIC_NACK], true)) {
            try { $sequences = $this->confirms->apply($reader); }
            catch (ProtocolException $error) { $this->connection->abort(); throw $error; }
            return new InboundEvent(
                InboundEvent::CONFIRMATION,
                $class,
                $method,
                sequences: $sequences,
                negative: $method === Protocol::BASIC_NACK,
            );
        }
        if ($class === Protocol::BASIC && $method === Protocol::BASIC_DELIVER) {
            return new InboundEvent(InboundEvent::DELIVERY, $class, $method, value: $this->readDelivery($reader));
        }
        if ($class === Protocol::BASIC && in_array($method, [30, 31], true)) {
            $consumerTag = $reader->shortstr();
            if ($method === 30) {
                $this->connection->send(Frame::method(
                    $this->channelId,
                    Protocol::BASIC,
                    31,
                    (new Writer())->shortstr($consumerTag)->bytes(),
                ));
            }
            return new InboundEvent(InboundEvent::CANCELLATION, $class, $method, value: $consumerTag);
        }
        return new InboundEvent(InboundEvent::METHOD, $class, $method, $reader);
    }

    public function publishingActive(): bool { return $this->publishingActive; }
    public function reset(): void { $this->publishingActive = true; }

    public function handleServerFlow(int $class, int $method, Reader $reader): bool
    {
        if ($class !== Protocol::CHANNEL || $method !== Protocol::CHANNEL_FLOW) return false;
        $this->acceptFlow($reader);
        return true;
    }

    private function acceptFlow(Reader $reader): void
    {
        $this->publishingActive = $reader->bit();
        $this->connection->send(Frame::method(
            $this->channelId,
            Protocol::CHANNEL,
            Protocol::CHANNEL_FLOW_OK,
            (new Writer())->bit($this->publishingActive)->bytes(),
        ));
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
}
