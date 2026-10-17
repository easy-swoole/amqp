<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Internal;

use EasySwoole\AMQP\Connection;
use EasySwoole\AMQP\Exception\ProtocolException;
use EasySwoole\AMQP\Message;
use EasySwoole\AMQP\Protocol;
use EasySwoole\AMQP\PublisherConfirmResult;
use EasySwoole\AMQP\ReturnedMessage;
use EasySwoole\AMQP\Wire\Frame;
use EasySwoole\AMQP\Wire\Reader;
use EasySwoole\AMQP\Wire\Writer;

final class ChannelDispatcher
{
    /** @var list<ReturnedMessage> */
    private array $returns = [];
    /** @var list<array{type:'delivery'|'cancel', value:Message|string}> */
    private array $consumerEvents = [];

    public function __construct(
        private readonly Connection $connection,
        private readonly int $channelId,
        private readonly ContentReader $contentReader,
        private readonly PublisherConfirmTracker $confirms,
    ) {}

    /** @return array{int, int, Reader} */
    public function readMethod(bool $closeOnTimeout = true, bool $bufferDeliveries = true, bool $returnCancellation = false): array
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
                $this->consumerEvents[] = ['type'=>'delivery', 'value'=>$this->readDelivery($reader)];
                continue;
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
                if ($returnCancellation) return [$class, $method, new Reader('')];
                $this->consumerEvents[] = ['type'=>'cancel', 'value'=>$consumerTag];
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
                if ($captureReturn) $returned = $message;
                else $this->returns[] = $message;
                continue;
            }
            if ($class === Protocol::BASIC && $method === Protocol::BASIC_DELIVER) {
                $this->consumerEvents[] = ['type'=>'delivery', 'value'=>$this->readDelivery($reader)];
                continue;
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
                $this->consumerEvents[] = ['type'=>'cancel', 'value'=>$consumerTag];
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

    /** @return array{type:'delivery'|'cancel', value:Message|string} */
    public function nextConsumerEvent(): array
    {
        $queued = $this->popConsumerEvent();
        if ($queued !== null) return $queued;
        while (true) {
            [$class, $method, $reader] = $this->connection->readMethod($this->channelId, false);
            if ($class === Protocol::BASIC && $method === Protocol::BASIC_RETURN) {
                $this->returns[] = $this->readReturn($reader);
                continue;
            }
            if ($this->confirms->enabled() && $class === Protocol::BASIC && in_array($method, [Protocol::BASIC_ACK, Protocol::BASIC_NACK], true)) {
                $this->applyConfirmation($reader);
                if ($method === Protocol::BASIC_NACK) throw new ProtocolException('RabbitMQ negatively acknowledged a published message.');
                continue;
            }
            if ($class === Protocol::BASIC && $method === Protocol::BASIC_DELIVER) {
                return ['type'=>'delivery', 'value'=>$this->readDelivery($reader)];
            }
            if ($class === Protocol::BASIC && in_array($method, [30, 31], true)) {
                $consumerTag = $reader->shortstr();
                if ($method === 30) {
                    $this->connection->send(Frame::method(
                        $this->channelId, Protocol::BASIC, 31, (new Writer())->shortstr($consumerTag)->bytes(),
                    ));
                }
                return ['type'=>'cancel', 'value'=>$consumerTag];
            }
            $this->protocolFailure("Expected consumer event, received {$class}.{$method}.");
        }
    }

    /** @param list<int> $sequences */
    public function waitForBatch(array $sequences): PublisherConfirmResult
    {
        $target = max($sequences);
        $first = min($sequences);
        $acked = []; $nacked = []; $returned = [];
        while ($this->confirms->hasPendingThrough($target)) {
            [$class, $method, $reader] = $this->connection->readMethod($this->channelId);
            if ($class === Protocol::BASIC && $method === Protocol::BASIC_RETURN) {
                $message = $this->readReturn($reader);
                $returned[] = $message;
                continue;
            }
            if ($class === Protocol::BASIC && $method === Protocol::BASIC_DELIVER) {
                $this->consumerEvents[] = ['type'=>'delivery', 'value'=>$this->readDelivery($reader)];
                continue;
            }
            if ($class === Protocol::BASIC && in_array($method, [30, 31], true)) {
                $this->bufferCancellation($method, $reader);
                continue;
            }
            if ($class !== Protocol::BASIC || !in_array($method, [Protocol::BASIC_ACK, Protocol::BASIC_NACK], true)) {
                $this->protocolFailure("Expected publisher confirmation, received {$class}.{$method}.");
            }
            $settled = $this->applyConfirmation($reader);
            foreach ($settled as $sequence) {
                if ($sequence < $first || $sequence > $target) continue;
                if ($method === Protocol::BASIC_ACK) $acked[] = $sequence - $first + 1;
                else $nacked[] = $sequence - $first + 1;
            }
        }
        sort($acked); sort($nacked);
        return new PublisherConfirmResult($acked, $nacked, $returned);
    }

    public function popReturn(): ?ReturnedMessage { return array_shift($this->returns); }
    /** @return null|array{type:'delivery'|'cancel', value:Message|string} */
    public function popConsumerEvent(): ?array { return array_shift($this->consumerEvents); }
    public function clear(): void { $this->returns = []; $this->consumerEvents = []; }

    /** @return list<int> */
    private function applyConfirmation(Reader $reader): array
    {
        try { return $this->confirms->apply($reader); }
        catch (ProtocolException $error) { $this->connection->abort(); throw $error; }
    }
    private function bufferCancellation(int $method, Reader $reader): void
    {
        $consumerTag = $reader->shortstr();
        if ($method === 30) {
            $this->connection->send(Frame::method(
                $this->channelId,
                Protocol::BASIC,
                31,
                (new Writer())->shortstr($consumerTag)->bytes(),
            ));
        }
        $this->consumerEvents[] = ['type'=>'cancel', 'value'=>$consumerTag];
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
