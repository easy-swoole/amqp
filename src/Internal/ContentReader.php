<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Internal;

use EasySwoole\AMQP\Connection;
use EasySwoole\AMQP\Exception\ProtocolException;
use EasySwoole\AMQP\Message;
use EasySwoole\AMQP\Wire\BasicProperties;
use EasySwoole\AMQP\Wire\Frame;
use EasySwoole\AMQP\Wire\Reader;

final class ContentReader
{
    public function __construct(private readonly Connection $connection, private readonly int $channelId) {}

    public function read(
        int $deliveryTag,
        bool $redelivered,
        string $exchange,
        string $routingKey,
        ?string $consumerTag = null,
        ?int $messageCount = null,
    ): Message {
        try {
            $frame = $this->connection->readFrame($this->channelId);
            if ($frame->type !== Frame::HEADER) throw new ProtocolException('Expected content header.');
            $reader = new Reader($frame->payload);
            if ($reader->short() !== 60 || $reader->short() !== 0) throw new ProtocolException('Invalid basic content header.');
            $size = $reader->longlong();
            $properties = BasicProperties::decode($reader);
            if ($size > $this->connection->maxMessageSize()) throw new ProtocolException('Message exceeds configured maximum size.');
            $body = '';
            while (strlen($body) < $size) {
                $part = $this->connection->readFrame($this->channelId);
                if ($part->type !== Frame::BODY) throw new ProtocolException('Expected content body.');
                $body .= $part->payload;
            }
            if (strlen($body) !== $size) throw new ProtocolException('Content body exceeds declared size.');
            return new Message($body, $properties, $deliveryTag, $redelivered, $exchange, $routingKey, $consumerTag, $messageCount);
        } catch (ProtocolException $error) {
            $this->connection->abort();
            throw $error;
        }
    }
}
