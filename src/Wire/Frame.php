<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Wire;

use EasySwoole\AMQP\Exception\ProtocolException;

final class Frame
{
    public const METHOD = 1, HEADER = 2, BODY = 3, HEARTBEAT = 8, END = 0xce;
    public function __construct(public readonly int $type, public readonly int $channel, public readonly string $payload)
    {
        if (!in_array($type, [self::METHOD, self::HEADER, self::BODY, self::HEARTBEAT], true)) throw new \InvalidArgumentException('Unsupported AMQP frame type.');
        if ($channel < 0 || $channel > 65535) throw new \InvalidArgumentException('AMQP frame channel is out of range.');
        if ($type === self::HEARTBEAT && ($channel !== 0 || $payload !== '')) throw new \InvalidArgumentException('AMQP heartbeat frames must be empty on channel zero.');
    }
    public function encode(): string { return pack('CnN', $this->type, $this->channel, strlen($this->payload)) . $this->payload . chr(self::END); }
    public static function read(callable $read, int $frameMax = 0): self
    {
        $header = $read(7);
        if (strlen($header) !== 7) throw new ProtocolException('Truncated AMQP frame header.');
        $parts = unpack('Ctype/nchannel/Nsize', $header); $size = $parts['size'];
        if (!in_array($parts['type'], [self::METHOD, self::HEADER, self::BODY, self::HEARTBEAT], true)) {
            throw new ProtocolException("Unsupported AMQP frame type {$parts['type']}.");
        }
        if ($parts['type'] === self::HEARTBEAT && ($parts['channel'] !== 0 || $size !== 0)) {
            throw new ProtocolException('Invalid AMQP heartbeat frame.');
        }
        if ($frameMax > 0 && $size + 8 > $frameMax) throw new ProtocolException('Inbound frame exceeds negotiated frame-max.');
        $payload = $size === 0 ? '' : $read($size);
        if (strlen($payload) !== $size) throw new ProtocolException('Truncated AMQP frame payload.');
        $end = $read(1);
        if (strlen($end) !== 1 || ord($end) !== self::END) throw new ProtocolException('Invalid AMQP frame terminator.');
        return new self($parts['type'], $parts['channel'], $payload);
    }
    public static function method(int $channel, int $classId, int $methodId, string $arguments = ''): self
    {
        if ($classId < 0 || $classId > 65535 || $methodId < 0 || $methodId > 65535) throw new \InvalidArgumentException('AMQP class or method id is out of range.');
        return new self(self::METHOD, $channel, pack('n2', $classId, $methodId) . $arguments);
    }
    /** @return array{0:int,1:int,2:Reader} */
    public function methodParts(): array
    {
        if ($this->type !== self::METHOD) throw new ProtocolException('Expected AMQP method frame.');
        $reader = new Reader($this->payload); return [$reader->short(), $reader->short(), $reader];
    }
}
