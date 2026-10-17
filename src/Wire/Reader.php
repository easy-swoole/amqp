<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Wire;

use EasySwoole\AMQP\Exception\ProtocolException;

final class Reader
{
    private int $offset = 0;
    private int $bits = 0;
    private int $bitOffset = 8;
    public function __construct(private readonly string $data) {}
    public function remaining(): int { return strlen($this->data) - $this->offset; }
    public function raw(int $length): string
    {
        $this->bitOffset = 8;
        if ($length < 0 || $this->remaining() < $length) throw new ProtocolException('Truncated AMQP payload.');
        $value = substr($this->data, $this->offset, $length); $this->offset += $length; return $value;
    }
    public function octet(): int { return unpack('C', $this->raw(1))[1]; }
    public function short(): int { return unpack('n', $this->raw(2))[1]; }
    public function long(): int { return unpack('N', $this->raw(4))[1]; }
    public function longlong(): int { $v = unpack('Nhigh/Nlow', $this->raw(8)); return ($v['high'] << 32) | $v['low']; }
    public function shortstr(): string { return $this->raw($this->octet()); }
    public function longstr(): string { return $this->raw($this->long()); }
    public function bit(): bool
    {
        if ($this->bitOffset === 8) { $this->bits = $this->octet(); $this->bitOffset = 0; }
        return (bool) ($this->bits & (1 << $this->bitOffset++));
    }
    /** @return array<string, mixed> */
    public function table(): array
    {
        $reader = new self($this->longstr()); $result = [];
        while ($reader->remaining() > 0) {
            $key = $reader->shortstr(); $type = $reader->raw(1);
            $result[$key] = $reader->field($type);
        }
        return $result;
    }
    /** @return list<mixed> */
    public function array(): array
    {
        $reader = new self($this->longstr()); $result = [];
        while ($reader->remaining() > 0) $result[] = $reader->field($reader->raw(1));
        return $result;
    }
    private function field(string $type): mixed
    {
        return match ($type) {
            'S', 'x' => $this->longstr(), 't' => (bool) $this->octet(),
            'b' => unpack('c', $this->raw(1))[1], 'B' => $this->octet(),
            's' => self::signed16($this->short()), 'I' => self::signed32($this->long()),
            'i' => $this->long(), 'l', 'T' => $this->longlong(),
            'f' => unpack('G', $this->raw(4))[1], 'd' => unpack('E', $this->raw(8))[1],
            'F' => $this->table(), 'A' => $this->array(), 'V' => null,
            default => throw new ProtocolException("Unsupported AMQP field type {$type}."),
        };
    }
    private static function signed16(int $value): int { return $value > 0x7fff ? $value - 0x10000 : $value; }
    private static function signed32(int $value): int { return $value > 0x7fffffff ? $value - 0x100000000 : $value; }
}
