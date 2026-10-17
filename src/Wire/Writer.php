<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Wire;

final class Writer
{
    private string $data = '';
    private int $bits = 0;
    private int $bitOffset = 0;
    public function octet(int $value): self { if ($value < 0 || $value > 255) throw new \InvalidArgumentException('octet is out of range.'); $this->flushBits(); $this->data .= pack('C', $value); return $this; }
    public function short(int $value): self { if ($value < 0 || $value > 65535) throw new \InvalidArgumentException('short is out of range.'); $this->flushBits(); $this->data .= pack('n', $value); return $this; }
    public function long(int $value): self { $this->flushBits(); $this->data .= pack('N', $value); return $this; }
    public function longlong(int $value): self { $this->flushBits(); $this->data .= pack('N2', ($value >> 32) & 0xffffffff, $value & 0xffffffff); return $this; }
    public function shortstr(string $value): self
    {
        if (strlen($value) > 255) throw new \InvalidArgumentException('shortstr exceeds 255 bytes.');
        return $this->octet(strlen($value))->raw($value);
    }
    public function longstr(string $value): self { return $this->long(strlen($value))->raw($value); }
    public function raw(string $value): self { $this->flushBits(); $this->data .= $value; return $this; }
    public function bit(bool $value): self
    {
        if ($value) $this->bits |= 1 << $this->bitOffset;
        if (++$this->bitOffset === 8) $this->flushBits();
        return $this;
    }
    /** @param array<string, mixed> $values */
    public function table(array $values): self
    {
        $table = new self();
        foreach ($values as $key => $value) {
            $table->shortstr($key);
            $table->field($value);
        }
        return $this->longstr($table->bytes());
    }
    /** @param list<mixed> $values */
    public function array(array $values): self
    {
        $data = new self(); foreach ($values as $value) $data->field($value);
        return $this->longstr($data->bytes());
    }
    public function bytes(): string { $this->flushBits(); return $this->data; }
    private function flushBits(): void
    {
        if ($this->bitOffset > 0) { $this->data .= pack('C', $this->bits); $this->bits = $this->bitOffset = 0; }
    }
    private function field(mixed $value): void
    {
        match (true) {
            is_string($value) => $this->raw('S')->longstr($value),
            is_bool($value) => $this->raw('t')->octet($value ? 1 : 0),
            is_int($value) && $value >= -0x80000000 && $value <= 0x7fffffff => $this->raw('I')->long($value),
            is_int($value) => $this->raw('l')->longlong($value),
            is_float($value) => $this->raw('d')->raw(pack('E', $value)),
            is_array($value) && array_is_list($value) => $this->raw('A')->array($value),
            is_array($value) => $this->raw('F')->table($value),
            $value === null => $this->raw('V'),
            default => throw new \InvalidArgumentException('Unsupported AMQP field value.'),
        };
    }
}
