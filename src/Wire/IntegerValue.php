<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Wire;

final class IntegerValue
{
    /** @var array<string, array{int, int}> */
    private const RANGES = [
        'b'=>[-128, 127], 'B'=>[0, 255],
        's'=>[-32768, 32767], 'U'=>[-32768, 32767], 'u'=>[0, 65535],
        'I'=>[-2147483648, 2147483647], 'i'=>[0, 4294967295],
        'L'=>[PHP_INT_MIN, PHP_INT_MAX], 'l'=>[0, PHP_INT_MAX],
    ];

    public function __construct(public readonly int $value, public readonly string $wireType)
    {
        if (!isset(self::RANGES[$wireType])) throw new \InvalidArgumentException('Unsupported AMQP integer wire type.');
        [$minimum, $maximum] = self::RANGES[$wireType];
        if ($value < $minimum || $value > $maximum) throw new \InvalidArgumentException("Value is out of range for AMQP integer type {$wireType}.");
    }
}
