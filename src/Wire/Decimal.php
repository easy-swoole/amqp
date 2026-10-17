<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Wire;

final class Decimal
{
    public function __construct(public readonly int $unscaled, public readonly int $scale)
    {
        if ($scale < 0 || $scale > 255) throw new \InvalidArgumentException('Decimal scale must be between 0 and 255.');
        if ($unscaled < -0x80000000 || $unscaled > 0x7fffffff) throw new \InvalidArgumentException('Decimal unscaled value exceeds signed 32-bit range.');
    }
}
