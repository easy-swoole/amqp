<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Wire;

final class ByteArray
{
    public function __construct(public readonly string $value) {}
}
