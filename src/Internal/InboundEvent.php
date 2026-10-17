<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Internal;

use EasySwoole\AMQP\Wire\Reader;

final class InboundEvent
{
    public const METHOD = 'method';
    public const DELIVERY = 'delivery';
    public const RETURNED = 'returned';
    public const CANCELLATION = 'cancellation';
    public const CONFIRMATION = 'confirmation';
    public const FLOW = 'flow';

    /** @param list<int> $sequences */
    public function __construct(
        public readonly string $type,
        public readonly int $classId = 0,
        public readonly int $methodId = 0,
        public readonly ?Reader $reader = null,
        public readonly mixed $value = null,
        public readonly array $sequences = [],
        public readonly bool $negative = false,
    ) {}
}
