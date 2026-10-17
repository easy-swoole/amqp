<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Internal;

use EasySwoole\AMQP\Wire\Reader;

/** Internal connection registry entry for routing cross-channel control methods. */
final class ChannelEndpoint
{
    /**
     * @param \Closure(int, int, Reader):bool $methodHandler
     * @param \Closure():void $invalidator
     */
    public function __construct(
        private readonly \Closure $methodHandler,
        private readonly \Closure $invalidator,
    ) {}

    public function handleMethod(int $class, int $method, Reader $reader): bool { return ($this->methodHandler)($class, $method, $reader); }
    public function invalidate(): void { ($this->invalidator)(); }
}
