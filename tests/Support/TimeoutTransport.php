<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Tests\Support;

use EasySwoole\AMQP\Exception\TimeoutException;
use EasySwoole\AMQP\Transport\TransportInterface;

final class TimeoutTransport implements TransportInterface
{
    public int $closeCount = 0;
    private int $reads = 0;

    public function __construct(private readonly ?string $firstRead = null) {}
    public function connect(): void {}
    public function write(string $data): void {}
    public function read(int $length): string
    {
        if ($this->reads++ === 0 && $this->firstRead !== null) return substr($this->firstRead, 0, $length);
        throw new TimeoutException('test timeout');
    }
    public function close(): void { $this->closeCount++; }
    public function isConnected(): bool { return $this->closeCount === 0; }
}
