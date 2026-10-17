<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Tests\Support;

use EasySwoole\AMQP\Exception\ConnectionException;
use EasySwoole\AMQP\Transport\TransportInterface;
use Swoole\Coroutine;

final class BlockingTransport implements TransportInterface
{
    public string $written = '';
    private int $offset = 0;
    private bool $connected = false;
    public function __construct(private readonly string $incoming) {}
    public function connect(): void { $this->connected = true; }
    public function write(string $data): void
    {
        if (!$this->connected) throw new ConnectionException('closed');
        $this->written .= $data;
    }
    public function read(int $length): string
    {
        while ($this->connected && strlen($this->incoming) - $this->offset < $length) Coroutine::sleep(0.001);
        if (!$this->connected) throw new ConnectionException('closed');
        $data = substr($this->incoming, $this->offset, $length);
        $this->offset += $length;
        return $data;
    }
    public function close(): void { $this->connected = false; }
    public function isConnected(): bool { return $this->connected; }
}
