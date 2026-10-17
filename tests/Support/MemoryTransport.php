<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Tests\Support;

use EasySwoole\AMQP\Exception\ConnectionException;
use EasySwoole\AMQP\Transport\TransportInterface;

final class MemoryTransport implements TransportInterface
{
    public string $written = '';
    private int $offset = 0;
    private bool $connected = false;
    public function __construct(public string $incoming = '') {}
    public function connect(): void { $this->connected = true; }
    public function write(string $data): void { if (!$this->connected) throw new ConnectionException('closed'); $this->written .= $data; }
    public function read(int $length): string
    {
        $data = substr($this->incoming, $this->offset, $length);
        if (strlen($data) !== $length) throw new ConnectionException('incomplete test input');
        $this->offset += $length; return $data;
    }
    public function close(): void { $this->connected = false; }
    public function isConnected(): bool { return $this->connected; }
}
