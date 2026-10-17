<?php
declare(strict_types=1);

namespace EasySwoole\AMQP;

use EasySwoole\AMQP\Transport\TransportInterface;

final class AMQP
{
    private readonly Connection $connection;
    public function __construct(Config $config = new Config(), ?TransportInterface $transport = null) { $this->connection = new Connection($config, $transport); }
    public function connect(): void { $this->connection->connect(); }
    public function channel(?int $id = null): Channel { return $this->connection->channel($id); }
    public function isConnected(): bool { return $this->connection->isConnected(); }
    public function blockedReason(): ?string { return $this->connection->blockedReason(); }
    public function close(): void { $this->connection->close(); }
}
