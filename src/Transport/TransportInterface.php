<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Transport;

interface TransportInterface
{
    public function connect(): void;
    public function write(string $data): void;
    public function read(int $length): string;
    public function close(): void;
    public function isConnected(): bool;
}
