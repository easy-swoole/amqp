<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Transport;

use EasySwoole\AMQP\Config;
use EasySwoole\AMQP\Exception\ConnectionException;
use EasySwoole\AMQP\Exception\TimeoutException;
use Swoole\Coroutine;
use Swoole\Coroutine\Socket;

final class SwooleSocketTransport implements TransportInterface
{
    private ?Socket $socket = null;
    public function __construct(private readonly Config $config) {}
    public function connect(): void
    {
        if (Coroutine::getCid() < 0) throw new ConnectionException('AMQP socket operations must run inside a Swoole coroutine.');
        $this->close();
        $this->socket = new Socket(str_contains($this->config->host, ':') ? AF_INET6 : AF_INET, SOCK_STREAM, 0);
        if ($this->config->tls !== null && !$this->socket->setProtocol($this->config->tls->socketSettings())) $this->fail('configure TLS');
        if (!$this->socket->connect($this->config->host, $this->config->port, $this->config->connectTimeout)) $this->fail('connect');
    }
    public function write(string $data): void
    {
        if ($this->socket === null || $this->socket->sendAll($data, $this->config->writeTimeout) !== strlen($data)) $this->fail('write');
    }
    public function read(int $length): string
    {
        $data = $this->socket?->recvAll($length, $this->config->readTimeout);
        if (($data === false || $data === '') && $length > 0 && $this->isTimeoutError()) {
            throw new TimeoutException('AMQP read timed out.', $this->socket?->errCode ?? 0);
        }
        if (!is_string($data) || strlen($data) !== $length) $this->fail('read');
        return $data;
    }
    public function isConnected(): bool { return $this->socket !== null; }
    public function close(): void { $socket = $this->socket; $this->socket = null; $socket?->close(); }
    private function fail(string $operation): never
    {
        $code = $this->socket?->errCode ?? 0; $message = $this->socket?->errMsg ?? 'socket is closed'; $this->close();
        throw new ConnectionException("AMQP {$operation} failed: {$message}", $code);
    }
    private function isTimeoutError(): bool
    {
        $code = $this->socket?->errCode ?? 0;
        $timeoutCodes = [60, 110];
        if (defined('SOCKET_ETIMEDOUT')) $timeoutCodes[] = constant('SOCKET_ETIMEDOUT');
        return in_array($code, $timeoutCodes, true) || str_contains(strtolower($this->socket?->errMsg ?? ''), 'timed out');
    }
}
