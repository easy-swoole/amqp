<?php
declare(strict_types=1);

namespace EasySwoole\AMQP;

final class Config
{
    public const AUTH_PLAIN = 'PLAIN';
    public const AUTH_EXTERNAL = 'EXTERNAL';

    public function __construct(
        public readonly string $host = '127.0.0.1',
        public readonly int $port = 5672,
        public readonly string $user = 'guest',
        public readonly string $password = 'guest',
        public readonly string $vhost = '/',
        public readonly float $connectTimeout = 3.0,
        public readonly float $readTimeout = 60.0,
        public readonly float $writeTimeout = 3.0,
        public readonly int $heartbeat = 0,
        public readonly int $channelMax = 2047,
        public readonly int $frameMax = 131072,
        public readonly int $maxMessageSize = 67_108_864,
        public readonly ?TlsConfig $tls = null,
        public readonly string $authMechanism = self::AUTH_PLAIN,
    ) {
        if (trim($host) === '' || $port < 1 || $port > 65535 || $heartbeat < 0 || $heartbeat > 65535) throw new \InvalidArgumentException('Invalid AMQP connection configuration.');
        if ($channelMax < 0 || $channelMax > 65535 || ($frameMax !== 0 && $frameMax < 4096)) throw new \InvalidArgumentException('Invalid AMQP tuning limits.');
        if ($maxMessageSize < 1) throw new \InvalidArgumentException('Maximum message size must be positive.');
        foreach ([$connectTimeout, $readTimeout, $writeTimeout] as $timeout) {
            if (!is_finite($timeout) || ($timeout <= 0 && $timeout !== -1.0)) throw new \InvalidArgumentException('Timeouts must be positive seconds or -1.');
        }
        if (!in_array($authMechanism, [self::AUTH_PLAIN, self::AUTH_EXTERNAL], true)) throw new \InvalidArgumentException('Unsupported AMQP authentication mechanism.');
        if ($authMechanism === self::AUTH_EXTERNAL && ($tls?->clientCertificate === null || $tls->clientPrivateKey === null)) {
            throw new \InvalidArgumentException('EXTERNAL authentication requires a TLS client certificate and private key.');
        }
    }
}
