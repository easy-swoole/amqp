<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Tests;

use EasySwoole\AMQP\Config;
use EasySwoole\AMQP\TlsConfig;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ConfigTest extends TestCase
{
    #[DataProvider('invalidConfigs')]
    public function testRejectsInvalidConfig(array $arguments): void
    {
        $this->expectException(\InvalidArgumentException::class); new Config(...$arguments);
    }
    public static function invalidConfigs(): iterable
    {
        yield 'host' => [['host'=>'']]; yield 'port' => [['port'=>0]]; yield 'heartbeat' => [['heartbeat'=>-1]];
        yield 'frame max' => [['frameMax'=>1024]]; yield 'timeout' => [['readTimeout'=>0.0]];
        yield 'message max' => [['maxMessageSize'=>0]];
        yield 'authentication mechanism' => [['authMechanism'=>'UNKNOWN']];
        yield 'external without TLS certificate' => [['authMechanism'=>Config::AUTH_EXTERNAL]];
        yield 'external TLS without client certificate' => [['authMechanism'=>Config::AUTH_EXTERNAL, 'tls'=>new TlsConfig()]];
    }
}
