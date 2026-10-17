<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Tests;

use EasySwoole\AMQP\Config;
use EasySwoole\AMQP\TlsConfig;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;
use Swoole\Coroutine\Socket;

final class TlsConfigTest extends TestCase
{
    public function testStrictDefaultsAndSocketSettings(): void
    {
        $tls = new TlsConfig();
        self::assertSame([
            'open_ssl'=>true,
            'ssl_verify_peer'=>true,
            'ssl_allow_self_signed'=>false,
        ], $tls->socketSettings());
        self::assertSame($tls, (new Config(tls: $tls))->tls);
    }

    public function testMutualTlsSettingsAreMapped(): void
    {
        $tls = new TlsConfig(
            allowSelfSigned: true,
            caFile: '/certs/ca.pem',
            caPath: '/certs',
            clientCertificate: '/certs/client.pem',
            clientPrivateKey: '/certs/client-key.pem',
            passphrase: 'secret',
            serverName: 'rabbit.internal',
        );
        self::assertSame('/certs/ca.pem', $tls->socketSettings()['ssl_cafile']);
        self::assertSame('/certs/client.pem', $tls->socketSettings()['ssl_cert_file']);
        self::assertSame('/certs/client-key.pem', $tls->socketSettings()['ssl_key_file']);
        self::assertSame('secret', $tls->socketSettings()['ssl_passphrase']);
        self::assertSame('rabbit.internal', $tls->socketSettings()['ssl_host_name']);
    }

    public function testSettingsAreAcceptedByCoroutineSocket(): void
    {
        $accepted = false;
        Coroutine\run(function () use (&$accepted): void {
            $socket = new Socket(AF_INET, SOCK_STREAM, 0);
            $accepted = $socket->setProtocol((new TlsConfig(verifyPeer: false))->socketSettings());
            $socket->close();
        });
        self::assertTrue($accepted);
    }

    #[DataProvider('invalidTlsProvider')]
    public function testInvalidTlsCombinationsAreRejected(array $arguments): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new TlsConfig(...$arguments);
    }

    public static function invalidTlsProvider(): iterable
    {
        yield 'self-signed without verification' => [['verifyPeer'=>false, 'allowSelfSigned'=>true]];
        yield 'certificate without key' => [['clientCertificate'=>'/certs/client.pem']];
        yield 'key without certificate' => [['clientPrivateKey'=>'/certs/client-key.pem']];
        yield 'passphrase without key' => [['passphrase'=>'secret']];
        yield 'empty CA file' => [['caFile'=>' ']];
        yield 'empty server name' => [['serverName'=>' ']];
    }
}
