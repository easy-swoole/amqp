<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Tests;

use EasySwoole\AMQP\AMQP;
use EasySwoole\AMQP\Channel;
use EasySwoole\AMQP\Config;
use EasySwoole\AMQP\Exception\MessageReturnedException;
use EasySwoole\AMQP\Message;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;

#[Group('integration')]
final class RabbitMQIntegrationTest extends TestCase
{
    protected function setUp(): void
    {
        if (getenv('AMQP_INTEGRATION') !== '1') self::markTestSkipped('Set AMQP_INTEGRATION=1 to run against a real RabbitMQ broker.');
    }

    public function testDeclarePublishGetAckAndNackRedelivery(): void
    {
        $error = null;
        Coroutine\run(function () use (&$error): void {
            $client = null; $channel = null;
            $suffix = bin2hex(random_bytes(8)); $exchange = 'easyswoole.amqp.test.' . $suffix; $queue = $exchange . '.queue';
            try {
                $client = new AMQP($this->config()); $channel = $client->channel();
                $channel->exchangeDeclare($exchange, 'direct', durable: false, autoDelete: true);
                $declared = $channel->queueDeclare($queue, durable: false, exclusive: true, autoDelete: true);
                self::assertSame($queue, $declared['queue']);
                $channel->queueBind($queue, $exchange, 'created');

                $body = json_encode(['id'=>$suffix], JSON_THROW_ON_ERROR);
                $channel->publishConfirmed($body, $exchange, 'created', [
                    'content_type'=>'application/json', 'delivery_mode'=>1,
                    'headers'=>['source'=>'phpunit', 'nested'=>['enabled'=>true]],
                ]);
                $message = $this->waitForMessage($channel, $queue);
                self::assertSame($body, $message->body);
                self::assertSame('application/json', $message->properties['content_type']);
                // AMQP field tables are unordered; RabbitMQ may normalize key order.
                self::assertEquals(['source'=>'phpunit', 'nested'=>['enabled'=>true]], $message->properties['headers']);

                $channel->nack($message->deliveryTag, requeue: true);
                $redelivered = $this->waitForMessage($channel, $queue);
                self::assertSame($body, $redelivered->body); self::assertTrue($redelivered->redelivered);
                $channel->ack($redelivered->deliveryTag);
                self::assertNull($channel->get($queue));
            } catch (\Throwable $throwable) { $error = $throwable; }
            finally {
                if ($channel instanceof Channel) {
                    try { $channel->queueDelete($queue); } catch (\Throwable) {}
                    try { $channel->exchangeDelete($exchange); } catch (\Throwable) {}
                }
                $client?->close();
            }
        });
        if ($error !== null) throw $error;
    }

    public function testHeartbeatKeepsIdleConnectionUsable(): void
    {
        $error = null;
        Coroutine\run(function () use (&$error): void {
            $client = null;
            try {
                $client = new AMQP($this->config(2));
                $client->connect();
                Coroutine::sleep(3.0);
                $channel = $client->channel();
                $declared = $channel->queueDeclare('', exclusive: true, autoDelete: true);
                self::assertNotSame('', $declared['queue']);
            } catch (\Throwable $throwable) { $error = $throwable; }
            finally { $client?->close(); }
        });
        if ($error !== null) throw $error;
    }

    public function testSharedClientSerializesConcurrentChannels(): void
    {
        $error = null;
        Coroutine\run(function () use (&$error): void {
            $client = null; $control = null; $queue = '';
            try {
                $client = new AMQP($this->config());
                $control = $client->channel();
                $queue = $control->queueDeclare('', exclusive: true, autoDelete: true)['queue'];
                $done = new Coroutine\Channel(4);
                for ($index = 0; $index < 4; $index++) {
                    Coroutine::create(function () use ($client, $queue, $index, $done): void {
                        try {
                            $channel = $client->channel();
                            $channel->publishConfirmed("concurrent-{$index}", routingKey: $queue);
                            $channel->close();
                            $done->push(null);
                        } catch (\Throwable $throwable) { $done->push($throwable); }
                    });
                }
                for ($index = 0; $index < 4; $index++) {
                    $result = $done->pop(10);
                    if ($result instanceof \Throwable) throw $result;
                }
                $bodies = [];
                for ($index = 0; $index < 4; $index++) {
                    $message = $this->waitForMessage($control, $queue);
                    $bodies[] = $message->body;
                    $control->ack($message->deliveryTag);
                }
                sort($bodies);
                self::assertSame(['concurrent-0', 'concurrent-1', 'concurrent-2', 'concurrent-3'], $bodies);
            } catch (\Throwable $throwable) { $error = $throwable; }
            finally {
                if ($control instanceof Channel && $queue !== '') try { $control->queueDelete($queue); } catch (\Throwable) {}
                $client?->close();
            }
        });
        if ($error !== null) throw $error;
    }

    public function testConsumerCallbackCanCancelAndAutoAck(): void
    {
        $error = null; $received = null;
        Coroutine\run(function () use (&$error, &$received): void {
            $client = null; $channel = null; $queue = '';
            try {
                $client = new AMQP($this->config());
                $channel = $client->channel();
                $queue = $channel->queueDeclare('', exclusive: true)['queue'];
                $channel->publishConfirmed('consumed', routingKey: $queue);
                $channel->consume($queue, function (Message $message, Channel $consumerChannel) use (&$received): bool {
                    $received = $message->body;
                    $consumerChannel->cancel((string) $message->consumerTag);
                    return true;
                });
                self::assertNull($channel->get($queue));
            } catch (\Throwable $throwable) { $error = $throwable; }
            finally {
                if ($channel instanceof Channel && $queue !== '') try { $channel->queueDelete($queue); } catch (\Throwable) {}
                $client?->close();
            }
        });
        if ($error !== null) throw $error;
        self::assertSame('consumed', $received);
    }

    public function testIdleConsumerSurvivesReadTimeoutAndCanBeCancelled(): void
    {
        $error = null; $returnedTag = null;
        Coroutine\run(function () use (&$error, &$returnedTag): void {
            $client = null; $channel = null; $queue = '';
            try {
                $client = new AMQP($this->config(readTimeout: 0.5));
                $channel = $client->channel();
                $queue = $channel->queueDeclare('', exclusive: true)['queue'];
                $done = new Coroutine\Channel(1);
                Coroutine::create(function () use ($channel, $queue, $done, &$returnedTag): void {
                    try { $returnedTag = $channel->consume($queue, static fn (): bool => true, 'idle-consumer'); $done->push(null); }
                    catch (\Throwable $throwable) { $done->push($throwable); }
                });
                Coroutine::sleep(1.2);
                $channel->cancel('idle-consumer');
                $result = $done->pop(3);
                if ($result instanceof \Throwable) throw $result;
                if ($result === false) throw new \RuntimeException('Idle consumer did not stop after cancel.');
            } catch (\Throwable $throwable) { $error = $throwable; }
            finally {
                if ($channel instanceof Channel && $queue !== '') try { $channel->queueDelete($queue); } catch (\Throwable) {}
                $client?->close();
            }
        });
        if ($error !== null) throw $error;
        self::assertSame('idle-consumer', $returnedTag);
    }

    public function testMandatoryPublishReturnsUnroutableMessageAndKeepsChannelUsable(): void
    {
        $error = null;
        Coroutine\run(function () use (&$error): void {
            $client = null; $channel = null;
            try {
                $client = new AMQP($this->config());
                $channel = $client->channel();
                $routingKey = 'easyswoole.amqp.missing.' . bin2hex(random_bytes(8));
                try {
                    $channel->publishConfirmed('unroutable', '', $routingKey, ['content_type'=>'text/plain'], true);
                    self::fail('RabbitMQ should return a mandatory unroutable message.');
                } catch (MessageReturnedException $returned) {
                    self::assertSame(312, $returned->returnedMessage->replyCode);
                    self::assertSame('NO_ROUTE', $returned->returnedMessage->replyText);
                    self::assertSame($routingKey, $returned->returnedMessage->routingKey);
                    self::assertSame('unroutable', $returned->returnedMessage->message->body);
                    self::assertSame('text/plain', $returned->returnedMessage->message->properties['content_type']);
                }
                self::assertNotSame('', $channel->queueDeclare('', exclusive: true, autoDelete: true)['queue']);
            } catch (\Throwable $throwable) { $error = $throwable; }
            finally { $client?->close(); }
        });
        if ($error !== null) throw $error;
    }

    public function testTransactionCommitAndRollback(): void
    {
        $error = null;
        Coroutine\run(function () use (&$error): void {
            $client = null; $channel = null; $queue = '';
            try {
                $client = new AMQP($this->config());
                $channel = $client->channel();
                $queue = $channel->queueDeclare('', exclusive: true, autoDelete: true)['queue'];
                $channel->txSelect();
                $channel->publish('committed', routingKey: $queue);
                $channel->txCommit();
                $committed = $this->waitForMessage($channel, $queue);
                self::assertSame('committed', $committed->body);
                $channel->ack($committed->deliveryTag);
                $channel->txCommit();

                $channel->publish('rolled-back', routingKey: $queue);
                $channel->txRollback();
                self::assertNull($channel->get($queue));
            } catch (\Throwable $throwable) { $error = $throwable; }
            finally {
                if ($channel instanceof Channel && $queue !== '') try { $channel->queueDelete($queue); } catch (\Throwable) {}
                $client?->close();
            }
        });
        if ($error !== null) throw $error;
    }

    private function waitForMessage(Channel $channel, string $queue): Message
    {
        $deadline = microtime(true) + 3;
        do { $message = $channel->get($queue); if ($message !== null) return $message; Coroutine::sleep(0.01); } while (microtime(true) < $deadline);
        self::fail('RabbitMQ did not deliver the published message within 3 seconds.');
    }

    private function config(?int $heartbeat = null, float $readTimeout = 5): Config
    {
        return new Config(
            host: $this->env('AMQP_HOST', '127.0.0.1'), port: (int) $this->env('AMQP_PORT', '5672'),
            user: $this->env('AMQP_USER', 'guest'), password: $this->env('AMQP_PASSWORD', 'guest'),
            vhost: $this->env('AMQP_VHOST', '/'), connectTimeout: 5, readTimeout: $readTimeout, writeTimeout: 5,
            heartbeat: $heartbeat ?? (int) $this->env('AMQP_HEARTBEAT', '0'),
        );
    }
    private function env(string $name, string $default): string { $value = getenv($name); return $value === false ? $default : $value; }
}
