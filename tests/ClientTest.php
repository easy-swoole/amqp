<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Tests;

use EasySwoole\AMQP\AMQP;
use EasySwoole\AMQP\Config;
use EasySwoole\AMQP\Connection;
use EasySwoole\AMQP\Message;
use EasySwoole\AMQP\Protocol;
use EasySwoole\AMQP\Exception\MessageReturnedException;
use EasySwoole\AMQP\Exception\ProtocolException;
use EasySwoole\AMQP\Exception\TimeoutException;
use EasySwoole\AMQP\ReturnedMessage;
use EasySwoole\AMQP\Tests\Support\MemoryTransport;
use EasySwoole\AMQP\Tests\Support\BlockingTransport;
use EasySwoole\AMQP\Tests\Support\ServerFrames;
use EasySwoole\AMQP\Tests\Support\TimeoutTransport;
use EasySwoole\AMQP\Transport\SwooleSocketTransport;
use EasySwoole\AMQP\Wire\Frame;
use EasySwoole\AMQP\Wire\Reader;
use EasySwoole\AMQP\Wire\Writer;
use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;
use Swoole\Timer;

final class ClientTest extends TestCase
{
    public function testHandshakeAndQueueDeclaration(): void
    {
        $declared = (new Writer())->shortstr('jobs')->long(4)->long(2)->bytes();
        $transport = new MemoryTransport(ServerFrames::handshake() . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes()) . ServerFrames::method(1, 50, 11, $declared));
        $client = new AMQP(new Config(), $transport); $channel = $client->channel();
        self::assertSame(['queue'=>'jobs','messageCount'=>4,'consumerCount'=>2], $channel->queueDeclare('jobs', durable: true));
        self::assertTrue($client->isConnected()); self::assertStringStartsWith("AMQP\x00\x00\x09\x01", $transport->written);
        $methods = self::writtenFrames($transport->written);
        self::assertSame([[10,11],[10,31],[10,40],[20,10],[50,10]], array_map(fn (Frame $f) => array_slice($f->methodParts(), 0, 2), $methods));
    }
    public function testExternalAuthenticationUsesEmptySaslResponse(): void
    {
        $transport = new MemoryTransport(ServerFrames::handshake(mechanisms: 'PLAIN EXTERNAL'));
        $client = new AMQP(new Config(
            tls: new \EasySwoole\AMQP\TlsConfig(clientCertificate: '/cert/client.pem', clientPrivateKey: '/cert/key.pem'),
            authMechanism: Config::AUTH_EXTERNAL,
        ), $transport);
        $client->connect();
        $startOk = self::writtenFrames($transport->written)[0];
        [, , $reader] = $startOk->methodParts();
        $reader->table();
        self::assertSame('EXTERNAL', $reader->shortstr());
        self::assertSame('', $reader->longstr());
        self::assertTrue($client->isConnected());
    }
    public function testUnsupportedConfiguredAuthenticationMechanismAbortsHandshake(): void
    {
        $transport = new MemoryTransport(ServerFrames::handshake(mechanisms: 'PLAIN'));
        $client = new AMQP(new Config(
            tls: new \EasySwoole\AMQP\TlsConfig(clientCertificate: '/cert/client.pem', clientPrivateKey: '/cert/key.pem'),
            authMechanism: Config::AUTH_EXTERNAL,
        ), $transport);
        try { $client->connect(); self::fail('Expected unsupported authentication mechanism.'); }
        catch (ProtocolException $error) { self::assertStringContainsString('EXTERNAL', $error->getMessage()); }
        self::assertFalse($client->isConnected());
    }
    public function testChannelFlowRequestAndServerFlowNotification(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 20, 21, (new Writer())->bit(false)->bytes())
            . ServerFrames::method(1, 20, 20, (new Writer())->bit(false)->bytes())
            . ServerFrames::method(1, 50, 11, (new Writer())->shortstr('jobs')->long(0)->long(0)->bytes());
        $transport = new MemoryTransport($incoming);
        $channel = (new AMQP(new Config(), $transport))->channel();
        self::assertFalse($channel->flow(false));
        self::assertSame('jobs', $channel->queueDeclare('jobs')['queue']);
        self::assertFalse($channel->isPublishingActive());
        try { $channel->publish('paused'); self::fail('Expected publishing pause.'); }
        catch (\LogicException $error) { self::assertStringContainsString('channel.flow', $error->getMessage()); }
        $flowMethods = array_filter(self::writtenFrames($transport->written), static function (Frame $frame): bool {
            return $frame->type === Frame::METHOD && array_slice($frame->methodParts(), 0, 2) === [20, 21];
        });
        self::assertCount(1, $flowMethods);
    }
    public function testFlowForAnotherChannelIsAppliedBeforeItsNextPublish(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(2, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(2, 20, 20, (new Writer())->bit(false)->bytes())
            . ServerFrames::method(1, 50, 11, (new Writer())->shortstr('jobs')->long(0)->long(0)->bytes());
        $transport = new MemoryTransport($incoming);
        $client = new AMQP(new Config(), $transport);
        $first = $client->channel(1);
        $second = $client->channel(2);
        self::assertSame('jobs', $first->queueDeclare('jobs')['queue']);
        self::assertFalse($second->isPublishingActive());
        $writtenBefore = strlen($transport->written);
        try { $second->publish('must-not-be-written'); self::fail('Expected cross-channel publishing pause.'); }
        catch (\LogicException) {}
        self::assertSame($writtenBefore, strlen($transport->written));
    }
    public function testCloseForAnotherChannelIsAcknowledgedAndInvalidatedImmediately(): void
    {
        $close = (new Writer())->short(404)->shortstr('gone')->short(60)->short(40)->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(2, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(2, 20, 40, $close)
            . ServerFrames::method(1, 50, 11, (new Writer())->shortstr('jobs')->long(0)->long(0)->bytes())
            . ServerFrames::method(2, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(2, 50, 11, (new Writer())->shortstr('replacement')->long(0)->long(0)->bytes());
        $transport = new MemoryTransport($incoming);
        $client = new AMQP(new Config(), $transport);
        $first = $client->channel(1);
        $second = $client->channel(2);
        self::assertSame('jobs', $first->queueDeclare('jobs')['queue']);
        $replacement = $client->channel(2);
        $writtenBefore = strlen($transport->written);
        try { $second->publish('must-not-be-written'); self::fail('Expected asynchronous channel.close.'); }
        catch (ProtocolException $error) {
            self::assertSame(404, $error->replyCode);
            self::assertSame(60, $error->classId);
            self::assertSame(40, $error->methodId);
        }
        self::assertSame($writtenBefore, strlen($transport->written));
        self::assertSame('replacement', $replacement->queueDeclare('replacement')['queue']);
        $methods = array_map(
            static fn (Frame $frame): array => array_slice($frame->methodParts(), 0, 2),
            array_values(array_filter(self::writtenFrames($transport->written), static fn (Frame $frame): bool => $frame->type === Frame::METHOD)),
        );
        self::assertContains([20, 41], $methods);
    }
    public function testTransactionsSelectCommitAndRollback(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 90, 11)
            . ServerFrames::method(1, 90, 21)
            . ServerFrames::method(1, 90, 31);
        $transport = new MemoryTransport($incoming);
        $channel = (new AMQP(new Config(), $transport))->channel();
        $channel->txSelect();
        $channel->txSelect(); // idempotent; must not issue a second tx.select
        $channel->publish('transactional');
        $channel->txCommit();
        $channel->txRollback();
        $txMethods = array_values(array_filter(array_map(
            static fn (Frame $frame): ?array => $frame->type === Frame::METHOD ? array_slice($frame->methodParts(), 0, 2) : null,
            self::writtenFrames($transport->written),
        ), static fn (?array $method): bool => $method !== null && $method[0] === 90));
        self::assertSame([[90,10],[90,20],[90,30]], $txMethods);
    }
    public function testTransactionsAndPublisherConfirmsAreMutuallyExclusive(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 90, 11);
        $channel = (new AMQP(new Config(), new MemoryTransport($incoming)))->channel();
        $channel->txSelect();
        $this->expectException(\LogicException::class);
        $channel->enablePublisherConfirms();
    }
    public function testTransactionCommitRequiresTransactionMode(): void
    {
        $channel = (new AMQP(new Config(), new MemoryTransport(ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes()))))->channel();
        $this->expectException(\LogicException::class);
        $channel->txCommit();
    }
    public function testAdditionalExchangeQueueAndRecoverMethods(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 40, 31)
            . ServerFrames::method(1, 40, 51)
            . ServerFrames::method(1, 50, 51)
            . ServerFrames::method(1, 50, 31, (new Writer())->long(7)->bytes())
            . ServerFrames::method(1, 60, 111);
        $transport = new MemoryTransport($incoming);
        $channel = (new AMQP(new Config(), $transport))->channel();
        $channel->exchangeBind('destination', 'source', 'route', ['x'=>1]);
        $channel->exchangeUnbind('destination', 'source', 'route', ['x'=>1]);
        $channel->queueUnbind('jobs', 'events', 'route', ['x'=>1]);
        self::assertSame(7, $channel->queuePurge('jobs'));
        $channel->recover();
        $channel->recoverAsync(false);
        $methods = array_map(
            fn (Frame $frame) => array_slice($frame->methodParts(), 0, 2),
            array_values(array_filter(self::writtenFrames($transport->written), fn (Frame $frame) => $frame->type === Frame::METHOD)),
        );
        self::assertSame([[40,30],[40,40],[50,50],[50,30],[60,110],[60,100]], array_slice($methods, -6));
    }
    public function testQosRejectsValuesThatWouldOverflowWireFields(): void
    {
        $channel = (new AMQP(new Config(), new MemoryTransport(ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes()))))->channel();
        $this->expectException(\InvalidArgumentException::class);
        $channel->qos(1, 0x100000000);
    }
    public function testHandshakeRejectsServerFrameMaxBelowProtocolMinimum(): void
    {
        $transport = new MemoryTransport(ServerFrames::handshake(frameMax: 1));
        $client = new AMQP(new Config(), $transport);
        try { $client->connect(); self::fail('Expected invalid server frame-max.'); }
        catch (ProtocolException $error) { self::assertStringContainsString('below 4096', $error->getMessage()); }
        self::assertFalse($client->isConnected());
    }
    public function testPublishFramesBodyAndProperties(): void
    {
        $transport = new MemoryTransport(ServerFrames::handshake(4096) . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes()));
        $channel = (new AMQP(new Config(frameMax: 4096), $transport))->channel();
        $channel->publish('hello', 'events', 'created', ['content_type'=>'application/json','delivery_mode'=>2]);
        $frames = self::writtenFrames($transport->written); $tail = array_slice($frames, -3);
        self::assertSame([Frame::METHOD, Frame::HEADER, Frame::BODY], array_column($tail, 'type'));
        self::assertSame('hello', $tail[2]->payload);
        $method = new Reader($tail[0]->payload); self::assertSame(60, $method->short()); self::assertSame(40, $method->short()); $method->short();
        self::assertSame('events', $method->shortstr()); self::assertSame('created', $method->shortstr());
    }
    public function testLargeBodyIsSplitAtNegotiatedFrameLimit(): void
    {
        $transport = new MemoryTransport(ServerFrames::handshake(4096) . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes()));
        $channel = (new AMQP(new Config(frameMax: 4096), $transport))->channel();
        $channel->publish(str_repeat('x', 9000));
        $bodyFrames = array_values(array_filter(self::writtenFrames($transport->written), fn (Frame $frame) => $frame->type === Frame::BODY));
        self::assertSame([4088, 4088, 824], array_map(fn (Frame $frame) => strlen($frame->payload), $bodyFrames));
    }
    public function testOversizedOutboundMethodFrameAbortsConnectionBeforeWrite(): void
    {
        $transport = new MemoryTransport(ServerFrames::handshake(4096)
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes()));
        $client = new AMQP(new Config(frameMax: 4096), $transport);
        $channel = $client->channel();
        $writtenBefore = strlen($transport->written);
        try { $channel->exchangeDeclare('events', arguments: ['large'=>str_repeat('x', 5000)]); self::fail('Expected frame-max rejection.'); }
        catch (ProtocolException $error) { self::assertStringContainsString('frame-max', $error->getMessage()); }
        self::assertSame($writtenBefore, strlen($transport->written));
        self::assertFalse($client->isConnected());
    }
    public function testEmptyGetReturnsNull(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 72, (new Writer())->shortstr('')->bytes());
        $channel = (new AMQP(new Config(), new MemoryTransport($incoming)))->channel();
        self::assertNull($channel->get('empty'));
    }
    public function testBasicGetBuildsMessageAndAck(): void
    {
        $get = (new Writer())->longlong(42)->bit(false)->shortstr('')->shortstr('jobs')->long(0)->bytes();
        $header = pack('n2', 60, 0) . (new Writer())->longlong(4)->raw(pack('n', 1 << 15))->shortstr('text/plain')->bytes();
        $incoming = ServerFrames::handshake() . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 71, $get) . (new Frame(Frame::HEADER, 1, $header))->encode() . (new Frame(Frame::BODY, 1, 'test'))->encode();
        $transport = new MemoryTransport($incoming); $channel = (new AMQP(new Config(), $transport))->channel(); $message = $channel->get('jobs');
        self::assertNotNull($message); self::assertSame('test', $message->body); self::assertSame(42, $message->deliveryTag); self::assertSame('text/plain', $message->properties['content_type']);
        $channel->ack($message->deliveryTag); $frames = self::writtenFrames($transport->written); self::assertSame([60,80], array_slice(end($frames)->methodParts(), 0, 2));
    }
    public function testAcknowledgementMethodsRejectInvalidDeliveryTags(): void
    {
        $channel = (new AMQP(new Config(), new MemoryTransport(ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes()))))->channel();
        foreach ([
            static fn () => $channel->ack(0),
            static fn () => $channel->nack(-1, true),
            static fn () => $channel->reject(0),
        ] as $operation) {
            try { $operation(); self::fail('Expected invalid delivery-tag.'); }
            catch (\InvalidArgumentException) {}
        }
        $channel->ack(0, true);
        self::assertTrue(true);
    }
    public function testOversizedInboundMessageAbortsConnection(): void
    {
        $get = (new Writer())->longlong(1)->bit(false)->shortstr('')->shortstr('jobs')->long(0)->bytes();
        $header = pack('n2', 60, 0) . (new Writer())->longlong(5)->raw(pack('n', 0))->bytes();
        $incoming = ServerFrames::handshake() . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 71, $get) . (new Frame(Frame::HEADER, 1, $header))->encode();
        $client = new AMQP(new Config(maxMessageSize: 4), new MemoryTransport($incoming));
        $channel = $client->channel();
        try { $channel->get('jobs'); self::fail('Expected oversized message rejection.'); }
        catch (ProtocolException $error) { self::assertStringContainsString('maximum size', $error->getMessage()); }
        self::assertFalse($client->isConnected());
    }
    public function testPublisherNackIsReported(): void
    {
        $declared = (new Writer())->shortstr('still-open')->long(0)->long(0)->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 85, 11)
            . ServerFrames::method(1, 60, 120, (new Writer())->longlong(1)->bit(false)->bit(false)->bytes())
            . ServerFrames::method(1, 50, 11, $declared);
        $client = new AMQP(new Config(), new MemoryTransport($incoming));
        $channel = $client->channel();
        try { $channel->publishConfirmed('body'); self::fail('Expected publisher NACK.'); }
        catch (ProtocolException $error) { self::assertStringContainsString('negatively acknowledged', $error->getMessage()); }
        self::assertTrue($client->isConnected());
        self::assertSame('still-open', $channel->queueDeclare('still-open')['queue']);
    }
    public function testMalformedMethodPayloadAbortsConnection(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 71, '');
        $client = new AMQP(new Config(), new MemoryTransport($incoming));
        $channel = $client->channel();
        try { $channel->get('jobs'); self::fail('Expected truncated method payload.'); }
        catch (ProtocolException $error) { self::assertStringContainsString('Truncated', $error->getMessage()); }
        self::assertFalse($client->isConnected());
    }
    public function testMandatoryConfirmedPublishReportsReturnedMessageAfterAck(): void
    {
        $returned = (new Writer())->short(312)->shortstr('NO_ROUTE')->shortstr('events')->shortstr('missing')->bytes();
        $header = pack('n2', 60, 0) . (new Writer())->longlong(4)->raw(pack('n', 1 << 15))->shortstr('text/plain')->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 85, 11)
            . ServerFrames::method(1, 60, 50, $returned)
            . (new Frame(Frame::HEADER, 1, $header))->encode()
            . (new Frame(Frame::BODY, 1, 'lost'))->encode()
            . ServerFrames::method(1, 60, 80, (new Writer())->longlong(1)->bit(false)->bytes());
        $channel = (new AMQP(new Config(), new MemoryTransport($incoming)))->channel();
        try { $channel->publishConfirmed('lost', 'events', 'missing', ['content_type'=>'text/plain'], true); self::fail('Expected returned message.'); }
        catch (MessageReturnedException $error) {
            self::assertSame(312, $error->getCode());
            self::assertSame('NO_ROUTE', $error->returnedMessage->replyText);
            self::assertSame('events', $error->returnedMessage->exchange);
            self::assertSame('missing', $error->returnedMessage->routingKey);
            self::assertSame('lost', $error->returnedMessage->message->body);
            self::assertSame('text/plain', $error->returnedMessage->message->properties['content_type']);
        }
        self::assertNull($channel->popReturnedMessage());
    }
    public function testReturnHandlerRunsOutsideIoOperationAndCanUseChannel(): void
    {
        $returned = (new Writer())->short(312)->shortstr('NO_ROUTE')->shortstr('events')->shortstr('missing')->bytes();
        $header = pack('n2', 60, 0) . (new Writer())->longlong(4)->raw(pack('n', 0))->bytes();
        $queue = (new Writer())->shortstr('next')->long(0)->long(0)->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 50, $returned)
            . (new Frame(Frame::HEADER, 1, $header))->encode()
            . (new Frame(Frame::BODY, 1, 'lost'))->encode()
            . ServerFrames::method(1, 50, 11, $queue)
            . ServerFrames::method(1, 50, 11, $queue);
        $channel = (new AMQP(new Config(), new MemoryTransport($incoming)))->channel();
        $handled = null;
        $channel->setReturnHandler(function (ReturnedMessage $returned, $sameChannel) use (&$handled, $channel): void {
            self::assertSame($channel, $sameChannel);
            $handled = $returned->message->body;
            self::assertSame('next', $sameChannel->queueDeclare('next')['queue']);
        });
        $channel->publish('lost', 'events', 'missing', mandatory: true);
        self::assertSame('next', $channel->queueDeclare('next')['queue']);
        self::assertSame('lost', $handled);
        self::assertNull($channel->popReturnedMessage());
    }
    public function testEarlierMandatoryReturnIsNotAttributedToNextConfirmedPublish(): void
    {
        $returned = (new Writer())->short(312)->shortstr('NO_ROUTE')->shortstr('events')->shortstr('old')->bytes();
        $header = pack('n2', 60, 0) . (new Writer())->longlong(3)->raw(pack('n', 0))->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 85, 11)
            . ServerFrames::method(1, 60, 50, $returned)
            . (new Frame(Frame::HEADER, 1, $header))->encode()
            . (new Frame(Frame::BODY, 1, 'old'))->encode()
            . ServerFrames::method(1, 60, 80, (new Writer())->longlong(1)->bit(false)->bytes())
            . ServerFrames::method(1, 60, 80, (new Writer())->longlong(2)->bit(false)->bytes());
        $channel = (new AMQP(new Config(), new MemoryTransport($incoming)))->channel();
        $channel->enablePublisherConfirms();
        $channel->publish('old', 'events', 'old', mandatory: true);
        $channel->publishConfirmed('new', 'events', 'valid', mandatory: true);
        self::assertSame('old', $channel->popReturnedMessage()?->message->body);
    }
    public function testMultipleConfirmationDrainsEarlierPlainPublishes(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 85, 11)
            . ServerFrames::method(1, 60, 80, (new Writer())->longlong(2)->bit(true)->bytes())
            . ServerFrames::method(1, 60, 80, (new Writer())->longlong(3)->bit(false)->bytes());
        $channel = (new AMQP(new Config(), new MemoryTransport($incoming)))->channel();
        $channel->enablePublisherConfirms();
        $channel->publish('one');
        $channel->publish('two');
        $channel->publishConfirmed('three');
        self::assertNull($channel->popReturnedMessage());
    }
    public function testPublisherBatchReportsAckNackAndReturnedMessages(): void
    {
        $returned = (new Writer())->short(312)->shortstr('NO_ROUTE')->shortstr('events')->shortstr('missing')->bytes();
        $header = pack('n2', 60, 0) . (new Writer())->longlong(3)->raw(pack('n', 0))->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 85, 11)
            . ServerFrames::method(1, 60, 50, $returned)
            . (new Frame(Frame::HEADER, 1, $header))->encode()
            . (new Frame(Frame::BODY, 1, 'two'))->encode()
            . ServerFrames::method(1, 60, 80, (new Writer())->longlong(2)->bit(true)->bytes())
            . ServerFrames::method(1, 60, 120, (new Writer())->longlong(3)->bit(false)->bit(false)->bytes());
        $channel = (new AMQP(new Config(), new MemoryTransport($incoming)))->channel();
        $batch = $channel->publisherBatch()
            ->publish('one')
            ->publish('two', 'events', 'missing', mandatory: true)
            ->publish('three');
        $result = $batch->wait();
        self::assertSame([1, 2], $result->acked);
        self::assertSame([3], $result->nacked);
        self::assertSame('two', $result->returned[0]->message->body);
        self::assertFalse($result->allAcknowledged());
        self::assertNull($channel->popReturnedMessage());
        self::assertSame([], self::privateValue($batch, 'messages'));
        self::assertNull(self::privateValue($batch, 'waiter'));
    }
    public function testPublisherBatchCannotBeEmptyOrReused(): void
    {
        $channel = (new AMQP(
            new Config(),
            new MemoryTransport(ServerFrames::handshake() . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())),
        ))->channel();
        $batch = $channel->publisherBatch();
        try { $batch->wait(); self::fail('Expected empty batch rejection.'); }
        catch (\LogicException) {}
        $this->expectException(\LogicException::class);
        $batch->wait();
    }
    public function testRpcSkipsAsynchronousPublisherConfirmation(): void
    {
        $declared = (new Writer())->shortstr('jobs')->long(0)->long(0)->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 85, 11)
            . ServerFrames::method(1, 60, 80, (new Writer())->longlong(1)->bit(false)->bytes())
            . ServerFrames::method(1, 50, 11, $declared);
        $channel = (new AMQP(new Config(), new MemoryTransport($incoming)))->channel();
        $channel->enablePublisherConfirms();
        $channel->publish('one');
        self::assertSame('jobs', $channel->queueDeclare('jobs')['queue']);
    }
    public function testRpcConsumesItsResponseBeforeReportingInterleavedPublisherNack(): void
    {
        $first = (new Writer())->shortstr('first')->long(0)->long(0)->bytes();
        $second = (new Writer())->shortstr('second')->long(0)->long(0)->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 85, 11)
            . ServerFrames::method(1, 60, 120, (new Writer())->longlong(1)->bit(false)->bit(false)->bytes())
            . ServerFrames::method(1, 50, 11, $first)
            . ServerFrames::method(1, 50, 11, $second);
        $client = new AMQP(new Config(), new MemoryTransport($incoming));
        $channel = $client->channel();
        $channel->enablePublisherConfirms();
        $channel->publish('plain');
        try { $channel->queueDeclare('first'); self::fail('Expected interleaved publisher NACK.'); }
        catch (ProtocolException $error) { self::assertStringContainsString('negatively acknowledged', $error->getMessage()); }
        self::assertTrue($client->isConnected());
        self::assertSame('second', $channel->queueDeclare('second')['queue']);
    }
    public function testUnknownPublisherConfirmationTagAbortsConnection(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 85, 11)
            . ServerFrames::method(1, 60, 80, (new Writer())->longlong(2)->bit(false)->bytes());
        $client = new AMQP(new Config(), new MemoryTransport($incoming));
        $channel = $client->channel();
        try { $channel->publishConfirmed('one'); self::fail('Expected invalid confirmation tag.'); }
        catch (ProtocolException $error) { self::assertStringContainsString('unknown delivery tag', $error->getMessage()); }
        self::assertFalse($client->isConnected());
    }
    public function testGetOperationDeadlineCancelsBlockedRead(): void
    {
        $elapsed = 0.0;
        Coroutine\run(function () use (&$elapsed): void {
            $incoming = ServerFrames::handshake()
                . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes());
            $client = new AMQP(new Config(readTimeout: 10), new BlockingTransport($incoming));
            $channel = $client->channel();
            $started = microtime(true);
            try { $channel->get('jobs', timeout: 0.05); self::fail('Expected operation deadline.'); }
            catch (TimeoutException $error) { self::assertStringContainsString('deadline', $error->getMessage()); }
            $elapsed = microtime(true) - $started;
            self::assertFalse($client->isConnected());
            $client->close();
        });
        self::assertLessThan(0.5, $elapsed);
    }
    public function testConfirmedPublishDeadlineIncludesWaitingForBrokerAck(): void
    {
        Coroutine\run(function (): void {
            $incoming = ServerFrames::handshake()
                . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
                . ServerFrames::method(1, 85, 11);
            $client = new AMQP(new Config(readTimeout: 10), new BlockingTransport($incoming));
            $channel = $client->channel();
            try { $channel->publishConfirmed(body: 'waiting', timeout: 0.05); self::fail('Expected publisher confirmation deadline.'); }
            catch (TimeoutException) { self::assertFalse($client->isConnected()); }
            $client->close();
        });
    }
    public function testOperationDeadlineMustBePositiveOrMinusOne(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes());
        $channel = (new AMQP(new Config(), new MemoryTransport($incoming)))->channel();
        $this->expectException(\InvalidArgumentException::class);
        $channel->get('jobs', timeout: 0);
    }
    public function testExplicitDeadlineOverridesShorterQueueWriteTimeout(): void
    {
        $completed = false;
        Coroutine\run(function () use (&$completed): void {
            $connection = new Connection(
                new Config(connectTimeout: 0.05, readTimeout: 0.05, writeTimeout: 0.05),
                new MemoryTransport(),
            );
            $started = new Coroutine\Channel(1);
            Coroutine::create(function () use ($connection, $started): void {
                $connection->execute(function () use ($started): void { $started->push(true); Coroutine::sleep(0.1); }, 0.2);
            });
            $started->pop(0.2);
            $completed = $connection->execute(static fn (): bool => true, 0.2);
            $connection->close();
        });
        self::assertTrue($completed);
    }
    public function testPublicBlockingOperationsDefaultToThreeSecondDeadline(): void
    {
        $get = new \ReflectionMethod(\EasySwoole\AMQP\Channel::class, 'get');
        $confirmed = new \ReflectionMethod(\EasySwoole\AMQP\Channel::class, 'publishConfirmed');
        self::assertSame(3.0, $get->getParameters()[2]->getDefaultValue());
        self::assertSame('float', (string) $get->getParameters()[2]->getType());
        self::assertSame(3.0, $confirmed->getParameters()[5]->getDefaultValue());
        self::assertSame('float', (string) $confirmed->getParameters()[5]->getType());
    }
    public function testInvalidBasicPropertyContinuationFlagAbortsConnection(): void
    {
        $get = (new Writer())->longlong(1)->bit(false)->shortstr('')->shortstr('jobs')->long(0)->bytes();
        $header = pack('n2', 60, 0) . (new Writer())->longlong(0)->raw(pack('n', 1))->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 71, $get)
            . (new Frame(Frame::HEADER, 1, $header))->encode();
        $client = new AMQP(new Config(), new MemoryTransport($incoming));
        try { $client->channel()->get('jobs'); self::fail('Expected invalid property flags.'); }
        catch (ProtocolException $error) { self::assertStringContainsString('continuation flag', $error->getMessage()); }
        self::assertFalse($client->isConnected());
    }
    public function testConcurrentConfirmedPublishesCannotInterleaveSendAndWait(): void
    {
        $errors = [];
        Coroutine\run(function () use (&$errors): void {
            $incoming = ServerFrames::handshake()
                . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
                . ServerFrames::method(1, 85, 11)
                . ServerFrames::method(1, 60, 80, (new Writer())->longlong(1)->bit(false)->bytes())
                . ServerFrames::method(1, 60, 80, (new Writer())->longlong(2)->bit(false)->bytes());
            $client = new AMQP(new Config(), new MemoryTransport($incoming));
            $channel = $client->channel();
            $done = new Coroutine\Channel(2);
            foreach (['one', 'two'] as $body) Coroutine::create(function () use ($channel, $body, $done): void {
                try { $channel->publishConfirmed($body); $done->push(null); }
                catch (\Throwable $error) { $done->push($error); }
            });
            for ($index = 0; $index < 2; $index++) {
                $result = $done->pop(2);
                if ($result instanceof \Throwable) $errors[] = $result;
            }
            $client->close();
        });
        self::assertSame([], $errors);
    }
    public function testDeliveryInterleavedWithPublisherConfirmIsBufferedForConsumer(): void
    {
        $consumeOk = (new Writer())->shortstr('consumer')->bytes();
        $delivery = static fn (int $tag): string => ServerFrames::method(1, 60, 60,
            (new Writer())->shortstr('consumer')->longlong($tag)->bit(false)->shortstr('')->shortstr('jobs')->bytes()
        );
        $content = static fn (string $body): string => (new Frame(Frame::HEADER, 1,
            pack('n2', 60, 0) . (new Writer())->longlong(strlen($body))->raw(pack('n', 0))->bytes()
        ))->encode() . (new Frame(Frame::BODY, 1, $body))->encode();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 21, $consumeOk)
            . $delivery(1) . $content('first')
            . ServerFrames::method(1, 85, 11)
            . $delivery(2) . $content('second')
            . ServerFrames::method(1, 60, 80, (new Writer())->longlong(1)->bit(false)->bytes())
            . ServerFrames::method(1, 60, 31, (new Writer())->shortstr('consumer')->bytes());
        $channel = (new AMQP(new Config(), new MemoryTransport($incoming)))->channel();
        $received = [];
        $channel->consume('jobs', function (Message $message, $sameChannel) use (&$received): void {
            $received[] = $message->body;
            if ($message->body === 'first') $sameChannel->publishConfirmed('reply');
            else $sameChannel->cancel('consumer');
        }, 'consumer', autoAck: true);
        self::assertSame(['first', 'second'], $received);
    }
    public function testServerCancelInterleavedWithPublisherConfirmStopsConsumerCleanly(): void
    {
        $delivery = (new Writer())->shortstr('consumer')->longlong(1)->bit(false)->shortstr('')->shortstr('jobs')->bytes();
        $header = pack('n2', 60, 0) . (new Writer())->longlong(5)->raw(pack('n', 0))->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('consumer')->bytes())
            . ServerFrames::method(1, 60, 60, $delivery)
            . (new Frame(Frame::HEADER, 1, $header))->encode()
            . (new Frame(Frame::BODY, 1, 'first'))->encode()
            . ServerFrames::method(1, 85, 11)
            . ServerFrames::method(1, 60, 30, (new Writer())->shortstr('consumer')->bit(false)->bytes())
            . ServerFrames::method(1, 60, 80, (new Writer())->longlong(1)->bit(false)->bytes());
        $transport = new MemoryTransport($incoming);
        $channel = (new AMQP(new Config(), $transport))->channel();
        $received = [];
        $channel->consume('jobs', function (Message $message, $sameChannel) use (&$received): void {
            $received[] = $message->body;
            $sameChannel->publishConfirmed('reply');
        }, 'consumer', autoAck: true);
        self::assertSame(['first'], $received);
        $methods = array_map(
            fn (Frame $frame) => array_slice($frame->methodParts(), 0, 2),
            array_values(array_filter(self::writtenFrames($transport->written), fn (Frame $frame) => $frame->type === Frame::METHOD)),
        );
        self::assertContains([60, 31], $methods);
    }
    public function testServerCancelWithNoWaitDoesNotSendCancelOk(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('consumer')->bytes())
            . ServerFrames::method(1, 60, 30, (new Writer())->shortstr('consumer')->bit(true)->bytes());
        $transport = new MemoryTransport($incoming);
        $channel = (new AMQP(new Config(), $transport))->channel();
        self::assertSame('consumer', $channel->consume('jobs', static fn (): null => null, 'consumer', autoAck: true));
        $cancelOk = array_filter(
            self::writtenFrames($transport->written),
            static fn (Frame $frame): bool => $frame->type === Frame::METHOD
                && array_slice($frame->methodParts(), 0, 2) === [Protocol::BASIC, 31],
        );
        self::assertCount(0, $cancelOk);
    }
    public function testSubscriptionCancelIsIdempotentWhileAwaitingBrokerReply(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('consumer')->bytes())
            . ServerFrames::method(1, 60, 31, (new Writer())->shortstr('consumer')->bytes());
        $transport = new MemoryTransport($incoming);
        $channel = (new AMQP(new Config(), $transport))->channel();
        $subscription = $channel->subscribe('jobs', static fn (): null => null, 'consumer');
        $subscription->cancel();
        $subscription->cancel();
        $channel->wait();
        $cancels = array_filter(
            self::writtenFrames($transport->written),
            static fn (Frame $frame): bool => $frame->type === Frame::METHOD
                && array_slice($frame->methodParts(), 0, 2) === [Protocol::BASIC, 30],
        );
        self::assertCount(1, $cancels);
        self::assertFalse($subscription->isActive());
    }
    public function testHeartbeatYieldsConsumerReadWithoutClosingConnection(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('consumer')->bytes())
            . (new Frame(Frame::HEARTBEAT, 0, ''))->encode()
            . ServerFrames::method(1, 60, 31, (new Writer())->shortstr('consumer')->bytes());
        $transport = new MemoryTransport($incoming);
        $client = new AMQP(new Config(), $transport);
        $channel = $client->channel();
        self::assertSame('consumer', $channel->consume('jobs', static fn (): null => null, 'consumer'));
        self::assertTrue($client->isConnected());
        $heartbeats = array_filter(self::writtenFrames($transport->written), static fn (Frame $frame): bool => $frame->type === Frame::HEARTBEAT);
        self::assertCount(1, $heartbeats);
    }
    public function testFiniteSubscriptionWaitContinuesAfterHeartbeatYield(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('consumer')->bytes())
            . (new Frame(Frame::HEARTBEAT, 0, ''))->encode()
            . ServerFrames::method(1, 60, 31, (new Writer())->shortstr('consumer')->bytes());
        $channel = (new AMQP(new Config(), new MemoryTransport($incoming)))->channel();
        $subscription = $channel->subscribe('jobs', static fn (): null => null, 'consumer');
        $channel->wait(1.0);
        self::assertFalse($subscription->isActive());
    }
    public function testSwooleTransportSlicesIdleReadsToHeartbeatCadence(): void
    {
        $transport = new SwooleSocketTransport(new Config(readTimeout: 60));
        $effective = new \ReflectionMethod($transport, 'effectiveReadTimeout');
        self::assertSame(60.0, $effective->invoke($transport));
        $transport->setHeartbeatInterval(10);
        self::assertSame(5.0, $effective->invoke($transport));

        $unbounded = new SwooleSocketTransport(new Config(readTimeout: -1));
        $unbounded->setHeartbeatInterval(1);
        self::assertSame(0.5, $effective->invoke($unbounded));
    }
    public function testMultipleSubscriptionsAreRoutedByConsumerTag(): void
    {
        $delivery = static fn (string $consumer, int $tag): string => ServerFrames::method(1, 60, 60,
            (new Writer())->shortstr($consumer)->longlong($tag)->bit(false)->shortstr('')->shortstr('jobs')->bytes()
        );
        $content = static fn (string $body): string => (new Frame(Frame::HEADER, 1,
            pack('n2', 60, 0) . (new Writer())->longlong(strlen($body))->raw(pack('n', 0))->bytes()
        ))->encode() . (new Frame(Frame::BODY, 1, $body))->encode();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('first')->bytes())
            . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('second')->bytes())
            . $delivery('second', 1) . $content('for-second')
            . ServerFrames::method(1, 60, 31, (new Writer())->shortstr('second')->bytes())
            . $delivery('first', 2) . $content('for-first')
            . ServerFrames::method(1, 60, 31, (new Writer())->shortstr('first')->bytes());
        $transport = new MemoryTransport($incoming);
        $channel = (new AMQP(new Config(), $transport))->channel();
        $received = [];
        $first = $channel->subscribe('jobs', function (Message $message, $sameChannel, $subscription) use (&$received): void {
            $received[] = $subscription->consumerTag . ':' . $message->body;
            $subscription->cancel();
        }, 'first');
        $second = $channel->subscribe('jobs', function (Message $message, $sameChannel, $subscription) use (&$received): void {
            $received[] = $subscription->consumerTag . ':' . $message->body;
            $subscription->cancel();
        }, 'second');
        $channel->wait();
        self::assertSame(['second:for-second', 'first:for-first'], $received);
        self::assertFalse($first->isActive()); self::assertFalse($second->isActive());
        self::assertNull(self::privateValue($first, 'channel')); self::assertNull(self::privateValue($first, 'handler'));
        self::assertNull(self::privateValue($second, 'channel')); self::assertNull(self::privateValue($second, 'handler'));
        $acks = array_filter(self::writtenFrames($transport->written), fn (Frame $frame) => $frame->type === Frame::METHOD && array_slice($frame->methodParts(), 0, 2) === [60, 80]);
        self::assertCount(2, $acks);
    }
    public function testUnknownConsumerTagAbortsConnection(): void
    {
        $delivery = (new Writer())->shortstr('other')->longlong(1)->bit(false)->shortstr('')->shortstr('jobs')->bytes();
        $header = pack('n2', 60, 0) . (new Writer())->longlong(0)->raw(pack('n', 0))->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('known')->bytes())
            . ServerFrames::method(1, 60, 60, $delivery)
            . (new Frame(Frame::HEADER, 1, $header))->encode();
        $client = new AMQP(new Config(), new MemoryTransport($incoming));
        $channel = $client->channel();
        $channel->subscribe('jobs', static fn (): null => null, 'known');
        try { $channel->wait(); self::fail('Expected unknown consumer tag.'); }
        catch (ProtocolException $error) { self::assertStringContainsString('unknown tag', $error->getMessage()); }
        self::assertFalse($client->isConnected());
    }
    public function testLegacyConsumerRejectsDeliveryForAnotherConsumerTag(): void
    {
        $delivery = (new Writer())->shortstr('other')->longlong(1)->bit(false)->shortstr('')->shortstr('jobs')->bytes();
        $header = pack('n2', 60, 0) . (new Writer())->longlong(0)->raw(pack('n', 0))->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('expected')->bytes())
            . ServerFrames::method(1, 60, 60, $delivery)
            . (new Frame(Frame::HEADER, 1, $header))->encode();
        $client = new AMQP(new Config(), new MemoryTransport($incoming));
        $channel = $client->channel();
        try { $channel->consume('jobs', static fn (): null => null, 'expected'); self::fail('Expected mismatched consumer tag.'); }
        catch (ProtocolException $error) { self::assertStringContainsString('unexpected tag', $error->getMessage()); }
        self::assertFalse($client->isConnected());
    }
    public function testManualAcknowledgementsDoNotAccumulateDeliveryState(): void
    {
        $transport = new MemoryTransport(ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes()));
        $channel = (new AMQP(new Config(), $transport))->channel();
        for ($tag = 1; $tag <= 1000; $tag++) $channel->ack($tag);
        $state = self::privateValue($channel, 'state');
        self::assertNull(self::privateValue($state, 'callbackDeliveryTag'));
        self::assertFalse((bool) self::privateValue($state, 'callbackSettled'));
    }
    public function testBlockingConsumeCannotBeMixedWithSubscriptions(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('subscription')->bytes());
        $channel = (new AMQP(new Config(), new MemoryTransport($incoming)))->channel();
        $channel->subscribe('jobs', static fn (): null => null, 'subscription');
        $this->expectException(\LogicException::class);
        $channel->consume('jobs', static fn (): null => null, 'blocking');
    }
    public function testDuplicateConsumerTagFromBrokerAbortsConnection(): void
    {
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('duplicate')->bytes())
            . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('duplicate')->bytes());
        $client = new AMQP(new Config(), new MemoryTransport($incoming));
        $channel = $client->channel();
        $channel->subscribe('jobs', static fn (): null => null, 'first');
        try { $channel->subscribe('jobs', static fn (): null => null, 'second'); self::fail('Expected duplicate consumer tag.'); }
        catch (ProtocolException $error) { self::assertStringContainsString('Duplicate consumer tag', $error->getMessage()); }
        self::assertFalse($client->isConnected());
    }
    public function testOnlyOneSubscriptionWaitLoopCanRunPerChannel(): void
    {
        $rejected = false;
        Coroutine\run(function () use (&$rejected): void {
            $incoming = ServerFrames::handshake()
                . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
                . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('subscription')->bytes());
            $client = new AMQP(new Config(readTimeout: 10), new BlockingTransport($incoming));
            $channel = $client->channel();
            $channel->subscribe('jobs', static fn (): null => null, 'subscription');
            Coroutine::create(function () use ($channel): void { try { $channel->wait(); } catch (\Throwable) {} });
            $deadline = microtime(true) + 0.2;
            $state = self::privateValue($channel, 'state');
            while (!self::privateValue($state, 'subscriptionWaitActive') && microtime(true) < $deadline) Coroutine::sleep(0.001);
            try { $channel->wait(); }
            catch (\LogicException) { $rejected = true; }
            $client->close();
        });
        self::assertTrue($rejected);
    }
    public function testConnectionCanReconnectAfterTransportWasClosed(): void
    {
        $transport = new MemoryTransport(ServerFrames::handshake()); $client = new AMQP(new Config(), $transport); $client->connect();
        $transport->close(); $transport->incoming .= ServerFrames::handshake();
        $client->connect(); self::assertTrue($client->isConnected());
    }
    public function testReconnectInvalidatesChannelsFromPreviousConnection(): void
    {
        $transport = new MemoryTransport(ServerFrames::handshake() . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes()));
        $client = new AMQP(new Config(), $transport);
        $oldChannel = $client->channel(1);
        $transport->close(); $transport->incoming .= ServerFrames::handshake() . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes());
        $client->connect();
        $newChannel = $client->channel(1);
        self::assertSame(1, $newChannel->id);
        $this->expectException(\LogicException::class);
        $oldChannel->publish('must not be sent');
    }
    public function testFramesForAnotherChannelAreBuffered(): void
    {
        $openOk = (new Writer())->longstr('')->bytes();
        $queueOne = (new Writer())->shortstr('one')->long(0)->long(0)->bytes();
        $queueTwo = (new Writer())->shortstr('two')->long(0)->long(0)->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, $openOk)
            . ServerFrames::method(2, 20, 11, $openOk)
            . ServerFrames::method(0, 10, 60, (new Writer())->shortstr('low disk')->bytes())
            . ServerFrames::method(2, 50, 11, $queueTwo)
            . ServerFrames::method(1, 50, 11, $queueOne);
        $client = new AMQP(new Config(), new MemoryTransport($incoming)); $first = $client->channel(1); $second = $client->channel(2);
        self::assertSame('one', $first->queueDeclare('one')['queue']);
        self::assertSame('two', $second->queueDeclare('two')['queue']);
        self::assertSame('low disk', $client->blockedReason());
    }
    public function testChannelCloseFromBrokerReleasesIdForReuse(): void
    {
        $close = (new Writer())->short(404)->shortstr('missing')->short(50)->short(10)->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(1, 20, 40, $close)
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes());
        $client = new AMQP(new Config(), new MemoryTransport($incoming));
        $channel = $client->channel(1);
        try { $channel->queueDeclare('missing', passive: true); self::fail('Expected broker channel.close.'); }
        catch (ProtocolException $error) { self::assertSame(404, $error->replyCode); }
        self::assertSame(1, $client->channel(1)->id);
    }
    public function testConnectionCloseFromBrokerInvalidatesAllChannels(): void
    {
        $close = (new Writer())->short(320)->shortstr('forced')->short(0)->short(0)->bytes();
        $incoming = ServerFrames::handshake()
            . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
            . ServerFrames::method(0, 10, 50, $close);
        $client = new AMQP(new Config(), new MemoryTransport($incoming));
        $channel = $client->channel();
        try { $channel->queueDeclare('jobs'); self::fail('Expected broker connection.close.'); }
        catch (ProtocolException $error) { self::assertSame(320, $error->replyCode); }
        self::assertFalse($client->isConnected());
        $this->expectException(\LogicException::class);
        $channel->publish('must not be sent');
    }
    public function testChannelIdNeverExceedsProtocolFieldWidth(): void
    {
        $client = new AMQP(new Config(channelMax: 0), new MemoryTransport(ServerFrames::handshake(channelMax: 0)));
        $client->connect();
        $this->expectException(\InvalidArgumentException::class);
        $client->channel(65536);
    }
    public function testMandatoryPublishSetsMethodFlag(): void
    {
        $transport = new MemoryTransport(ServerFrames::handshake() . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes()));
        $channel = (new AMQP(new Config(), $transport))->channel();
        $channel->publish('body', mandatory: true);
        $frames = self::writtenFrames($transport->written);
        $method = new Reader($frames[count($frames) - 3]->payload);
        self::assertSame(60, $method->short()); self::assertSame(40, $method->short()); $method->short(); $method->shortstr(); $method->shortstr();
        self::assertTrue($method->bit());
    }
    public function testOperationsFromDifferentCoroutinesAreSerializedByWorker(): void
    {
        $result = []; $error = null;
        Coroutine\run(function () use (&$result, &$error): void {
            try {
                $reply = fn (string $name) => (new Writer())->shortstr($name)->long(0)->long(0)->bytes();
                $incoming = ServerFrames::handshake()
                    . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
                    . ServerFrames::method(1, 50, 11, $reply('first'))
                    . ServerFrames::method(1, 50, 11, $reply('second'));
                $client = new AMQP(new Config(), new MemoryTransport($incoming));
                $channel = $client->channel();
                $done = new Coroutine\Channel(2);
                foreach (['first', 'second'] as $name) {
                    Coroutine::create(function () use ($channel, $done, $name, &$result): void {
                        $result[] = $channel->queueDeclare($name)['queue'];
                        $done->push(true);
                    });
                }
                $done->pop(1); $done->pop(1);
                $client->close();
            } catch (\Throwable $throwable) { $error = $throwable; }
        });
        if ($error !== null) throw $error;
        self::assertSame(['first', 'second'], $result);
    }
    public function testQueuedOperationIsSkippedAfterCallerTimesOut(): void
    {
        $executed = 0; $timeouts = 0;
        Coroutine\run(function () use (&$executed, &$timeouts): void {
            $connection = new Connection(new Config(connectTimeout: 0.05, readTimeout: 0.05, writeTimeout: 0.05), new MemoryTransport());
            Coroutine::create(function () use ($connection, &$timeouts): void {
                try { $connection->execute(fn () => Coroutine::sleep(1.2)); }
                catch (\RuntimeException) { $timeouts++; }
            });
            Coroutine::sleep(0.01);
            Coroutine::create(function () use ($connection, &$executed, &$timeouts): void {
                try { $connection->execute(function () use (&$executed): void { $executed++; }); }
                catch (\RuntimeException) { $timeouts++; }
            });
            Coroutine::sleep(1.4);
            unset($connection);
            gc_collect_cycles();
        });
        self::assertSame(2, $timeouts);
        self::assertSame(0, $executed);
    }
    public function testIdleReadTimeoutAtFrameBoundaryKeepsTransportOpen(): void
    {
        $transport = new TimeoutTransport();
        $connection = new Connection(new Config(), $transport);
        try { $connection->readFrame(0, false); self::fail('Expected read timeout.'); }
        catch (TimeoutException) { self::assertSame(0, $transport->closeCount); }
    }
    public function testReadTimeoutAfterFrameHeaderClosesTransport(): void
    {
        $encoded = Frame::method(0, 10, 10, 'payload')->encode();
        $transport = new TimeoutTransport(substr($encoded, 0, 7));
        $connection = new Connection(new Config(), $transport);
        try { $connection->readFrame(0, false); self::fail('Expected read timeout.'); }
        catch (TimeoutException) { self::assertSame(1, $transport->closeCount); }
    }
    public function testHeartbeatTimerStopsWhenClientCloses(): void
    {
        $heartbeatFrames = 0; $writesAfterClose = -1; $writesLater = -2; $error = null;
        Coroutine\run(function () use (&$heartbeatFrames, &$writesAfterClose, &$writesLater, &$error): void {
            try {
                $transport = new MemoryTransport(ServerFrames::handshake(131072, 1));
                $client = new AMQP(new Config(heartbeat: 1), $transport);
                $client->connect();
                Coroutine::sleep(1.1);
                $heartbeatFrames = count(array_filter(self::writtenFrames($transport->written), fn (Frame $frame) => $frame->type === Frame::HEARTBEAT));
                $client->close();
                $writesAfterClose = strlen($transport->written);
                Coroutine::sleep(1.1);
                $writesLater = strlen($transport->written);
            } catch (\Throwable $throwable) { $error = $throwable; }
        });
        if ($error !== null) throw $error;
        self::assertGreaterThanOrEqual(1, $heartbeatFrames);
        self::assertSame($writesAfterClose, $writesLater);
    }
    public function testCloseInterruptsBlockedConsumerAndStopsWorkerPromptly(): void
    {
        $elapsed = 0.0; $consumerStopped = false;
        Coroutine\run(function () use (&$elapsed, &$consumerStopped): void {
            $incoming = ServerFrames::handshake()
                . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
                . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('idle')->bytes());
            $client = new AMQP(new Config(readTimeout: 10), new BlockingTransport($incoming));
            $channel = $client->channel();
            $done = new Coroutine\Channel(1);
            Coroutine::create(function () use ($channel, $done): void {
                try { $channel->consume('jobs', static fn (): bool => true, 'idle'); $done->push(null); }
                catch (\Throwable $error) { $done->push($error); }
            });
            Coroutine::sleep(0.02);
            $started = microtime(true);
            $client->close();
            $elapsed = microtime(true) - $started;
            $consumerStopped = $done->pop(0.5) instanceof \Throwable;
            $connection = self::connectionOf($client);
            self::assertFalse((bool) self::privateValue($connection, 'workerRunning'));
            self::assertNull(self::privateValue($connection, 'heartbeatTimerId'));
        });
        self::assertTrue($consumerStopped);
        self::assertLessThan(0.5, $elapsed);
    }
    public function testNewOperationsAreRejectedWhileConnectionIsClosing(): void
    {
        $rejected = false;
        Coroutine\run(function () use (&$rejected): void {
            $connection = new Connection(new Config(), new MemoryTransport());
            $started = new Coroutine\Channel(1);
            Coroutine::create(function () use ($connection, $started): void {
                $connection->execute(function () use ($started): void { $started->push(true); Coroutine::sleep(0.1); });
            });
            $started->pop(0.5);
            Coroutine::create(fn () => $connection->close());
            Coroutine::sleep(0.01);
            try { $connection->execute(static fn (): null => null); }
            catch (\LogicException $error) { $rejected = str_contains($error->getMessage(), 'closing'); }
            Coroutine::sleep(0.15);
        });
        self::assertTrue($rejected);
    }
    public function testForcedStopKeepsClosingGateUntilOldWorkerActuallyExits(): void
    {
        $closedGate = false; $reusable = false;
        Coroutine\run(function () use (&$closedGate, &$reusable): void {
            $connection = new Connection(
                new Config(connectTimeout: 0.05, readTimeout: 0.05, writeTimeout: 0.05),
                new MemoryTransport(),
            );
            $started = new Coroutine\Channel(1);
            Coroutine::create(function () use ($connection, $started): void {
                try { $connection->execute(function () use ($started): void { $started->push(true); Coroutine::sleep(1.3); }); }
                catch (\RuntimeException) {}
            });
            $started->pop(0.5);
            $connection->close();
            try { $connection->execute(static fn (): null => null); }
            catch (\LogicException) { $closedGate = true; }
            Coroutine::sleep(0.3);
            $reusable = $connection->execute(static fn (): bool => true);
            $connection->close();
        });
        self::assertTrue($closedGate);
        self::assertTrue($reusable);
    }
    public function testDestructorStopsHeartbeatTimer(): void
    {
        $writesAfterDestroy = -1; $writesLater = -2;
        Coroutine\run(function () use (&$writesAfterDestroy, &$writesLater): void {
            $transport = new MemoryTransport(ServerFrames::handshake(131072, 1));
            $client = new AMQP(new Config(heartbeat: 1), $transport);
            $client->connect();
            unset($client);
            gc_collect_cycles();
            $writesAfterDestroy = strlen($transport->written);
            Coroutine::sleep(1.1);
            $writesLater = strlen($transport->written);
        });
        self::assertSame($writesAfterDestroy, $writesLater);
    }
    public function testDestructorCollectsConnectionChannelAndSubscriptionCycles(): void
    {
        $collected = false; $timerCleared = false;
        Coroutine\run(function () use (&$collected, &$timerCleared): void {
            $incoming = ServerFrames::handshake(heartbeat: 1)
                . ServerFrames::method(1, 20, 11, (new Writer())->longstr('')->bytes())
                . ServerFrames::method(1, 60, 21, (new Writer())->shortstr('consumer')->bytes());
            $client = new AMQP(new Config(heartbeat: 1), new MemoryTransport($incoming));
            $channel = $client->channel();
            $subscription = $channel->subscribe('jobs', static fn (): null => null, 'consumer');
            $connection = self::connectionOf($client);
            $timer = self::privateValue($connection, 'heartbeatTimerId');
            $weakConnection = \WeakReference::create($connection);
            $weakChannel = \WeakReference::create($channel);
            $weakSubscription = \WeakReference::create($subscription);
            unset($subscription, $channel, $connection, $client);
            gc_collect_cycles();
            Coroutine::sleep(0.01);
            gc_collect_cycles();
            $collected = $weakConnection->get() === null && $weakChannel->get() === null && $weakSubscription->get() === null;
            $timerCleared = is_int($timer) && !Timer::exists($timer);
        });
        self::assertTrue($collected);
        self::assertTrue($timerCleared);
    }
    public function testWorkerAndHeartbeatTimerAreNotDuplicated(): void
    {
        $error = null;
        Coroutine\run(function () use (&$error): void {
            try {
                $transport = new MemoryTransport(ServerFrames::handshake(heartbeat: 1));
                $client = new AMQP(new Config(heartbeat: 1), $transport);
                $connection = self::connectionOf($client);
                $done = new Coroutine\Channel(8);
                for ($index = 0; $index < 8; $index++) Coroutine::create(function () use ($client, $done): void { $client->connect(); $done->push(true); });
                for ($index = 0; $index < 8; $index++) $done->pop(1);

                $worker = self::privateValue($connection, 'workerCid');
                $timer = self::privateValue($connection, 'heartbeatTimerId');
                self::assertIsInt($worker); self::assertIsInt($timer); self::assertTrue(Timer::exists($timer));
                $client->connect();
                self::assertSame($worker, self::privateValue($connection, 'workerCid'));
                self::assertSame($timer, self::privateValue($connection, 'heartbeatTimerId'));

                $transport->close();
                $transport->incoming .= ServerFrames::handshake(heartbeat: 1);
                $client->connect();
                $replacementTimer = self::privateValue($connection, 'heartbeatTimerId');
                self::assertSame($worker, self::privateValue($connection, 'workerCid'));
                self::assertNotSame($timer, $replacementTimer);
                self::assertFalse(Timer::exists($timer)); self::assertTrue(Timer::exists($replacementTimer));

                $client->close();
                self::assertFalse((bool) self::privateValue($connection, 'workerRunning'));
                self::assertNull(self::privateValue($connection, 'heartbeatTimerId'));

                $transport->incoming .= ServerFrames::handshake(heartbeat: 1);
                $client->connect();
                $newWorker = self::privateValue($connection, 'workerCid');
                $newTimer = self::privateValue($connection, 'heartbeatTimerId');
                self::assertIsInt($newWorker); self::assertNotSame($worker, $newWorker);
                self::assertIsInt($newTimer); self::assertTrue(Timer::exists($newTimer));
                $client->connect();
                self::assertSame($newWorker, self::privateValue($connection, 'workerCid'));
                self::assertSame($newTimer, self::privateValue($connection, 'heartbeatTimerId'));
                $client->close();
            } catch (\Throwable $throwable) { $error = $throwable; }
        });
        if ($error !== null) throw $error;
    }
    /** @return list<Frame> */
    private static function writtenFrames(string $bytes): array
    {
        $bytes = substr($bytes, 8); $offset = 0; $frames = [];
        while ($offset < strlen($bytes)) $frames[] = Frame::read(function (int $length) use (&$offset, $bytes): string { $part = substr($bytes, $offset, $length); $offset += $length; return $part; });
        return $frames;
    }
    private static function connectionOf(AMQP $client): Connection { return self::privateValue($client, 'connection'); }
    private static function privateValue(object $object, string $property): mixed
    {
        return (new \ReflectionProperty($object, $property))->getValue($object);
    }
}
