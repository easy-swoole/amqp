<?php
declare(strict_types=1);

namespace EasySwoole\AMQP;

use EasySwoole\AMQP\Exception\ProtocolException;
use EasySwoole\AMQP\Exception\TimeoutException;
use EasySwoole\AMQP\Transport\SwooleSocketTransport;
use EasySwoole\AMQP\Transport\HeartbeatAwareTransportInterface;
use EasySwoole\AMQP\Transport\TransportInterface;
use EasySwoole\AMQP\Wire\Frame;
use EasySwoole\AMQP\Wire\Reader;
use EasySwoole\AMQP\Wire\Writer;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel as CoroutineChannel;
use Swoole\Timer;

final class Connection
{
    public const PROTOCOL_HEADER = "AMQP\x00\x00\x09\x01";
    private readonly TransportInterface $transport;
    private bool $connected = false;
    private int $frameMax;
    private int $channelMax;
    private int $heartbeat;
    private int $nextChannel = 1;
    private ?CoroutineChannel $operations = null;
    private ?int $workerCid = null;
    private bool $workerRunning = false;
    private bool $workerBusy = false;
    private bool $closing = false;
    private ?int $heartbeatTimerId = null;
    /** @var array<int, true|Channel> */
    private array $channels = [];
    /** @var array<int, list<Frame>> */
    private array $pendingFrames = [];
    private float $lastReadAt = 0.0;
    private float $lastWriteAt = 0.0;
    private bool $writing = false;
    private ?string $blockedReason = null;

    public function __construct(private readonly Config $config = new Config(), ?TransportInterface $transport = null)
    {
        $this->transport = $transport ?? new SwooleSocketTransport($config);
        $this->frameMax = $config->frameMax; $this->channelMax = $config->channelMax; $this->heartbeat = $config->heartbeat;
    }

    public function __destruct()
    {
        // Destructors cannot safely wait for coroutine work; perform an immediate fallback cleanup.
        $this->closing = true;
        $this->clearHeartbeatTimer();
        $this->workerRunning = false;
        $this->operations?->close();
        $this->transport->close();
    }

    public function connect(): void
    {
        $this->execute(function (): void {
            if ($this->connected && $this->transport->isConnected()) return;
            if ($this->connected) $this->closeTransport();
            $this->handshake();
        });
    }

    private function handshake(): void
    {
        try {
            if ($this->transport instanceof HeartbeatAwareTransportInterface) $this->transport->setHeartbeatInterval(0);
            $this->transport->connect(); $this->transport->write(self::PROTOCOL_HEADER);
            [$class, $method, $start] = $this->readMethod(0);
            if ($class !== Protocol::CONNECTION || $method !== Protocol::CONNECTION_START) throw new ProtocolException('Expected connection.start.');
            $start->octet(); $start->octet(); $start->table(); $mechanisms = $start->longstr(); $locales = $start->longstr();
            $mechanism = $this->config->authMechanism;
            $offeredMechanisms = preg_split('/\s+/', trim($mechanisms)) ?: [];
            if (!in_array($mechanism, $offeredMechanisms, true)) throw new ProtocolException("Server does not support {$mechanism} authentication.");
            $locale = str_contains(" {$locales} ", ' en_US ') ? 'en_US' : strtok($locales, ' ');
            $response = $mechanism === Config::AUTH_EXTERNAL ? '' : "\0{$this->config->user}\0{$this->config->password}";
            $args = (new Writer())->table(['product'=>'easyswoole/amqp','platform'=>'PHP','version'=>'1.0'])
                ->shortstr($mechanism)->longstr($response)->shortstr((string) $locale)->bytes();
            $this->transport->write(Frame::method(0, Protocol::CONNECTION, Protocol::CONNECTION_START_OK, $args)->encode());
            [$class, $method, $tune] = $this->readMethod(0);
            if ($class !== Protocol::CONNECTION || $method !== Protocol::CONNECTION_TUNE) throw new ProtocolException('Expected connection.tune.');
            $this->channelMax = self::negotiate($this->config->channelMax, $tune->short());
            $serverFrameMax = $tune->long();
            if ($serverFrameMax !== 0 && $serverFrameMax < 4096) throw new ProtocolException('Server proposed an invalid frame-max below 4096.');
            $this->frameMax = self::negotiate($this->config->frameMax, $serverFrameMax);
            $serverHeartbeat = $tune->short();
            // A client value of zero explicitly disables heartbeats, matching php-amqplib compatibility semantics.
            $this->heartbeat = $this->config->heartbeat === 0 ? 0 : self::negotiate($this->config->heartbeat, $serverHeartbeat);
            if ($this->transport instanceof HeartbeatAwareTransportInterface) $this->transport->setHeartbeatInterval($this->heartbeat);
            $tuneOk = (new Writer())->short($this->channelMax)->long($this->frameMax)->short($this->heartbeat)->bytes();
            $this->transport->write(Frame::method(0, Protocol::CONNECTION, Protocol::CONNECTION_TUNE_OK, $tuneOk)->encode());
            $open = (new Writer())->shortstr($this->config->vhost)->shortstr('')->bit(false)->bytes();
            $this->transport->write(Frame::method(0, Protocol::CONNECTION, Protocol::CONNECTION_OPEN, $open)->encode());
            [$class, $method] = $this->readMethod(0);
            if ($class !== Protocol::CONNECTION || $method !== Protocol::CONNECTION_OPEN_OK) throw new ProtocolException('Expected connection.open-ok.');
            $this->connected = true; $this->lastReadAt = $this->lastWriteAt = microtime(true);
            $this->startHeartbeatTimer();
        } catch (\Throwable $error) { $this->closeTransport(); throw $error; }
    }

    public function channel(?int $id = null): Channel
    {
        $this->connect();
        $id = $this->execute(function () use ($id): int {
            $channelId = $id;
            if ($channelId === null) {
                while (isset($this->channels[$this->nextChannel])) $this->nextChannel++;
                $channelId = $this->nextChannel++;
            }
            $maximum = $this->channelMax > 0 ? $this->channelMax : 65535;
            if ($channelId < 1 || $channelId > $maximum) throw new \InvalidArgumentException('Channel id exceeds negotiated channel-max.');
            if (isset($this->channels[$channelId])) throw new \LogicException("AMQP channel {$channelId} is already open.");
            $this->channels[$channelId] = true;
            return $channelId;
        });
        try {
            $channel = new Channel($this, $id);
            $this->execute(function () use ($channel, $id): void { $this->channels[$id] = $channel; });
            $channel->open();
            return $channel;
        }
        catch (\Throwable $error) { $this->execute(fn () => $this->releaseChannel($id)); throw $error; }
    }
    public function isConnected(): bool { return $this->connected && $this->transport->isConnected(); }
    public function frameMax(): int { return $this->frameMax; }
    public function maxMessageSize(): int { return $this->config->maxMessageSize; }
    public function blockedReason(): ?string { return $this->blockedReason; }
    public function execute(callable $operation, ?float $timeout = null): mixed
    {
        if ($this->closing) throw new \LogicException('AMQP connection is closing.');
        return $this->executeOperation($operation, $timeout);
    }
    private function executeOperation(callable $operation, ?float $timeout = null): mixed
    {
        if ($timeout !== null && (!is_finite($timeout) || ($timeout <= 0 && $timeout !== -1.0))) {
            throw new \InvalidArgumentException('Operation timeout must be positive seconds, -1, or null.');
        }
        $cid = Coroutine::getCid();
        if ($cid < 0 || $cid === $this->workerCid) return $operation();
        $this->startWorker();
        $reply = new CoroutineChannel(1);
        $startedAt = microtime(true);
        $state = (object) [
            'cancelled'=>false,
            'started'=>false,
            'deadline'=>$timeout === null || $timeout === -1.0 ? null : $startedAt + $timeout,
        ];
        $enqueueTimeout = $timeout ?? $this->config->writeTimeout;
        if (!$this->operations?->push(['operation'=>$operation, 'reply'=>$reply, 'state'=>$state], $enqueueTimeout)) {
            throw new TimeoutException('AMQP operation timed out while waiting for the I/O worker.');
        }
        $waitTimeout = $timeout ?? ($this->config->readTimeout === -1.0
            ? -1
            : max($this->config->connectTimeout, $this->config->readTimeout, $this->config->writeTimeout) + 1);
        if ($waitTimeout !== -1) $waitTimeout = max(0.001, $waitTimeout - (microtime(true) - $startedAt));
        $result = $reply->pop($waitTimeout);
        if (!is_array($result)) {
            $state->cancelled = true;
            if ($state->started) $this->closeTransport();
        }
        $reply->close();
        if (!is_array($result) || !array_key_exists('value', $result)) throw new TimeoutException('AMQP operation deadline exceeded.');
        if ($result['value'] instanceof \Throwable) throw $result['value'];
        return $result['value'];
    }
    public function send(Frame $frame): void
    {
        try {
            while ($this->writing) {
                if (Coroutine::getCid() < 0) throw new \LogicException('Concurrent AMQP writes require a coroutine context.');
                Coroutine::sleep(0.001);
            }
            $this->writing = true;
            $encoded = $frame->encode();
            if ($this->frameMax > 0 && strlen($encoded) > $this->frameMax) {
                throw new ProtocolException('Outbound frame exceeds negotiated frame-max.');
            }
            $this->transport->write($encoded);
            $this->lastWriteAt = microtime(true);
        } catch (\Throwable $error) {
            $this->closeTransport();
            throw $error;
        } finally { $this->writing = false; }
    }
    public function releaseChannel(int $id, ?Channel $expected = null): void
    {
        if ($expected !== null && ($this->channels[$id] ?? null) !== $expected) return;
        unset($this->channels[$id], $this->pendingFrames[$id]);
        if ($id < $this->nextChannel) $this->nextChannel = $id;
    }
    public function abort(): void { $this->closeTransport(); }
    public function readFrame(int $channel, bool $closeOnTimeout = true): Frame
    {
        while (true) {
            if (($this->pendingFrames[$channel] ?? []) !== []) return array_shift($this->pendingFrames[$channel]);
            $segmentsRead = 0;
            try {
                $frame = Frame::read(function (int $length) use (&$segmentsRead): string {
                    $bytes = $this->transport->read($length);
                    $segmentsRead++;
                    return $bytes;
                }, $this->frameMax);
                $this->lastReadAt = microtime(true);
            } catch (TimeoutException $error) {
                if ($closeOnTimeout || $segmentsRead > 0) $this->closeTransport();
                throw $error;
            } catch (\Throwable $error) { $this->closeTransport(); throw $error; }
            if ($frame->type === Frame::HEARTBEAT) {
                $this->send(new Frame(Frame::HEARTBEAT, 0, ''));
                if (!$closeOnTimeout) throw new TimeoutException('AMQP consumer read yielded after heartbeat.');
                continue;
            }
            if ($frame->channel === 0 && $this->handleConnectionControl($frame)) continue;
            if ($channel !== 0 && $frame->channel === 0 && $this->isConnectionClose($frame)) {
                return $this->validateFrameForChannel($frame, $channel);
            }
            if ($frame->channel !== $channel) {
                $target = $this->channels[$frame->channel] ?? null;
                if ($target instanceof Channel && $frame->type === Frame::METHOD) {
                    [$class, $method, $reader] = $frame->methodParts();
                    if ($target->handleAsyncMethod($class, $method, $reader)) continue;
                }
                if ($target === null) {
                    $this->closeTransport();
                    throw new ProtocolException("Frame received for unknown channel {$frame->channel}.");
                }
                $this->pendingFrames[$frame->channel][] = $frame;
                continue;
            }
            return $this->validateFrameForChannel($frame, $channel);
        }
    }
    /** @return array{0:int,1:int,2:Reader} */
    public function readMethod(int $channel, bool $closeOnTimeout = true): array
    {
        [$class, $method, $reader] = $this->readFrame($channel, $closeOnTimeout)->methodParts();
        if (($class === 10 && $method === 50) || ($class === 20 && $method === 40)) {
            $code = $reader->short(); $text = $reader->shortstr(); $failedClass = $reader->short(); $failedMethod = $reader->short();
            $this->send(Frame::method($channel, $class, $method + 1));
            if ($class === 10) $this->closeTransport();
            throw new ProtocolException($text, $code, $failedClass, $failedMethod);
        }
        return [$class, $method, $reader];
    }
    public function close(): void
    {
        if ($this->closing) return;
        $this->closing = true;
        $this->clearHeartbeatTimer();
        if (Coroutine::getCid() < 0) {
            // Outside a coroutine there is no safe way to await AMQP close-ok or a worker.
            // Fall back to immediate cancellation, matching destructor semantics.
            $this->closeTransport();
            $this->operations?->close();
            if (!$this->workerRunning) {
                $this->workerBusy = false; $this->workerCid = null; $this->operations = null; $this->closing = false;
            }
            return;
        }
        // An operation blocked in recv cannot consume a queued close command. Closing the
        // transport is the cancellation signal; the worker remains the only frame processor.
        if ($this->workerBusy && Coroutine::getCid() !== $this->workerCid) $this->closeTransport();
        try {
            $this->executeOperation(function (): void {
                if ($this->isConnected()) {
                    try { $args = (new Writer())->short(200)->shortstr('Goodbye')->short(0)->short(0)->bytes(); $this->send(Frame::method(0, 10, 50, $args)); $this->readMethod(0); } catch (\Throwable) {}
                }
                $this->closeTransport();
            });
        } catch (\Throwable) {
            // close() is best-effort and idempotent. Queue/read timeouts still fall through
            // to stopWorker(), which closes the transport and wakes blocked I/O.
        } finally {
            $this->stopWorker();
            // A forced stop can return before an arbitrary user operation exits. Keep the
            // gate closed until the worker's cleanup block runs in that case.
            $this->closing = $this->workerRunning;
        }
    }
    private function validateFrameForChannel(Frame $frame, int $channel): Frame
    {
        if ($channel !== 0 && $frame->channel === 0 && $this->isConnectionClose($frame)) {
            [, , $reader] = $frame->methodParts();
            $code = $reader->short(); $text = $reader->shortstr(); $failedClass = $reader->short(); $failedMethod = $reader->short();
            try { $this->send(Frame::method(0, Protocol::CONNECTION, Protocol::CONNECTION_CLOSE_OK)); } catch (\Throwable) {}
            $this->closeTransport();
            throw new ProtocolException($text, $code, $failedClass, $failedMethod);
        }
        return $frame;
    }
    private function isConnectionClose(Frame $frame): bool
    {
        if ($frame->type !== Frame::METHOD || $frame->channel !== 0 || strlen($frame->payload) < 4) return false;
        $ids = unpack('nclass/nmethod', substr($frame->payload, 0, 4));
        return $ids['class'] === Protocol::CONNECTION && $ids['method'] === Protocol::CONNECTION_CLOSE;
    }
    private function handleConnectionControl(Frame $frame): bool
    {
        if ($frame->type !== Frame::METHOD || strlen($frame->payload) < 4) return false;
        [$class, $method, $reader] = $frame->methodParts();
        if ($class !== Protocol::CONNECTION) return false;
        if ($method === 60) { $this->blockedReason = $reader->shortstr(); return true; }
        if ($method === 61) { $this->blockedReason = null; return true; }
        return false;
    }
    private function closeTransport(): void
    {
        $this->clearHeartbeatTimer();
        $this->connected = false;
        foreach ($this->channels as $channel) if ($channel instanceof Channel) $channel->invalidate();
        $this->channels = []; $this->pendingFrames = []; $this->blockedReason = null; $this->nextChannel = 1;
        $this->transport->close();
    }
    private function startWorker(): void
    {
        if ($this->workerRunning) return;
        $this->operations = new CoroutineChannel(64);
        $this->workerRunning = true;
        $queue = $this->operations;
        $weak = \WeakReference::create($this);
        $workerCid = Coroutine::create(static function () use ($queue, $weak): void {
            while (true) {
                $command = $queue->pop();
                if (!is_array($command)) break;
                $reply = $command['reply'] ?? null;
                $state = $command['state'] ?? null;
                if (($command['stop'] ?? false) === true) {
                    if ($reply instanceof CoroutineChannel) $reply->push(['value'=>null]);
                    break;
                }
                if (is_object($state) && ($state->cancelled ?? false) === true) {
                    unset($command, $reply, $state);
                    continue;
                }
                if (is_object($state) && is_float($state->deadline ?? null) && microtime(true) >= $state->deadline) {
                    if ($reply instanceof CoroutineChannel) $reply->push(['value'=>new TimeoutException('AMQP operation deadline exceeded before execution.')]);
                    unset($command, $reply, $state);
                    continue;
                }
                $self = $weak->get();
                if (!$self instanceof self) {
                    if ($reply instanceof CoroutineChannel) $reply->push(['value'=>new \RuntimeException('AMQP connection was destroyed.')]);
                    break;
                }
                $self->workerCid = Coroutine::getCid();
                if (is_object($state)) $state->started = true;
                $self->workerBusy = true;
                try { $value = ($command['operation'])(); }
                catch (\Throwable $error) { $value = $error; }
                finally { $self->workerBusy = false; }
                if (is_object($state) && ($state->cancelled ?? false) === true) $self->closeTransport();
                if ($reply instanceof CoroutineChannel) $reply->push(['value'=>$value]);
                // Do not retain an operation closure capturing the connection while waiting for the next command.
                unset($command, $value, $self, $reply, $state);
            }
            $queue->close();
            $self = $weak->get();
            if ($self instanceof self && $self->operations === $queue) {
                $self->workerRunning = false;
                $self->workerBusy = false;
                $self->workerCid = null;
                $self->operations = null;
                $self->closing = false;
            }
        });
        if ($workerCid === false) {
            $queue->close();
            $this->operations = null;
            $this->workerRunning = false;
            throw new \RuntimeException('Unable to create AMQP I/O worker coroutine.');
        }
        $this->workerCid = $workerCid;
    }
    private function stopWorker(): void
    {
        if (!$this->workerRunning || !$this->operations) return;
        if (Coroutine::getCid() === $this->workerCid) {
            $queue = $this->operations;
            $this->workerRunning = false; $this->workerBusy = false; $this->workerCid = null; $this->operations = null;
            $queue->close();
            return;
        }
        $reply = new CoroutineChannel(1);
        $queue = $this->operations;
        $queued = $queue->push(['stop'=>true, 'reply'=>$reply], $this->config->writeTimeout);
        $stopped = $queued ? $reply->pop($this->config->writeTimeout) : false;
        $reply->close();
        if (is_array($stopped)) {
            $this->workerRunning = false; $this->workerBusy = false; $this->workerCid = null; $this->operations = null;
            return;
        }
        // Keep workerRunning=true until the worker actually exits, preventing a second worker
        // from being created against the same transport during forced shutdown.
        $this->transport->close();
        $queue->close();
    }
    private function startHeartbeatTimer(): void
    {
        $this->clearHeartbeatTimer();
        if ($this->heartbeat <= 0 || Coroutine::getCid() < 0) return;
        $weak = \WeakReference::create($this);
        $timerId = Timer::tick(max(1, intdiv($this->heartbeat * 1000, 2)), static function () use ($weak): void {
            $self = $weak->get();
            if (!$self instanceof self || !$self->workerRunning || !$self->operations) return;
            // The timer only submits work. The socket remains owned by the I/O worker.
            $self->operations->push([
                'operation'=>static function () use ($weak): void { $weak->get()?->sendHeartbeatIfDue(); },
                'reply'=>null,
                'state'=>null,
            ], 0.001);
        });
        if ($timerId === false) throw new \RuntimeException('Unable to create AMQP heartbeat timer.');
        $this->heartbeatTimerId = $timerId;
    }
    private function clearHeartbeatTimer(): void
    {
        if ($this->heartbeatTimerId !== null && Timer::exists($this->heartbeatTimerId)) Timer::clear($this->heartbeatTimerId);
        $this->heartbeatTimerId = null;
    }
    private function sendHeartbeatIfDue(): void
    {
        if ($this->heartbeat > 0 && $this->isConnected() && microtime(true) - $this->lastWriteAt >= $this->heartbeat / 2) {
            $this->send(new Frame(Frame::HEARTBEAT, 0, ''));
        }
    }
    private static function negotiate(int $client, int $server): int { return $client === 0 ? $server : ($server === 0 ? $client : min($client, $server)); }
}
