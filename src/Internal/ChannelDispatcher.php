<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Internal;

use EasySwoole\AMQP\Connection;
use EasySwoole\AMQP\Exception\ProtocolException;
use EasySwoole\AMQP\Exception\PublisherNackException;
use EasySwoole\AMQP\Message;
use EasySwoole\AMQP\PublisherConfirmResult;
use EasySwoole\AMQP\ReturnedMessage;
use EasySwoole\AMQP\Wire\Reader;

final class ChannelDispatcher
{
    /** @var list<ReturnedMessage> */
    private array $returns = [];
    /** @var list<array{type:'delivery'|'cancel', value:Message|string}> */
    private array $consumerEvents = [];
    private readonly ChannelEventPump $pump;

    public function __construct(
        private readonly Connection $connection,
        int $channelId,
        ContentReader $contentReader,
        private readonly PublisherConfirmTracker $confirms,
    ) {
        $this->pump = new ChannelEventPump($connection, $channelId, $contentReader, $confirms);
    }

    /** @return array{int, int, Reader} */
    public function readMethod(bool $closeOnTimeout = true, bool $bufferDeliveries = true, bool $returnCancellation = false): array
    {
        $deferredNack = null;
        while (true) {
            $event = $this->pump->next($closeOnTimeout);
            switch ($event->type) {
                case InboundEvent::FLOW: continue 2;
                case InboundEvent::RETURNED: $this->returns[] = $event->value; continue 2;
                case InboundEvent::CONFIRMATION:
                    if ($event->negative) $deferredNack = $this->publisherNack();
                    continue 2;
                case InboundEvent::DELIVERY:
                    if (!$bufferDeliveries) break;
                    $this->bufferDelivery($event);
                    continue 2;
                case InboundEvent::CANCELLATION:
                    if ($returnCancellation) return [$event->classId, $event->methodId, new Reader('')];
                    $this->bufferCancellation($event);
                    continue 2;
            }
            if ($deferredNack !== null) throw $deferredNack;
            if (!$event->reader instanceof Reader) $this->protocolFailure('Expected AMQP method response.');
            return [$event->classId, $event->methodId, $event->reader];
        }
    }

    public function waitForConfirms(int $targetSequence, bool $captureReturn): ?ReturnedMessage
    {
        $returned = null;
        while ($this->confirms->hasPendingThrough($targetSequence)) {
            $event = $this->pump->next();
            if ($event->type === InboundEvent::FLOW) continue;
            if ($event->type === InboundEvent::RETURNED) {
                if ($captureReturn) $returned = $event->value;
                else $this->returns[] = $event->value;
                continue;
            }
            if ($event->type === InboundEvent::DELIVERY) { $this->bufferDelivery($event); continue; }
            if ($event->type === InboundEvent::CANCELLATION) { $this->bufferCancellation($event); continue; }
            if ($event->type !== InboundEvent::CONFIRMATION) {
                $this->protocolFailure("Expected publisher confirmation, received {$event->classId}.{$event->methodId}.");
            }
            if ($event->negative) throw $this->publisherNack();
        }
        return $returned;
    }

    /** @return array{type:'delivery'|'cancel', value:Message|string} */
    public function nextConsumerEvent(): array
    {
        $queued = $this->popConsumerEvent();
        if ($queued !== null) return $queued;
        while (true) {
            $event = $this->pump->next(false);
            if ($event->type === InboundEvent::FLOW) continue;
            if ($event->type === InboundEvent::RETURNED) { $this->returns[] = $event->value; continue; }
            if ($event->type === InboundEvent::CONFIRMATION) {
                if ($event->negative) throw $this->publisherNack();
                continue;
            }
            if ($event->type === InboundEvent::DELIVERY) return ['type'=>'delivery', 'value'=>$event->value];
            if ($event->type === InboundEvent::CANCELLATION) return ['type'=>'cancel', 'value'=>$event->value];
            $this->protocolFailure("Expected consumer event, received {$event->classId}.{$event->methodId}.");
        }
    }

    /** @param list<int> $sequences */
    public function waitForBatch(array $sequences): PublisherConfirmResult
    {
        $target = max($sequences);
        $first = min($sequences);
        $acked = []; $nacked = []; $returned = [];
        while ($this->confirms->hasPendingThrough($target)) {
            $event = $this->pump->next();
            if ($event->type === InboundEvent::FLOW) continue;
            if ($event->type === InboundEvent::RETURNED) { $returned[] = $event->value; continue; }
            if ($event->type === InboundEvent::DELIVERY) { $this->bufferDelivery($event); continue; }
            if ($event->type === InboundEvent::CANCELLATION) { $this->bufferCancellation($event); continue; }
            if ($event->type !== InboundEvent::CONFIRMATION) {
                $this->protocolFailure("Expected publisher confirmation, received {$event->classId}.{$event->methodId}.");
            }
            foreach ($event->sequences as $sequence) {
                if ($sequence < $first || $sequence > $target) continue;
                if ($event->negative) $nacked[] = $sequence - $first + 1;
                else $acked[] = $sequence - $first + 1;
            }
        }
        sort($acked); sort($nacked);
        return new PublisherConfirmResult($acked, $nacked, $returned);
    }

    public function popReturn(): ?ReturnedMessage { return array_shift($this->returns); }
    /** @return null|array{type:'delivery'|'cancel', value:Message|string} */
    public function popConsumerEvent(): ?array { return array_shift($this->consumerEvents); }
    public function publishingActive(): bool { return $this->pump->publishingActive(); }
    public function clear(): void { $this->returns = []; $this->consumerEvents = []; $this->pump->reset(); }
    public function handleServerFlow(int $class, int $method, Reader $reader): bool { return $this->pump->handleServerFlow($class, $method, $reader); }

    private function bufferDelivery(InboundEvent $event): void { $this->consumerEvents[] = ['type'=>'delivery', 'value'=>$event->value]; }
    private function bufferCancellation(InboundEvent $event): void { $this->consumerEvents[] = ['type'=>'cancel', 'value'=>$event->value]; }
    private function publisherNack(): PublisherNackException { return new PublisherNackException('RabbitMQ negatively acknowledged a published message.'); }
    private function protocolFailure(string $message): never
    {
        $this->connection->abort();
        throw new ProtocolException($message);
    }
}
