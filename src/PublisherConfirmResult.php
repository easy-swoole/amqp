<?php
declare(strict_types=1);

namespace EasySwoole\AMQP;

final class PublisherConfirmResult
{
    /**
     * @param list<int> $acked Sequence numbers within this batch, starting at 1.
     * @param list<int> $nacked Sequence numbers within this batch, starting at 1.
     * @param list<ReturnedMessage> $returned
     */
    public function __construct(
        public readonly array $acked,
        public readonly array $nacked,
        public readonly array $returned,
    ) {}

    public function allAcknowledged(): bool { return $this->nacked === [] && count($this->acked) > 0; }
}
