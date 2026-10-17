<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Tests\Support;

use EasySwoole\AMQP\Wire\Frame;
use EasySwoole\AMQP\Wire\Writer;

final class ServerFrames
{
    public static function handshake(int $frameMax = 131072, int $heartbeat = 60, int $channelMax = 2047): string
    {
        $start = (new Writer())->octet(0)->octet(9)->table(['product'=>'RabbitMQ'])->longstr('AMQPLAIN PLAIN')->longstr('en_US')->bytes();
        $tune = (new Writer())->short($channelMax)->long($frameMax)->short($heartbeat)->bytes();
        return Frame::method(0, 10, 10, $start)->encode() . Frame::method(0, 10, 30, $tune)->encode() . Frame::method(0, 10, 41)->encode();
    }
    public static function method(int $channel, int $class, int $method, string $arguments = ''): string
    { return Frame::method($channel, $class, $method, $arguments)->encode(); }
}
