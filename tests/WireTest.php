<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Tests;

use EasySwoole\AMQP\Exception\ProtocolException;
use EasySwoole\AMQP\Wire\Frame;
use EasySwoole\AMQP\Wire\BasicProperties;
use EasySwoole\AMQP\Wire\Reader;
use EasySwoole\AMQP\Wire\Writer;
use PHPUnit\Framework\TestCase;

final class WireTest extends TestCase
{
    public function testZeroLengthHeartbeatDoesNotReadEmptyPayloadFromTransport(): void
    {
        $encoded = (new Frame(Frame::HEARTBEAT, 0, ''))->encode();
        $offset = 0; $lengths = [];
        $frame = Frame::read(function (int $length) use ($encoded, &$offset, &$lengths): string {
            $lengths[] = $length;
            $bytes = substr($encoded, $offset, $length);
            $offset += $length;
            return $bytes;
        });
        self::assertSame(Frame::HEARTBEAT, $frame->type);
        self::assertSame([7, 1], $lengths);
    }
    public function testScalarBitsAndTableRoundTrip(): void
    {
        $bytes = (new Writer())->octet(7)->short(258)->long(70000)->longlong(5_000_000_000)->bit(true)->bit(false)
            ->shortstr('ok')->longstr('body')->table(['name'=>'rabbit','active'=>true,'count'=>-3,'tags'=>['a','b',7],'capabilities'=>['nack'=>true],'nil'=>null])->bytes();
        $reader = new Reader($bytes);
        self::assertSame(7, $reader->octet()); self::assertSame(258, $reader->short()); self::assertSame(70000, $reader->long());
        self::assertSame(5_000_000_000, $reader->longlong()); self::assertTrue($reader->bit()); self::assertFalse($reader->bit());
        self::assertSame('ok', $reader->shortstr()); self::assertSame('body', $reader->longstr());
        self::assertSame(['name'=>'rabbit','active'=>true,'count'=>-3,'tags'=>['a','b',7],'capabilities'=>['nack'=>true],'nil'=>null], $reader->table());
    }
    public function testLargeSignedIntegersRoundTripInFieldTable(): void
    {
        $values = ['minimum32'=>-2_147_483_648, 'below32'=>-2_147_483_649, 'above32'=>2_147_483_648, 'large'=>5_000_000_000];
        $reader = new Reader((new Writer())->table($values)->bytes());
        self::assertSame($values, $reader->table());
    }
    public function testSignedShortFieldIsDecodedAsSigned(): void
    {
        $encoded = (new Writer())->longstr((new Writer())->shortstr('negative')->raw('s')->raw(pack('n', 0xffff))->bytes())->bytes();
        self::assertSame(['negative'=>-1], (new Reader($encoded))->table());
    }
    public function testNullBasicPropertiesAreOmitted(): void
    {
        $reader = new Reader(BasicProperties::encode(['content_type'=>null, 'delivery_mode'=>2]));
        self::assertSame(['delivery_mode'=>2], BasicProperties::decode($reader));
    }
    public function testWriterRejectsOutOfRangeOctetAndShort(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        (new Writer())->octet(256);
    }
    public function testFrameRoundTripAndInvalidTerminator(): void
    {
        $encoded = Frame::method(2, 60, 80, 'data')->encode(); $offset = 0;
        $frame = Frame::read(function (int $length) use (&$offset, $encoded): string { $part = substr($encoded, $offset, $length); $offset += $length; return $part; });
        self::assertSame(2, $frame->channel); self::assertSame([60, 80], array_slice($frame->methodParts(), 0, 2));
        $bad = substr($encoded, 0, -1) . "\0"; $offset = 0; $this->expectException(ProtocolException::class);
        Frame::read(function (int $length) use (&$offset, $bad): string { $part = substr($bad, $offset, $length); $offset += $length; return $part; });
    }
    public function testFrameRejectsTruncatedHeader(): void
    {
        $this->expectException(ProtocolException::class);
        Frame::read(fn (int $length): string => substr("\x01\x00", 0, $length));
    }
}
