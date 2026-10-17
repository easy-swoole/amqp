<?php
declare(strict_types=1);

namespace EasySwoole\AMQP;

/** AMQP 0-9-1 class and method identifiers used by the client. */
final class Protocol
{
    public const CONNECTION = 10;
    public const CHANNEL = 20;
    public const EXCHANGE = 40;
    public const QUEUE = 50;
    public const BASIC = 60;
    public const CONFIRM = 85;

    public const CONNECTION_START = 10;
    public const CONNECTION_START_OK = 11;
    public const CONNECTION_TUNE = 30;
    public const CONNECTION_TUNE_OK = 31;
    public const CONNECTION_OPEN = 40;
    public const CONNECTION_OPEN_OK = 41;
    public const CONNECTION_CLOSE = 50;
    public const CONNECTION_CLOSE_OK = 51;

    public const CHANNEL_OPEN = 10;
    public const CHANNEL_OPEN_OK = 11;
    public const CHANNEL_CLOSE = 40;
    public const CHANNEL_CLOSE_OK = 41;

    public const BASIC_RETURN = 50;
    public const BASIC_DELIVER = 60;
    public const BASIC_ACK = 80;
    public const BASIC_NACK = 120;

    private function __construct() {}
}
