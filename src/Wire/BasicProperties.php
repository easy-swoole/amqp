<?php
declare(strict_types=1);

namespace EasySwoole\AMQP\Wire;

/** Encodes and decodes the fixed property list of AMQP class basic (60). */
final class BasicProperties
{
    /** @var array<string, array{int, string}> */
    private const TYPES = [
        'content_type'=>[15,'s'], 'content_encoding'=>[14,'s'], 'headers'=>[13,'t'],
        'delivery_mode'=>[12,'o'], 'priority'=>[11,'o'], 'correlation_id'=>[10,'s'],
        'reply_to'=>[9,'s'], 'expiration'=>[8,'s'], 'message_id'=>[7,'s'],
        'timestamp'=>[6,'l'], 'type'=>[5,'s'], 'user_id'=>[4,'s'],
        'app_id'=>[3,'s'], 'cluster_id'=>[2,'s'],
    ];

    /** @param array<string, mixed> $properties */
    public static function encode(array $properties): string
    {
        $unknown = array_diff_key($properties, self::TYPES);
        if ($unknown !== []) throw new \InvalidArgumentException('Unknown basic property: ' . array_key_first($unknown));
        $flags = 0; $writer = new Writer();
        foreach (self::TYPES as $name => [$bit]) if (($properties[$name] ?? null) !== null) $flags |= 1 << $bit;
        foreach (self::TYPES as $name => [, $type]) {
            if (($properties[$name] ?? null) === null) continue;
            $value = $properties[$name];
            match ($type) {
                's' => $writer->shortstr((string) $value), 'o' => $writer->octet((int) $value),
                'l' => $writer->longlong((int) $value), 't' => $writer->table((array) $value),
            };
        }
        return pack('n', $flags) . $writer->bytes();
    }

    /** @return array<string, mixed> */
    public static function decode(Reader $reader): array
    {
        $flags = $reader->short(); $result = [];
        if (($flags & 1) !== 0) throw new \UnexpectedValueException('Multiple basic property flag words are unsupported by AMQP 0-9-1.');
        foreach (self::TYPES as $name => [$bit, $type]) {
            if (($flags & (1 << $bit)) === 0) continue;
            $result[$name] = match ($type) {
                's' => $reader->shortstr(), 'o' => $reader->octet(),
                'l' => $reader->longlong(), 't' => $reader->table(),
            };
        }
        return $result;
    }

    private function __construct() {}
}
