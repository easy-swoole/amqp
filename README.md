# EasySwoole AMQP

基于 `Swoole\Coroutine\Socket` 实现的 AMQP 0-9-1 协程客户端。协议编解码由本项目实现，不依赖 `ext-amqp` 或阻塞式 AMQP 客户端。

## 环境与安装

- PHP 8.1+
- Swoole 5.1+
- RabbitMQ 或其他兼容 AMQP 0-9-1 的 Broker
- 64 位 PHP

```bash
composer require easyswoole/amqp
```

所有网络操作都应在 Swoole 协程内执行：

```php
use EasySwoole\AMQP\AMQP;
use EasySwoole\AMQP\Config;
use Swoole\Coroutine;

Coroutine\run(function (): void {
    $client = new AMQP(new Config(
        host: '127.0.0.1',
        port: 5672,
        user: 'guest',
        password: 'guest',
        vhost: '/',
    ));

    try {
        $channel = $client->channel();
        // 使用 Channel……
    } finally {
        $client->close();
    }
});
```

`channel()` 会按需建立连接，通常不必提前调用 `connect()`。

## 声明、绑定与发布

```php
$channel = $client->channel();

$channel->exchangeDeclare(
    exchange: 'events',
    type: 'topic',
    durable: true,
);

$declared = $channel->queueDeclare(
    queue: 'jobs',
    durable: true,
);

$channel->queueBind(
    queue: $declared['queue'],
    exchange: 'events',
    routingKey: 'job.*',
);

$channel->publish(
    body: json_encode(['id' => 1], JSON_THROW_ON_ERROR),
    exchange: 'events',
    routingKey: 'job.created',
    properties: [
        'content_type' => 'application/json',
        'delivery_mode' => 2,
        'message_id' => 'job-1',
        'headers' => ['source' => 'example'],
    ],
);
```

`publish()` 只负责写入消息，不等待 Broker 确认。可靠发布请使用 Publisher Confirm。

## 拉取消息

```php
$message = $channel->get('jobs', timeout: 2.0);

if ($message !== null) {
    try {
        handle($message->body, $message->properties);
        $channel->ack($message->deliveryTag);
    } catch (Throwable $error) {
        $channel->nack($message->deliveryTag, requeue: true);
        throw $error;
    }
}
```

`get()` 返回 `null` 表示队列当前为空。使用 `autoAck: true` 后不要再手动 ACK/NACK。

## 消费消息

### 单个阻塞式消费者

```php
use EasySwoole\AMQP\Channel;
use EasySwoole\AMQP\Message;

$channel->qos(prefetchCount: 20);

$consumerTag = $channel->consume(
    queue: 'jobs',
    handler: function (Message $message, Channel $channel): bool {
        if ($message->body === 'stop') {
            $channel->cancel((string) $message->consumerTag);
        }

        return process($message->body);
    },
);
```

手动确认模式下，回调返回值含义如下：

- `false`：发送 `basic.nack(requeue: true)`。
- 其他值（包括 `null`）：发送 `basic.ack`。
- 抛出异常：尽量 NACK 并重新入队，然后重新抛出原异常。
- 回调内已经调用 `ack()`、`nack()` 或 `reject()`：不会重复确认。

### 同一 Channel 上的多个消费者

```php
$first = $channel->subscribe('queue-a', function (Message $message, Channel $channel, $subscription): void {
    consumeA($message);
    $subscription->cancel();
});

$second = $channel->subscribe('queue-b', function (Message $message): bool {
    return consumeB($message);
});

$channel->wait();               // 等到所有订阅取消
// $channel->wait(timeout: 10); // 最多等待 10 秒
```

限制与注意事项：

- 同一个 Channel 不能混用 `consume()` 与 `subscribe()`。
- 同一个 Channel 同时只能运行一个 `wait()`。
- `Subscription::cancel()` 在等待 Broker 回执期间是幂等的。
- 需要并行处理多个消费者时，建议使用不同的 AMQP Channel。

## Publisher Confirm

### 单条确认

```php
$channel->publishConfirmed(
    body: $body,
    exchange: 'events',
    routingKey: 'job.created',
    properties: ['delivery_mode' => 2],
    timeout: 5.0,
);
```

首次调用会自动发送 `confirm.select`，也可以提前启用：

```php
$channel->enablePublisherConfirms();
```

Broker 返回 NACK 时抛出 `PublisherNackException`。NACK 不表示连接损坏，Channel 仍可继续使用。

### 批量确认

```php
$result = $channel->publisherBatch()
    ->publish('one', 'events', 'job.created')
    ->publish('two', 'events', 'missing', mandatory: true)
    ->publish('three', 'events', 'job.created')
    ->wait(timeout: 5.0);

$result->acked;    // 批次内从 1 开始的序号
$result->nacked;
$result->returned; // list<ReturnedMessage>

if (!$result->allAcknowledged()) {
    // 处理 NACK 或退回消息
}
```

批次不能为空，调用 `wait()` 后不能重复使用。

## mandatory 与 basic.return

需要同步确定消息是否成功路由时，将 mandatory 与 Confirm 一起使用：

```php
use EasySwoole\AMQP\Exception\MessageReturnedException;

try {
    $channel->publishConfirmed(
        body: 'important',
        exchange: 'events',
        routingKey: 'possibly.missing',
        mandatory: true,
    );
} catch (MessageReturnedException $error) {
    $returned = $error->returnedMessage;
    echo $returned->replyCode;          // 例如 312
    echo $returned->replyText;          // 例如 NO_ROUTE
    echo $returned->message->body;      // 原消息体
    var_dump($returned->message->properties);
}
```

普通 `publish(..., mandatory: true)` 不会等待返回结果。客户端在之后读取 Socket 时解析 `basic.return`，可使用：

```php
$channel->setReturnHandler(function ($returned, $channel): void {
    reportUnroutable($returned);
});

// 或获取已经解析并缓存的退回消息
$returned = $channel->popReturnedMessage();
```

Return handler 在 I/O 操作结束后执行，可以安全调用该 Channel 的其他公开方法。

## Timeout 与 deadline

以下业务等待操作默认超时为 3 秒：

```php
$channel->get('jobs', timeout: 3.0);
$channel->publishConfirmed(body: $body, timeout: 3.0);
$channel->publisherBatch()->publish($body)->wait(timeout: 3.0);
```

这里的 `timeout` 是完整操作 deadline，从提交给内部 I/O worker 开始计算，包括排队、写帧及等待和读取 Broker 响应。传入 `-1.0` 表示不设置单操作 deadline。

超时会抛出 `TimeoutException`：

- 尚未开始的排队操作会被跳过。
- 已经开始且阻塞在网络 I/O 的操作会关闭连接，因为响应边界已不再可信，不能安全复用原字节流。

`Config` 的 `connectTimeout`、`readTimeout`、`writeTimeout` 是底层 Socket 超时；方法参数 `timeout` 是更上层的完整操作 deadline。

## ACK、NACK 与恢复

```php
$channel->ack($deliveryTag);
$channel->ack($deliveryTag, multiple: true);

$channel->nack($deliveryTag, requeue: true);
$channel->nack($deliveryTag, multiple: true, requeue: false);

$channel->reject($deliveryTag, requeue: false);

$channel->recover(requeue: true);       // 等待 basic.recover-ok
$channel->recoverAsync(requeue: true);  // 只发送 basic.recover-async
```

`deliveryTag` 只在产生它的 Channel 内有效，不能跨 Channel 确认。

## 事务

```php
$channel->txSelect();

try {
    $channel->publish('one', routingKey: 'jobs');
    $channel->publish('two', routingKey: 'jobs');
    $channel->txCommit();
} catch (Throwable $error) {
    $channel->txRollback();
    throw $error;
}
```

AMQP 事务与 Publisher Confirm 不能在同一个 Channel 上同时启用。需要两种语义时应使用不同 Channel。

## Exchange 与 Queue API

- Exchange：`exchangeDeclare()`、`exchangeDelete()`、`exchangeBind()`、`exchangeUnbind()`。
- Queue：`queueDeclare()`、`queueDelete()`、`queueBind()`、`queueUnbind()`、`queuePurge()`。

`queueDeclare()` 返回实际队列名、消息数和消费者数：

```php
[
    'queue' => '实际队列名',
    'messageCount' => 0,
    'consumerCount' => 0,
]
```

空队列名可由 Broker 自动生成。临时队列通常使用 `exclusive: true, autoDelete: true`；RabbitMQ 4.x 对部分非持久、非独占队列组合有限制，应按 Broker 版本选择参数。

## Channel Flow 与连接阻塞

```php
$channel->flow(false);
$channel->flow(true);

if (!$channel->isPublishingActive()) {
    // 当前暂停发布
}

$reason = $client->blockedReason(); // connection.blocked 原因；未阻塞为 null
```

Broker 主动发送 `channel.flow` 时客户端自动回复。发布被暂停时调用 `publish()` 会抛出 `LogicException`。

客户端处理 RabbitMQ 的 `connection.blocked`/`connection.unblocked` 通知，但不会替业务无限缓存或自动重试消息。

## 多 Channel 与协程模型

```php
$channel1 = $client->channel();
$channel2 = $client->channel();
$channel10 = $client->channel(10);
```

一个 `AMQP` 实例是一条 TCP/AMQP Connection，不是连接池：

- 一个实例只有一个 Socket、一个 I/O worker。
- 多个业务协程可以共享实例，完整操作在 worker 中串行执行。
- 一个连接可以创建多个 AMQP Channel。
- 其他 Channel 的入站帧会被路由或暂存，不会与当前 RPC 混淆。
- 同一个 Channel 不允许重入操作。

如果业务需要多个 TCP 连接，请创建多个 `AMQP` 实例，或在上层自行管理连接池。

## 心跳、生命周期与关闭

```php
$client = new AMQP(new Config(
    host: 'rabbit.internal',
    heartbeat: 10,
    readTimeout: 25,
));
```

默认 `heartbeat=0`。启用后：

- 每个实例最多创建一个心跳定时器和一个 I/O worker。
- 定时器只提交指令，Socket 始终由 I/O worker 独占。
- 空闲消费者按不超过 `heartbeat / 2` 的读取时间片让出 worker，避免阻塞心跳、取消和其他 Channel。
- `close()` 会清除定时器、关闭 Socket 并停止 worker。

建议始终在 `finally` 中显式调用 `close()`。该方法幂等；若 worker 正阻塞于读取，会关闭底层 Socket 作为取消信号，旧 worker 完全退出前不会创建第二个 worker。

Channel 也可单独关闭：

```php
$channel->close();
```

Broker 主动关闭 Connection/Channel 时，客户端会回复 `close-ok`，使相关对象失效，并抛出带 `replyCode`、`classId`、`methodId` 的协议异常。

## TLS、mTLS 与 EXTERNAL

```php
use EasySwoole\AMQP\TlsConfig;

$config = new Config(
    host: 'rabbit.internal',
    port: 5671,
    tls: new TlsConfig(
        caFile: '/etc/ssl/rabbitmq-ca.pem',
        serverName: 'rabbit.internal',
    ),
);
```

双向 TLS：

```php
tls: new TlsConfig(
    caFile: '/etc/ssl/rabbitmq-ca.pem',
    serverName: 'rabbit.internal',
    clientCertificate: '/etc/ssl/client.pem',
    clientPrivateKey: '/etc/ssl/client-key.pem',
    passphrase: getenv('AMQP_TLS_KEY_PASSPHRASE') ?: null,
),
```

证书身份认证使用 SASL EXTERNAL：

```php
$config = new Config(
    host: 'rabbit.internal',
    port: 5671,
    authMechanism: Config::AUTH_EXTERNAL,
    tls: new TlsConfig(
        caFile: '/etc/ssl/rabbitmq-ca.pem',
        serverName: 'rabbit.internal',
        clientCertificate: '/etc/ssl/client.pem',
        clientPrivateKey: '/etc/ssl/client-key.pem',
    ),
);
```

注意：

- 默认认证机制为 `PLAIN`。
- `EXTERNAL` 要求客户端证书、私钥以及 Broker 对该机制的支持。
- 客户端证书和私钥必须同时配置。
- 生产环境应保持 `verifyPeer=true`。
- 私有或自签 CA 应配置 `caFile`/`caPath`，不要直接关闭证书验证。

## Basic Properties 与 Field Table

支持全部 `basic` 固定属性：`content_type`、`content_encoding`、`headers`、`delivery_mode`、`priority`、`correlation_id`、`reply_to`、`expiration`、`message_id`、`timestamp`、`type`、`user_id`、`app_id`、`cluster_id`。

Field table 支持字符串、布尔、整数、浮点、Decimal、二进制、时间戳、数组、嵌套 table 和 `null`：

```php
use EasySwoole\AMQP\Wire\ByteArray;
use EasySwoole\AMQP\Wire\Decimal;
use EasySwoole\AMQP\Wire\IntegerValue;

$headers = [
    'enabled' => true,
    'count' => 10,
    'ratio' => 0.5,
    'price' => new Decimal(unscaled: 1999, scale: 2),
    'binary' => new ByteArray("\x00\x01"),
    'created_at' => new DateTimeImmutable(),
    'items' => ['a', 'b'],
    'metadata' => ['source' => 'api'],
    'small_unsigned' => new IntegerValue(255, 'B'),
    'optional' => null,
];
```

普通 PHP `int` 自动编码为 32 或 64 位整数；需要固定 AMQP 整数类型时使用 `IntegerValue`。

## 配置项

| 参数 | 默认值 | 说明 |
| --- | ---: | --- |
| `host` | `127.0.0.1` | Broker 地址 |
| `port` | `5672` | AMQP 端口；TLS 通常为 5671 |
| `user` | `guest` | 用户名 |
| `password` | `guest` | 密码 |
| `vhost` | `/` | Virtual host |
| `connectTimeout` | `3.0` | 建连超时，秒 |
| `readTimeout` | `60.0` | Socket 读取超时，秒 |
| `writeTimeout` | `3.0` | Socket 写入及内部队列等待超时，秒 |
| `heartbeat` | `0` | 请求的心跳周期；0 为关闭 |
| `channelMax` | `2047` | 请求的最大 Channel ID |
| `frameMax` | `131072` | 请求的最大帧大小 |
| `maxMessageSize` | `67108864` | 单条入站消息上限，默认 64 MiB |
| `tls` | `null` | TLS 配置 |
| `authMechanism` | `PLAIN` | `PLAIN` 或 `EXTERNAL` |

`channelMax`、`frameMax` 和 `heartbeat` 最终会与 Broker 的 `connection.tune` 参数协商。配置超时可以传 `-1.0` 表示无限等待。

## 异常

| 异常 | 含义 |
| --- | --- |
| `ConnectionException` | TCP、TLS、连接或写入失败 |
| `TimeoutException` | Socket 或完整操作 deadline 超时 |
| `ProtocolException` | 帧、方法或 AMQP 数据不符合预期 |
| `ChannelClosedException` | Broker 主动关闭 Channel |
| `ConnectionClosedByBrokerException` | Broker 主动关闭 Connection |
| `PublisherNackException` | Publisher Confirm 收到 NACK |
| `MessageReturnedException` | mandatory 消息被退回 |

截断帧、未知 Channel、超限消息等本地协议错误会关闭连接，避免继续使用已经失去帧边界可信度的 Socket。

## 协议支持情况

本项目覆盖常见 RabbitMQ 工作负载，不宣称实现 AMQP 0-9-1 的每个扩展方法。

| 能力 | 状态 |
| --- | --- |
| AMQP 0-9-1 握手、tune、open、close | 支持 |
| PLAIN / EXTERNAL SASL | 支持 |
| TCP、TLS、mTLS | 支持 |
| 多 Channel 与跨 Channel 帧路由 | 支持 |
| Exchange declare/delete/bind/unbind | 支持 |
| Queue declare/delete/bind/unbind/purge | 支持 |
| Basic publish/get/consume/cancel | 支持 |
| ACK/NACK/reject/recover/recover-async | 支持 |
| QoS | 支持 |
| Publisher Confirm（单条、批量、multiple） | 支持 |
| mandatory / basic.return | 支持 |
| AMQP Transaction | 支持 |
| channel.flow | 支持 |
| connection.blocked/unblocked | 支持 |
| Heartbeat | 支持 |
| Field table 与 Basic Properties | 支持 |
| 自动重连、拓扑自动恢复 | 不提供 |
| 客户端连接池 | 不提供 |
| AMQP 1.0 | 不支持 |
| RabbitMQ Stream 协议 | 不支持 |

断线后应重新建立 Channel，并按业务需要重新声明拓扑与消费者。客户端不会隐式重放 publish，以免确认边界不明时产生不可控重复。

## 重要注意事项

1. 网络调用必须位于 Swoole 协程环境。
2. 一个 `AMQP` 实例是一条连接，不是连接池。
3. 不要跨 Channel 使用 delivery-tag。
4. `publish()` 返回不等于 Broker 已确认；可靠发布使用 Confirm。
5. mandatory 需配合 return handler、返回队列或 `publishConfirmed()` 才能观察路由失败。
6. deadline 到期后连接可能关闭，这是保护协议流完整性的预期行为。
7. 事务与 Confirm 应使用不同 Channel。
8. 消费回调应尽量短；耗时业务需谨慎设计业务协程和 ACK 时机。
9. ACK/NACK 提供至少一次投递基础，业务仍需保证幂等。
10. 显式调用 `close()`，不要只依赖析构函数。

## 测试

```bash
composer test
```

真实 RabbitMQ 集成测试默认跳过。显式提供测试 Broker 后运行：

```bash
AMQP_INTEGRATION=1 \
AMQP_HOST=127.0.0.1 \
AMQP_PORT=5672 \
AMQP_USER=guest \
AMQP_PASSWORD=guest \
AMQP_VHOST=/ \
vendor/bin/phpunit --group integration
```

集成测试覆盖声明、绑定、Confirm、get、ACK/NACK 重投、多 Channel 并发、consume/cancel、heartbeat、空闲消费者、mandatory/basic.return 与事务。测试资源使用随机名称并在结束时清理。
