# EasySwoole AMQP

基于 `Swoole\Coroutine\Socket` 的轻量 AMQP 0-9-1 客户端。协议编解码由本项目实现，不依赖阻塞式 AMQP 扩展。

运行要求：PHP 8.1+、Swoole 5.1+。

## 使用

所有网络操作都必须在 Swoole 协程内执行。一个 AMQP 实例内部只有一个 I/O 协程持有 Socket，业务协程通过内部指令 Channel 提交操作，因此同一实例可以被多个业务协程共享。

```php
use EasySwoole\AMQP\AMQP;
use EasySwoole\AMQP\Config;

Swoole\Coroutine\run(function (): void {
    $client = new AMQP(new Config(
        host: '127.0.0.1',
        user: 'guest',
        password: 'guest',
        vhost: '/',
    ));

    $channel = $client->channel();
    $channel->exchangeDeclare('events', 'topic', durable: true);
    $queue = $channel->queueDeclare('jobs', durable: true);
    $channel->queueBind($queue['queue'], 'events', 'job.*');
    $channel->publish('{"id":1}', 'events', 'job.created', [
        'content_type' => 'application/json',
        'delivery_mode' => 2,
    ]);

    // 需要 Broker 落地确认时使用 Publisher Confirm。
    $channel->publishConfirmed('important', 'events', 'job.important', [
        'delivery_mode' => 2,
    ], timeout: 5.0);

    // mandatory 消息无法路由时，basic.return 会携带原消息返回。
    try {
        $channel->publishConfirmed('must-route', 'events', 'missing', mandatory: true);
    } catch (\EasySwoole\AMQP\Exception\MessageReturnedException $error) {
        echo $error->returnedMessage->replyText; // NO_ROUTE
    }

    $channel->qos(10);
    $channel->consume('jobs', function ($message): bool {
        echo $message->body, PHP_EOL;
        return true; // true/null: ACK，false: NACK 并重新入队
    });

    $client->close();
});
```

也可以用 `get('jobs', timeout: 2.0)` 拉取单条消息，并通过 `ack()`、`nack()` 或 `reject()` 手动确认。传入自定义 `TransportInterface` 可进行无网络测试。

`get()` 和 `publishConfirmed()` 的 `timeout` 默认是 `3.0` 秒，表示整个操作的 deadline，从提交到内部 worker 时开始计算，包含排队、写帧和等待 Broker 响应。deadline 到期时，未执行的操作会被跳过；已阻塞在网络 I/O 的操作会取消连接并抛出 `TimeoutException`，因为超时后 AMQP 字节流已无法安全复用。传入 `-1.0` 表示不设置单操作 deadline。

批量 Publisher Confirm 会先发送整批消息，再统一等待确认，并保留部分 ACK、NACK 和 mandatory return：

```php
$result = $channel->publisherBatch()
    ->publish('one', 'events', 'created')
    ->publish('two', 'events', 'missing', mandatory: true)
    ->wait(timeout: 5.0);

$result->acked;   // 批次内从 1 开始的序号
$result->nacked;
$result->returned;
```

同一 Channel 多 Consumer 使用 `subscribe()` 注册，再由一个 `wait()` 循环按 consumer-tag 分发：

```php
$first = $channel->subscribe('queue-a', function ($message, $channel, $subscription): void {
    echo $message->body;
    $subscription->cancel();
});
$second = $channel->subscribe('queue-b', $anotherHandler);
$channel->wait();
```

同一 Channel 只能运行一个 `wait()`，并且不能混用阻塞式 `consume()` 与 `subscribe()`；不同 Channel 不受此限制。

协议操作还包括 `exchangeBind()`、`exchangeUnbind()`、`queueUnbind()`、`queuePurge()`、`recover()` 和 `recoverAsync()`。Field table 支持 Decimal、时间戳、二进制值和显式整数位宽，分别使用 `Wire\Decimal`、`DateTimeInterface`、`Wire\ByteArray` 和 `Wire\IntegerValue` 表示。

普通 `publish(..., mandatory: true)` 保持非阻塞；可通过 `setReturnHandler()` 接收之后读到的 `basic.return`，或通过 `popReturnedMessage()` 取出已解析的退回消息。若必须在本次调用中确定消息是否可路由，请使用 `publishConfirmed(..., mandatory: true)`；RabbitMQ 会先发送 `basic.return`，再发送 Publisher Confirm，客户端会完整读取两者并抛出 `MessageReturnedException`。

一个连接可以建立多个 Channel。多个业务协程可以提交操作，客户端会在唯一的 I/O 协程中串行执行完整的请求/响应过程，避免 AMQP 帧交错；读取过程中遇到的其他 Channel 帧会按频道缓存。客户端本身不是连接池，也不会创建或管理多个连接。

每个 AMQP Channel 内部使用统一的事件分发器处理同步 RPC 响应和异步 `basic.deliver`、`basic.return`、Publisher ACK/NACK。消息内容组装与 Publisher Confirm 序号状态分别由独立内部组件维护，因此异步投递插入 RPC 或 Confirm 响应之前时不会丢失或错误归类。公开 API 不依赖这些内部实现。

默认关闭 AMQP heartbeat。启用后，每个 AMQP 实例会创建一个定时器，但定时器只向内部操作队列投递心跳指令；所有 Socket 读写均由唯一的 I/O 协程串行执行。`close()` 会清除定时器、关闭连接并停止 I/O 协程。这里仅参考 Pool 的定时器生命周期管理，客户端本身不包含连接池或对象借还逻辑。单条入站消息默认限制为 64 MiB，可通过 `maxMessageSize` 调整。

`close()` 是幂等的尽力关闭操作：空闲时会发送 AMQP `connection.close` 并等待响应；若 worker 正阻塞在消费读取中，则关闭底层传输作为协程取消信号，待旧 worker 真正退出后才允许重新连接。关闭期间的新操作会被拒绝，避免旧、新 worker 同时操作一个 Socket。

## TLS 与双向 TLS

TLS 默认验证服务端证书。私有 CA、SNI 和客户端证书通过 `TlsConfig` 配置：

```php
use EasySwoole\AMQP\Config;
use EasySwoole\AMQP\TlsConfig;

$config = new Config(
    host: 'rabbit.internal',
    port: 5671,
    tls: new TlsConfig(
        caFile: '/etc/rabbitmq/ca.pem',
        serverName: 'rabbit.internal',
        // RabbitMQ 要求 mTLS 时，同时提供以下两项：
        clientCertificate: '/etc/rabbitmq/client.pem',
        clientPrivateKey: '/etc/rabbitmq/client-key.pem',
        passphrase: getenv('AMQP_TLS_KEY_PASSPHRASE') ?: null,
    ),
);
```

`clientCertificate` 和 `clientPrivateKey` 必须同时配置。生产环境建议保持 `verifyPeer=true`；使用私有或自签 CA 时优先配置 `caFile`，不要直接关闭证书验证。

## 测试

```bash
composer test
```

真实 RabbitMQ 集成测试默认跳过，显式提供连接配置后运行：

```bash
AMQP_INTEGRATION=1 AMQP_HOST=127.0.0.1 AMQP_PORT=5672 \
AMQP_USER=guest AMQP_PASSWORD=guest AMQP_VHOST=/ \
vendor/bin/phpunit --group integration
```

集成测试使用随机命名的 exchange/queue，并在结束时清理，不会依赖或删除业务队列。
