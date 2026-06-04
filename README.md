# Kafka Bus Commiter for PHP

[![Latest Version on Packagist](https://img.shields.io/packagist/v/micromus/kafka-bus-commiter.svg?style=flat-square)](https://packagist.org/packages/micromus/kafka-bus-commiter)
[![GitHub Tests Action Status](https://img.shields.io/github/actions/workflow/status/micromus/kafka-bus-commiter/run-tests.yml?branch=1.x&label=tests&style=flat-square)](https://github.com/micromus/kafka-bus-commiter/actions?query=workflow%3Arun-tests+branch%3A1.x)
[![GitHub Code Style](https://img.shields.io/github/actions/workflow/status/micromus/kafka-bus-commiter/php-code-style.yml?branch=1.x&label=code-style&style=flat-square)](https://github.com/micromus/kafka-bus-commiter/actions?query=workflow%3Acode-style+branch%3A1.x)
[![GitHub PHPStan](https://img.shields.io/github/actions/workflow/status/micromus/kafka-bus-commiter/phpstan.yml?branch=1.x&label=phpstan&style=flat-square)](https://github.com/micromus/kafka-bus-commiter/actions?query=workflow%3Aphpstan+branch%3A1.x)
[![Total Downloads](https://img.shields.io/packagist/dt/micromus/kafka-bus-commiter.svg?style=flat-square)](https://packagist.org/packages/micromus/kafka-bus-commiter)

A middleware package for [micromus/kafka-bus](https://github.com/micromus/kafka-bus) that provides idempotent Kafka
message processing. It tracks which messages have already been handled, prevents duplicate processing, and allows
limiting the maximum number of read attempts.

## How It Works

`ConsumerCommiterMiddleware` is inserted into the consumer pipeline and handles four outcomes:

1. **Already committed** — if the message was already successfully processed (`commitedAt` is not `null`), the middleware logs a warning and stops the pipeline.
2. **Max attempts exceeded** — if `maxAttempt` is set and the attempt count has exceeded it, the middleware logs an error and stops the pipeline.
3. **Successful processing** — if both checks pass, the message continues down the pipeline; once handled, `commit()` is called to record it as processed.
4. **Handler error** — if downstream processing throws an exception, the middleware calls `failed()` and rethrows the exception.

## Installation

```bash
composer require micromus/kafka-bus-commiter
```

## Usage

### Basic Example

Implement `RepositorySourceInterface` to persist message state (for example in a database or Redis), then pass the
middleware into your worker options:

```php
use Micromus\KafkaBus\Bus;
use Micromus\KafkaBus\Consumers\Router\ConsumerRoutesBuilder;
use Micromus\KafkaBus\Consumers\Router\RouteInfo;
use Micromus\KafkaBus\Topics\Topic;
use Micromus\KafkaBus\Topics\TopicRegistry;
use Micromus\KafkaBusCommiter\Middleware\ConsumerCommiterMiddleware;
 
$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'));
 
$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new YourMessageHandler()))
    ->build();
 
$workerRegistry = (new Bus\Listeners\Workers\MemoryWorkerRegistry())
    ->add(
        new Bus\Listeners\Workers\Worker(
            name: 'default-listener',
            routes: $consumerRoutes,
            options: new Bus\Listeners\Workers\Options(
                middleware: [
                    new ConsumerCommiterMiddleware(new YourMessageRepository())
                ]
            )
        )
    );
```

### Middleware Options

```php
new ConsumerCommiterMiddleware(
    repository: $repository, // required
    logger: $logger,         // PSR-3 logger, defaults to NullLogger
    maxAttempt: 3,           // max attempts, -1 = unlimited
)
```

| Parameter    | Type                                 | Default      | Description                                                 |
|--------------|--------------------------------------|--------------|-------------------------------------------------------------|
| `repository` | `RepositorySourceInterface`          | —            | Storage for message state                                   |
| `logger`     | `LoggerInterface`                    | `NullLogger` | PSR-3 compatible logger                                     |
| `maxAttempt` | `int`                                | `-1`         | Maximum number of processing attempts. `-1` means unlimited |

### Implementing the Repository

You need to provide your own implementation of `RepositorySourceInterface`:

```php
use Micromus\KafkaBusCommiter\Attempt;
use Micromus\KafkaBusCommiter\Interfaces\RepositorySourceInterface;
 
class DatabaseConsumerMessageRepository implements RepositorySourceInterface
{
    /**
     * Returns the current attempt for a given key.
     */
    public function get(string $key): ?Attempt
    {
        // ...
    }

    /**
     * Increments the number of failed read attempts for a key.
     */
    public function increment(string $key): void
    {
        // ...
    }

    /**
     * Marks the message key as successfully processed.
     */
    public function commit(string $key): void
    {
        // ...
    }
}
```

The middleware reads/writes processing state through `RepositorySourceInterface`.
If you need key derivation from message data, use `IdempotencyMessageRepository` as an adapter that maps `ConsumerMessageInterface` to repository keys.

## Idempotency Keys

Out of the box the consumer uses Kafka's `msgId()` (a combination of topic, partition and offset) as the storage key.
That works as long as the same physical message is never replayed under a different offset. As soon as you have
retries, producer-side resends, or cross-cluster mirroring, the same logical event can arrive with a different
`msgId()` and slip past the duplicate check.

An **idempotency key** is a stable identifier that the producer attaches to a message so the consumer can recognize
duplicates regardless of where or how they arrive. This package transports the key through the
`x-idempotency-key` Kafka header (`IdempotencyMessageRepository::HEADER_NAME`).

### Producing: `HasIdempotency` + `ProducerIdempotencyMiddleware`

On the producer side you mark your message class with the `HasIdempotency` interface and return the stable key.
The `ProducerIdempotencyMiddleware` reads that key and writes it into the `x-idempotency-key` header before the
message hits the broker.

```php
use Micromus\KafkaBus\Interfaces\Producers\Messages\ProducerMessageInterface;
use Micromus\KafkaBusCommiter\Interfaces\HasIdempotency;

final readonly class ProductCreated implements ProducerMessageInterface, HasIdempotency
{
    public function __construct(
        private string $productId,
        private string $payload,
    ) {
    }

    public function toPayload(): string
    {
        return $this->payload;
    }

    public function getIdempotencyKey(): string
    {
        return $this->productId;
    }
}
```

Wire the middleware into the publisher route for that message class:

```php
use Micromus\KafkaBus\Bus\Publishers\Router\Options;
use Micromus\KafkaBus\Bus\Publishers\Router\PublisherRoutesBuilder;
use Micromus\KafkaBusCommiter\Middleware\ProducerIdempotencyMiddleware;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(
        ProductCreated::class,
        'products',
        new Options(middleware: [new ProducerIdempotencyMiddleware()])
    )
    ->build();
```

Middleware is registered per publisher route, so you opt individual message classes into the header. Messages
that do not implement `HasIdempotency` pass through the middleware untouched — no header is added.

### Consuming: `IdempotencyMessageRepository`

On the consumer side, plug `IdempotencyMessageRepository` into `ConsumerCommiterMiddleware`. It reads the
`x-idempotency-key` header and builds the storage key as `"{header}-{topicName}"`, so the same idempotency key
in two different topics is still treated as two distinct events. If the header is missing, it falls back to
`msgId()` so legacy producers keep working.

```php
use Micromus\KafkaBusCommiter\Middleware\ConsumerCommiterMiddleware;
use Micromus\KafkaBusCommiter\Repositories\IdempotencyMessageRepository;

$repository = new IdempotencyMessageRepository(new YourRepositorySource());

new ConsumerCommiterMiddleware($repository, maxAttempt: 3);
```

### Picking a key

Pick something that uniquely identifies the **business event**, not the transport. Good choices are an aggregate
id plus a version (`order-42-v3`), an outbox row id, or any value the upstream system already treats as unique.
Avoid values that change on retry (timestamps, random UUIDs generated per send attempt) — they defeat the whole
mechanism.


## Testing

```bash
composer test
```

## Changelog

Please see [CHANGELOG](CHANGELOG.md) for more information on what has changed recently.

## Contributing

Please see [CONTRIBUTING](CONTRIBUTING.md) for details.

## Security Vulnerabilities

Please review [our security policy](../../security/policy) on how to report security vulnerabilities.

## Credits

- [Kirill Popkov](https://github.com/popkovkirill)
- [All Contributors](../../contributors)

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.
