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

`ConsumerCommiterMiddleware` is inserted into the consumer pipeline and performs three checks before passing a message further:

1. **Already committed** — if the message was already successfully processed (`commitedAt` is not null), the middleware logs a warning and stops the pipeline.
2. **Max attempts exceeded** — if `maxAttempt` is set and the attempt count has exceeded it, the middleware logs an error and stops the pipeline.
3. **Successful processing** — if both checks pass, the message continues down the pipeline; once handled, `commit()` is called to record it as processed.

## Installation

```bash
composer require micromus/kafka-bus-commiter
```

## Usage

### Basic Example

Implement `ConsumerMessageRepositoryInterface` to persist message state (e.g. in a database or Redis), then pass the 
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
    consumerMessageRepository: $repository, // required
    logger: $logger,                         // PSR-3 logger, defaults to NullLogger
    maxAttempt: 3,                           // max attempts, -1 = unlimited
)
```

| Parameter                   | Type                                 | Default      | Description                                                 |
|-----------------------------|--------------------------------------|--------------|-------------------------------------------------------------|
| `consumerMessageRepository` | `ConsumerMessageRepositoryInterface` | —            | Storage for message state                                   |
| `logger`                    | `LoggerInterface`                    | `NullLogger` | PSR-3 compatible logger                                     |
| `maxAttempt`                | `int`                                | `-1`         | Maximum number of processing attempts. `-1` means unlimited |

### Implementing the Repository

You need to provide your own implementation of `ConsumerMessageRepositoryInterface`:

```php
use Micromus\KafkaBus\Interfaces\Consumers\Messages\ConsumerMessageInterface;
use Micromus\KafkaBusCommiter\Attempt;
use Micromus\KafkaBusCommiter\Interfaces\ConsumerMessageRepositoryInterface;
 
class DatabaseConsumerMessageRepository implements ConsumerMessageRepositoryInterface
{
    /**
     * Returns the current attempt for a given message.
     * Return Attempt(number: 1) if this is the first time the message is seen.
     * Return Attempt with a non-null commitedAt if it was already committed.
     */
    public function attempt(ConsumerMessageInterface $message): Attempt
    {
        // ...
    }
 
    /**
     * Marks the message as successfully processed.
     */
    public function commit(ConsumerMessageInterface $message): void
    {
        // ...
    }
 
    /**
     * Returns true if a record for this message already exists in storage.
     */
    public function exists(ConsumerMessageInterface $message): bool
    {
        // ...
    }
}
```

The message identifier is available via `$message->msgId()`.


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
