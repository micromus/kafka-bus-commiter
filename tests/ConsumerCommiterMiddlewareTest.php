<?php

namespace Micromus\KafkaBusCommiter\Tests;

use Micromus\KafkaBus\Bus;
use Micromus\KafkaBus\Connections\Registry\ConnectionRegistry;
use Micromus\KafkaBus\Consumers\Messages\ConsumerMessage;
use Micromus\KafkaBus\Consumers\Router\ConsumerRoutesBuilder;
use Micromus\KafkaBus\Consumers\Router\RouteInfo;
use Micromus\KafkaBus\Exceptions\Consumers\MessageConsumerNotHandledException;
use Micromus\KafkaBus\Testing\Connections\ConnectionFaker;
use Micromus\KafkaBus\Testing\Connections\ConnectionRegistryFaker;
use Micromus\KafkaBus\Testing\Consumers\MessageFactory;
use Micromus\KafkaBus\Testing\Messages\ConsumerHandlerFaker;
use Micromus\KafkaBus\Topics\Topic;
use Micromus\KafkaBus\Topics\TopicRegistry;
use Micromus\KafkaBusCommiter\Middleware\ConsumerCommiterMiddleware;
use Micromus\KafkaBusCommiter\Repositories\ArrayRepositorySource;
use Micromus\KafkaBusCommiter\Repositories\IdempotencyMessageRepository;
use RuntimeException;
use Testo\Assert;
use Testo\Test;

#[Test]
final class ConsumerCommiterMiddlewareTest
{
    public function canConsumeMessage(): void
    {
        $topicRegistry = (new TopicRegistry())
            ->add(new Topic('production.fact.products.1', 'products'));

        $connectionFaker = new ConnectionFaker($topicRegistry);

        $message = MessageFactory::for()
            ->withTopicKey('products')
            ->withHeaders(['foo' => 'bar'])
            ->make('test-message');

        $connectionFaker->addMessage($message);

        $repository = new IdempotencyMessageRepository(new ArrayRepositorySource());

        $workerRegistry = (new Bus\Listeners\Workers\MemoryWorkerRegistry())
            ->add(
                new Bus\Listeners\Workers\Worker(
                    name: 'default-listener',
                    routes: ConsumerRoutesBuilder::make($topicRegistry)
                        ->add(new RouteInfo('products', new ConsumerHandlerFaker()))
                        ->build(),
                    options: new Bus\Listeners\Workers\Options(
                        middleware: [new ConsumerCommiterMiddleware($repository)]
                    )
                )
            );

        self::buildBus($connectionFaker, $workerRegistry)
            ->listener('default-listener')
            ->listen();

        Assert::array($connectionFaker->committedMessages)
            ->hasCount(1);
    }

    public function doesNotReadIfMessageAlreadyRead(): void
    {
        $topicRegistry = (new TopicRegistry())
            ->add(new Topic('production.fact.products.1', 'products'));

        $connectionFaker = new ConnectionFaker($topicRegistry);

        $message = MessageFactory::for()
            ->withTopicKey('products')
            ->withHeaders(['foo' => 'bar'])
            ->make('test-message');

        $connectionFaker->addMessage($message);

        $repository = new IdempotencyMessageRepository(new ArrayRepositorySource());
        $repository->commit(new ConsumerMessage($message));

        $workerRegistry = (new Bus\Listeners\Workers\MemoryWorkerRegistry())
            ->add(
                new Bus\Listeners\Workers\Worker(
                    name: 'default-listener',
                    routes: ConsumerRoutesBuilder::make($topicRegistry)
                        ->add(new RouteInfo('products', new ConsumerHandlerFaker()))
                        ->build(),
                    options: new Bus\Listeners\Workers\Options(
                        middleware: [new ConsumerCommiterMiddleware($repository)]
                    )
                )
            );

        self::buildBus($connectionFaker, $workerRegistry)
            ->listener('default-listener')
            ->listen();

        Assert::array($connectionFaker->committedMessages)
            ->hasCount(1);
    }

    public function doesNotReadIfMaxAttemptExceeded(): void
    {
        $topicRegistry = (new TopicRegistry())
            ->add(new Topic('production.fact.products.1', 'products'));

        $connectionFaker = new ConnectionFaker($topicRegistry);

        $message = MessageFactory::for()
            ->withTopicKey('products')
            ->withHeaders([IdempotencyMessageRepository::HEADER_NAME => 'over-limit'])
            ->make('test-message');

        $connectionFaker->addMessage($message);

        $source = new ArrayRepositorySource();
        $source->increment('over-limit-production.fact.products.1');
        $source->increment('over-limit-production.fact.products.1');

        $repository = new IdempotencyMessageRepository($source);

        $workerRegistry = (new Bus\Listeners\Workers\MemoryWorkerRegistry())
            ->add(
                new Bus\Listeners\Workers\Worker(
                    name: 'default-listener',
                    routes: ConsumerRoutesBuilder::make($topicRegistry)
                        ->add(new RouteInfo('products', new class {
                            public function __invoke(string $message): void
                            {
                                throw new RuntimeException($message);
                            }
                        }))
                        ->build(),
                    options: new Bus\Listeners\Workers\Options(
                        middleware: [new ConsumerCommiterMiddleware($repository, maxAttempt: 1)]
                    )
                )
            );

        self::buildBus($connectionFaker, $workerRegistry)
            ->listener('default-listener')
            ->listen();

        Assert::array($connectionFaker->committedMessages)
            ->hasCount(1);
    }

    public function incrementsFailedAttemptAndRethrowsException(): void
    {
        $topicRegistry = (new TopicRegistry())
            ->add(new Topic('production.fact.products.1', 'products'));

        $connectionFaker = new ConnectionFaker($topicRegistry);

        $message = MessageFactory::for()
            ->withTopicKey('products')
            ->withHeaders([IdempotencyMessageRepository::HEADER_NAME => 'failing'])
            ->make('test-message');

        $connectionFaker->addMessage($message);

        $source = new ArrayRepositorySource();
        $source->increment('failing-production.fact.products.1');

        $repository = new IdempotencyMessageRepository($source);

        $workerRegistry = (new Bus\Listeners\Workers\MemoryWorkerRegistry())
            ->add(
                new Bus\Listeners\Workers\Worker(
                    name: 'default-listener',
                    routes: ConsumerRoutesBuilder::make($topicRegistry)
                        ->add(new RouteInfo('products', new class {
                            public function __invoke(string $message): void
                            {
                                throw new RuntimeException($message);
                            }
                        }))
                        ->build(),
                    options: new Bus\Listeners\Workers\Options(
                        middleware: [new ConsumerCommiterMiddleware($repository)]
                    )
                )
            );

        try {
            self::buildBus($connectionFaker, $workerRegistry)
                ->listener('default-listener')
                ->listen();

            Assert::fail('RuntimeException was expected');
        } catch (MessageConsumerNotHandledException $exception) {
            Assert::same($exception->getPrevious()?->getMessage(), 'test-message');
        }

        Assert::array($connectionFaker->committedMessages)
            ->hasCount(0);

        Assert::same($source->get('failing-production.fact.products.1')?->number, 2);
        Assert::null($source->get('failing-production.fact.products.1')?->commitedAt);
    }

    private static function buildBus(
        ConnectionFaker                            $connectionFaker,
        Bus\Listeners\Workers\MemoryWorkerRegistry $workerRegistry,
    ): Bus
    {
        return new Bus(
            new Bus\ThreadRegistry(
                new ConnectionRegistryFaker($connectionFaker),
                new Bus\ThreadFactory(
                    new Bus\Listeners\ListenerFactory(workerRegistry: $workerRegistry),
                    new Bus\Publishers\PublisherFactory(),
                )
            ),
            ConnectionRegistry::DEFAULT_CONNECTION_NAME
        );
    }
}
