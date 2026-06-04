<?php

use Micromus\KafkaBus\Bus;
use Micromus\KafkaBus\Connections\Registry\ConnectionRegistry;
use Micromus\KafkaBus\Consumers\Messages\ConsumerMessage;
use Micromus\KafkaBus\Consumers\Router\ConsumerRoutesBuilder;
use Micromus\KafkaBus\Consumers\Router\RouteInfo;
use Micromus\KafkaBus\Testing\Connections\ConnectionFaker;
use Micromus\KafkaBus\Testing\Connections\ConnectionRegistryFaker;
use Micromus\KafkaBus\Testing\Consumers\MessageFactory;
use Micromus\KafkaBus\Testing\Messages\ConsumerHandlerFaker;
use Micromus\KafkaBus\Topics\Topic;
use Micromus\KafkaBus\Topics\TopicRegistry;
use Micromus\KafkaBusCommiter\Middleware\ConsumerCommiterMiddleware;
use Micromus\KafkaBusCommiter\Repositories\ArrayRepositorySource;
use Micromus\KafkaBusCommiter\Repositories\IdempotencyMessageRepository;
use Micromus\KafkaBusCommiter\Testing\Repositories\ArrayConsumerMessageRepository;
use Testo\Assert;
use Testo\Test;

#[Test]
function can_consume_message(): void
{
    $topicRegistry = (new TopicRegistry())
        ->add(new Topic('production.fact.products.1', 'products'));

    $connectionFaker = new ConnectionFaker($topicRegistry);

    $message = MessageFactory::for()
        ->withTopicKey('products')
        ->withHeaders(['foo' => 'bar'])
        ->make('test-message');

    $connectionFaker->addMessage($message);

    $source = new ArrayRepositorySource();
    $repository = new IdempotencyMessageRepository($source);

    $consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
        ->add(new RouteInfo('products', new ConsumerHandlerFaker()))
        ->build();

    $workerRegistry = (new Bus\Listeners\Workers\MemoryWorkerRegistry())
        ->add(
            new Bus\Listeners\Workers\Worker(
                name: 'default-listener',
                routes: $consumerRoutes,
                options: new Bus\Listeners\Workers\Options(middleware: [new ConsumerCommiterMiddleware($repository)])
            )
        );

    $bus = new Bus(
        new Bus\ThreadRegistry(
            new ConnectionRegistryFaker($connectionFaker),
            new Bus\ThreadFactory(
                new Bus\Listeners\ListenerFactory(workerRegistry: $workerRegistry),
                new Bus\Publishers\PublisherFactory(),
            )
        ),
        ConnectionRegistry::DEFAULT_CONNECTION_NAME
    );

    $bus->listener('default-listener')
        ->listen();

    Assert::array($connectionFaker->committedMessages)
        ->hasCount(1);
}

#[Test]
function consume_message_not_read_if_message_already_read() {
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

    $consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
        ->add(new RouteInfo('products', new ConsumerHandlerFaker()))
        ->build();

    $workerRegistry = (new Bus\Listeners\Workers\MemoryWorkerRegistry())
        ->add(
            new Bus\Listeners\Workers\Worker(
                name: 'default-listener',
                routes: $consumerRoutes,
                options: new Bus\Listeners\Workers\Options(middleware: [new ConsumerCommiterMiddleware($repository)])
            )
        );

    $bus = new Bus(
        new Bus\ThreadRegistry(
            new ConnectionRegistryFaker($connectionFaker),
            new Bus\ThreadFactory(
                new Bus\Listeners\ListenerFactory(workerRegistry: $workerRegistry),
                new Bus\Publishers\PublisherFactory(),
            )
        ),
        ConnectionRegistry::DEFAULT_CONNECTION_NAME
    );

    $bus->listener('default-listener')
        ->listen();

    Assert::array($connectionFaker->committedMessages)
        ->hasCount(1);
}
