<?php

use Micromus\KafkaBus\Consumers\Messages\ConsumerMessage;
use Micromus\KafkaBus\Testing\Consumers\MessageFactory;
use Micromus\KafkaBusCommiter\Repositories\ArrayRepositorySource;
use Micromus\KafkaBusCommiter\Repositories\IdempotencyMessageRepository;
use Testo\Assert;
use Testo\Test;

#[Test]
function idempotency_repository_uses_header_key_and_delegates_to_source(): void
{
    $source = new ArrayRepositorySource();
    $repository = new IdempotencyMessageRepository($source);

    $message = new ConsumerMessage(
        MessageFactory::for()
            ->withTopicKey('products')
            ->withHeaders([IdempotencyMessageRepository::HEADER_NAME => 'order-123'])
            ->make('payload')
    );

    Assert::same($repository->attempt($message)->key, 'order-123-products');
    Assert::same($repository->attempt($message)->number, 1);

    $repository->failed($message);
    Assert::same($source->get('order-123-products')?->number, 1);

    $repository->commit($message);
    Assert::notNull($source->get('order-123-products')?->commitedAt);
}

#[Test]
function idempotency_repository_falls_back_to_message_id_when_header_missing(): void
{
    $source = new ArrayRepositorySource();
    $repository = new IdempotencyMessageRepository($source);

    $message = new ConsumerMessage(
        MessageFactory::for()
            ->withTopicKey('products')
            ->withHeaders(['foo' => 'bar'])
            ->make('payload')
    );

    Assert::same($repository->attempt($message)->key, $message->msgId());
}
