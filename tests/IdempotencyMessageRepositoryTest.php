<?php

namespace Micromus\KafkaBusCommiter\Tests;

use Micromus\KafkaBus\Consumers\Messages\ConsumerMessage;
use Micromus\KafkaBus\Testing\Consumers\MessageFactory;
use Micromus\KafkaBusCommiter\Repositories\ArrayRepositorySource;
use Micromus\KafkaBusCommiter\Repositories\IdempotencyMessageRepository;
use Testo\Assert;
use Testo\Test;

#[Test]
final class IdempotencyMessageRepositoryTest
{
    public function usesHeaderKeyAndDelegatesToSource(): void
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

    public function fallsBackToMessageIdWhenHeaderMissing(): void
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
}
