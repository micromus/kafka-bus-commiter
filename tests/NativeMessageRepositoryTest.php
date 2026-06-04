<?php

namespace Micromus\KafkaBusCommiter\Tests;

use Micromus\KafkaBus\Consumers\Messages\ConsumerMessage;
use Micromus\KafkaBus\Testing\Consumers\MessageFactory;
use Micromus\KafkaBusCommiter\Repositories\ArrayRepositorySource;
use Micromus\KafkaBusCommiter\Repositories\NativeMessageRepository;
use Testo\Assert;
use Testo\Test;

#[Test]
final class NativeMessageRepositoryTest
{
    public function usesMessageIdForAttemptFailedAndCommit(): void
    {
        $source = new ArrayRepositorySource();
        $repository = new NativeMessageRepository($source);

        $message = new ConsumerMessage(
            MessageFactory::for()
                ->withTopicKey('products')
                ->withHeaders(['foo' => 'bar'])
                ->make('payload')
        );

        $attempt = $repository->attempt($message);
        Assert::same($attempt->key, $message->msgId());
        Assert::same($attempt->number, 1);
        Assert::notNull($attempt->commitedAt);

        $repository->failed($message);
        Assert::same($source->get($message->msgId())?->number, 1);
        Assert::null($source->get($message->msgId())?->commitedAt);

        $repository->commit($message);
        Assert::notNull($source->get($message->msgId())?->commitedAt);
    }
}
