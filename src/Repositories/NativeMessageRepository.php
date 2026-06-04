<?php

namespace Micromus\KafkaBusCommiter\Repositories;

use DateTimeImmutable;
use Micromus\KafkaBus\Interfaces\Consumers\Messages\ConsumerMessageInterface;
use Micromus\KafkaBusCommiter\Attempt;
use Micromus\KafkaBusCommiter\Interfaces\ConsumerMessageRepositoryInterface;
use Micromus\KafkaBusCommiter\Interfaces\RepositorySourceInterface;

final readonly class NativeMessageRepository implements ConsumerMessageRepositoryInterface
{
    public function __construct(
        private RepositorySourceInterface $source,
    ) {
    }

    public function attempt(ConsumerMessageInterface $message): Attempt
    {
        return $this->source->get($message->msgId())
            ?? new Attempt($message->msgId(), 1, new DateTimeImmutable());
    }

    public function failed(ConsumerMessageInterface $message): void
    {
        $this->source->increment($message->msgId());
    }

    public function commit(ConsumerMessageInterface $message): void
    {
        $this->source->commit($message->msgId());
    }
}
