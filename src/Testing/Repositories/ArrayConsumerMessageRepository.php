<?php

namespace Micromus\KafkaBusCommiter\Testing\Repositories;

use DateTimeImmutable;
use Micromus\KafkaBus\Interfaces\Consumers\Messages\ConsumerMessageInterface;
use Micromus\KafkaBusCommiter\Attempt;
use Micromus\KafkaBusCommiter\Interfaces\ConsumerMessageRepositoryInterface;

final class ArrayConsumerMessageRepository implements ConsumerMessageRepositoryInterface
{
    /**
     * @var array<string, Attempt>
     */
    protected array $commited = [];

    public function commit(ConsumerMessageInterface $message): void
    {
        $attempt = $this->commited[$message->msgId()] ?? null;

        $this->commited[$message->msgId()] = $attempt == null
            ? new Attempt(1, new DateTimeImmutable())
            : new Attempt($attempt->number, new DateTimeImmutable());
    }

    public function exists(ConsumerMessageInterface $message): bool
    {
        return isset($this->commited[$message->msgId()]);
    }

    public function attempt(ConsumerMessageInterface $message): Attempt
    {
        $attempt = $this->commited[$message->msgId()] ?? null;

        return $this->commited[$message->msgId()] = $attempt == null
            ? new Attempt(1)
            : new Attempt($attempt->number + 1);
    }
}
