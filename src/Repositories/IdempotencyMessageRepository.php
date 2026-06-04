<?php

namespace Micromus\KafkaBusCommiter\Repositories;

use Micromus\KafkaBus\Interfaces\Consumers\Messages\ConsumerMessageInterface;
use Micromus\KafkaBusCommiter\Attempt;
use Micromus\KafkaBusCommiter\Interfaces\ConsumerMessageRepositoryInterface;
use Micromus\KafkaBusCommiter\Interfaces\RepositorySourceInterface;

final readonly class IdempotencyMessageRepository implements ConsumerMessageRepositoryInterface
{
    public const HEADER_NAME = 'x-idempotency-key';

    public function __construct(
        private RepositorySourceInterface $source,
    ) {
    }

    public function attempt(ConsumerMessageInterface $message): Attempt
    {
        $messageId = $this->getKey($message);

        return $this->source->get($this->getKey($message))
            ?? new Attempt($messageId);
    }

    public function commit(ConsumerMessageInterface $message): void
    {
        $this->source->commit($this->getKey($message));
    }

    public function failed(ConsumerMessageInterface $message): void
    {
        $this->source->increment($this->getKey($message));
    }

    private function getKey(ConsumerMessageInterface $message): string
    {
        $headers = $message->headers();

        if (array_key_exists(self::HEADER_NAME, $headers)) {
            return "{$headers[self::HEADER_NAME]}-{$message->topicName()}";
        }

        return $message->msgId();
    }
}
