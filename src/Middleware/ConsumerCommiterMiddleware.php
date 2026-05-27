<?php

namespace Micromus\KafkaBusCommiter\Middleware;

use Micromus\KafkaBus\Consumers\Pipelines\ConsumerPipelineHandler;
use Micromus\KafkaBus\Consumers\Pipelines\ConsumerPipelineMiddleware;
use Micromus\KafkaBus\Interfaces\Pipelines\PipelineInterface;
use Micromus\KafkaBusCommiter\Interfaces\ConsumerMessageRepositoryInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final readonly class ConsumerCommiterMiddleware implements ConsumerPipelineMiddleware
{
    public function __construct(
        private ConsumerMessageRepositoryInterface $consumerMessageRepository,
        private LoggerInterface $logger = new NullLogger(),
        private int $maxAttempt = -1
    ) {
    }

    /**
     * @param PipelineInterface<ConsumerPipelineHandler> $pipeline
     * @return PipelineInterface<ConsumerPipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $message = $pipeline->handler()
            ->target();

        $context = [
            'worker' => $message->workerName(),
            'msg_id' => $message->msgId(),
        ];

        $attempt = $this->consumerMessageRepository
            ->attempt($message);

        if (!\is_null($attempt->commitedAt)) {
            $this->logger
                ->warning("Message #{$message->msgId()} already read", $context);

            return $pipeline;
        }

        if ($this->maxAttempt > 0 && $attempt->number > $this->maxAttempt) {
            $this->logger
                ->error("Message #{$message->msgId()} number of read attempts has been exceeded", $context);

            return $pipeline;
        }

        $pipeline->continue();

        $this->consumerMessageRepository
            ->commit($message);

        $this->logger
            ->debug("Message #{$message->msgId()} successfully read", $context);

        return $pipeline;
    }
}
