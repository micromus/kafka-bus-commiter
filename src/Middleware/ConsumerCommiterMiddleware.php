<?php

namespace Micromus\KafkaBusCommiter\Middleware;

use Exception;
use Micromus\KafkaBus\Consumers\Pipelines\ConsumerPipelineHandler;
use Micromus\KafkaBus\Consumers\Pipelines\ConsumerPipelineMiddleware;
use Micromus\KafkaBus\Interfaces\Pipelines\PipelineInterface;
use Micromus\KafkaBusCommiter\Interfaces\ConsumerMessageRepositoryInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

use function is_null;

final readonly class ConsumerCommiterMiddleware implements ConsumerPipelineMiddleware
{
    public function __construct(
        private ConsumerMessageRepositoryInterface $repository,
        private LoggerInterface $logger = new NullLogger(),
        private int $maxAttempt = -1
    ) {
    }

    /**
     * @param PipelineInterface<ConsumerPipelineHandler> $pipeline
     * @return PipelineInterface<ConsumerPipelineHandler>
     *
     * @throws Exception
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $message = $pipeline->handler()
            ->target();

        $attempt = $this->repository->attempt($message);

        $context = [
            'worker' => $message->workerName(),
            'msg_id' => $attempt->key,
            'topic_name' => $message->topicName(),
            'partition' => $message->original()->partition,
            'offset' => $message->original()->offset,
        ];

        if (!is_null($attempt->commitedAt)) {
            $this->logger
                ->warning("Message #$attempt->key already read", $context);

            return $pipeline;
        }

        if ($this->maxAttempt > 0 && $attempt->number > $this->maxAttempt) {
            $this->logger
                ->error("Message #$attempt->key number of read attempts has been exceeded", $context);

            return $pipeline;
        }

        try {
            $pipeline->continue();

            $this->repository->commit($message);

            $this->logger
                ->debug("Message #$attempt->key successfully read", $context);

            return $pipeline;
        }
        catch (Exception $exception) {
            $this->repository->failed($message);

            throw $exception;
        }
    }
}
