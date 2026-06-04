<?php

namespace Micromus\KafkaBusCommiter\Middleware;

use Micromus\KafkaBus\Interfaces\Pipelines\PipelineInterface;
use Micromus\KafkaBus\Producers\Pipelines\ProducerPipelineHandler;
use Micromus\KafkaBus\Producers\Pipelines\ProducerPipelineMiddleware;
use Micromus\KafkaBusCommiter\Interfaces\HasIdempotency;
use Micromus\KafkaBusCommiter\Repositories\IdempotencyMessageRepository;

final readonly class ProducerIdempotencyMiddleware implements ProducerPipelineMiddleware
{
    /**
     * @param PipelineInterface<ProducerPipelineHandler> $pipeline
     * @return PipelineInterface<ProducerPipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $message = $pipeline->handler()
            ->target();

        if ($message instanceof HasIdempotency) {
            $pipeline->handler()
                ->withHeader(IdempotencyMessageRepository::HEADER_NAME, $message->getIdempotencyKey());
        }

        return $pipeline->continue();
    }
}
