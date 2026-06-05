<?php

namespace Micromus\KafkaBusCommiter\Tests\Fakes;

use Micromus\KafkaBus\Interfaces\Pipelines\PipelineHandlerInterface;
use Micromus\KafkaBus\Interfaces\Producers\Messages\ProducerMessageInterface;

final class FakeProducerPipelineHandler implements PipelineHandlerInterface
{
    /**
     * @var array<string, mixed>
     */
    public array $headers = [];

    public function __construct(
        private ProducerMessageInterface $target,
    ) {
    }

    public function withHeader(string $key, mixed $value): self
    {
        $this->headers[$key] = $value;

        return $this;
    }

    public function target(): mixed
    {
        return $this->target;
    }

    public function handle(): mixed
    {
        return null;
    }
}
