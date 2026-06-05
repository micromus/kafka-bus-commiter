<?php

namespace Micromus\KafkaBusCommiter\Tests\Fakes;

use Micromus\KafkaBus\Interfaces\Pipelines\PipelineHandlerInterface;
use Micromus\KafkaBus\Interfaces\Pipelines\PipelineInterface;

final class FakePipeline implements PipelineInterface
{
    public bool $continued = false;

    public function __construct(
        private PipelineHandlerInterface $handler,
    ) {
    }

    public function handler(): PipelineHandlerInterface
    {
        return $this->handler;
    }

    public function continue(): PipelineInterface
    {
        $this->continued = true;

        return $this;
    }
}
