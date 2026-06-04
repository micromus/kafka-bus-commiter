<?php

use Micromus\KafkaBus\Interfaces\Pipelines\PipelineHandlerInterface;
use Micromus\KafkaBus\Interfaces\Pipelines\PipelineInterface;
use Micromus\KafkaBus\Interfaces\Producers\Messages\ProducerMessageInterface;
use Micromus\KafkaBusCommiter\Interfaces\HasIdempotency;
use Micromus\KafkaBusCommiter\Middleware\ProducerIdempotencyMiddleware;
use Micromus\KafkaBusCommiter\Repositories\IdempotencyMessageRepository;
use Testo\Assert;
use Testo\Test;

#[Test]
function producer_idempotency_middleware_sets_header_for_has_idempotency_message(): void
{
    $message = new class implements ProducerMessageInterface, HasIdempotency {
        public function toPayload(): string
        {
            return 'payload';
        }

        public function getIdempotencyKey(): string
        {
            return 'idem-1';
        }
    };

    $handler = new FakeProducerPipelineHandler($message);
    $pipeline = new FakePipeline($handler);

    (new ProducerIdempotencyMiddleware())->handle($pipeline);

    Assert::same($handler->headers[IdempotencyMessageRepository::HEADER_NAME] ?? null, 'idem-1');
    Assert::true($pipeline->continued);
}

#[Test]
function producer_idempotency_middleware_does_not_set_header_for_regular_message(): void
{
    $message = new class implements ProducerMessageInterface {
        public function toPayload(): string
        {
            return 'payload';
        }
    };

    $handler = new FakeProducerPipelineHandler($message);
    $pipeline = new FakePipeline($handler);

    (new ProducerIdempotencyMiddleware())->handle($pipeline);

    Assert::false(array_key_exists(IdempotencyMessageRepository::HEADER_NAME, $handler->headers));
    Assert::true($pipeline->continued);
}

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
