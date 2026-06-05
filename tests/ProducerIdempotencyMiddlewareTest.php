<?php

namespace Micromus\KafkaBusCommiter\Tests;

use Micromus\KafkaBus\Interfaces\Producers\Messages\ProducerMessageInterface;
use Micromus\KafkaBusCommiter\Interfaces\HasIdempotency;
use Micromus\KafkaBusCommiter\Middleware\ProducerIdempotencyMiddleware;
use Micromus\KafkaBusCommiter\Repositories\IdempotencyMessageRepository;
use Micromus\KafkaBusCommiter\Tests\Fakes\FakePipeline;
use Micromus\KafkaBusCommiter\Tests\Fakes\FakeProducerPipelineHandler;
use Testo\Assert;
use Testo\Test;

#[Test]
final class ProducerIdempotencyMiddlewareTest
{
    public function setsHeaderForHasIdempotencyMessage(): void
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

    public function doesNotSetHeaderForRegularMessage(): void
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
}
