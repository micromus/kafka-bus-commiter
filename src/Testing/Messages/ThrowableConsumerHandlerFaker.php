<?php

namespace Micromus\KafkaBusCommiter\Testing\Messages;

use Exception;
use Micromus\KafkaBus\Consumers\Messages\ConsumerMessage;
use Micromus\KafkaBus\Interfaces\Consumers\Messages\ConsumerMessageInterface;

/**
 * @internal
 */
final class ThrowableConsumerHandlerFaker
{
    /**
     * @param ConsumerMessage $message
     * @return void
     *
     * @throws Exception
     */
    public function __invoke(ConsumerMessageInterface $message): void
    {
        throw new Exception();
    }
}
