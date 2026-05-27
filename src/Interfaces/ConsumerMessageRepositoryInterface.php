<?php

namespace Micromus\KafkaBusCommiter\Interfaces;

use Micromus\KafkaBus\Interfaces\Consumers\Messages\ConsumerMessageInterface;
use Micromus\KafkaBusCommiter\Attempt;

interface ConsumerMessageRepositoryInterface
{
    public function attempt(ConsumerMessageInterface $message): Attempt;

    public function commit(ConsumerMessageInterface $message): void;

    public function exists(ConsumerMessageInterface $message): bool;
}
