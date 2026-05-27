<?php

namespace Micromus\KafkaBusCommiter;

use DateTimeImmutable;

final readonly class Attempt
{
    public function __construct(
        public int $number = 1,
        public ?DateTimeImmutable $commitedAt = null,
    ) {
    }
}
