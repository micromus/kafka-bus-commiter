<?php

namespace Micromus\KafkaBusCommiter\Interfaces;

interface HasIdempotency
{
    public function getIdempotencyKey(): string;
}
