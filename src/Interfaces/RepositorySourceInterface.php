<?php

namespace Micromus\KafkaBusCommiter\Interfaces;

use Micromus\KafkaBusCommiter\Attempt;

interface RepositorySourceInterface
{
    public function get(string $key): ?Attempt;

    public function increment(string $key): void;

    public function commit(string $key): void;
}
