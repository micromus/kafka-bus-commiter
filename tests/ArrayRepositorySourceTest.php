<?php

use Micromus\KafkaBusCommiter\Repositories\ArrayRepositorySource;
use Testo\Assert;
use Testo\Test;

#[Test]
function array_repository_source_can_increment_and_commit(): void
{
    $source = new ArrayRepositorySource();

    Assert::null($source->get('key-1'));

    $source->increment('key-1');

    Assert::same($source->get('key-1')?->number, 1);
    Assert::null($source->get('key-1')?->commitedAt);

    $source->increment('key-1');

    Assert::same($source->get('key-1')?->number, 2);
    Assert::null($source->get('key-1')?->commitedAt);

    $source->commit('key-1');

    Assert::same($source->get('key-1')?->number, 2);
    Assert::notNull($source->get('key-1')?->commitedAt);
}
