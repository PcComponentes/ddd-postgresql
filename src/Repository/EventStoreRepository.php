<?php declare(strict_types=1);

namespace PcComponentes\DddPostgreSQL\Repository;

use PcComponentes\Ddd\Domain\Model\ValueObject\DateTimeValueObject;
use PcComponentes\Ddd\Domain\Model\ValueObject\Uuid;
use PcComponentes\Ddd\Infrastructure\Repository\EventStoreRepository as DddEventStoreRepository;

interface EventStoreRepository extends DddEventStoreRepository
{
    public function countEventsFor(Uuid $aggregateId): int;
    public function countEventsForSince(Uuid $aggregateId, DateTimeValueObject $since): int;
    public function getGivenEventsByAggregate(Uuid $aggregateId, int $page, int $pageSize, string ...$events): array;
    public function countEventsForSinceVersion(Uuid $aggregateId, int $aggregateVersion): int;
    public function getSinceVersion(Uuid $aggregateId, int $aggregateVersion): array;
    public function getPaginated(Uuid $aggregateId, int $page, int $pageSize): array;
    public function getAll(int $page, int $pageSize): array;
}
