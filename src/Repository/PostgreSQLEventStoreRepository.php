<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Repository;

use Pccomponentes\Ddd\Domain\Model\DomainEvent;
use Pccomponentes\Ddd\Domain\Model\ValueObject\DateTimeValueObject;
use Pccomponentes\Ddd\Domain\Model\ValueObject\Uuid;
use Pccomponentes\Ddd\Infrastructure\Repository\EventStoreRepository;

class PostgreSQLEventStoreRepository extends PostgreSQLBaseAggregateRepository implements EventStoreRepository
{
    protected function table(): string
    {
        return PostgreSQLSchemaCreation::EVENT_STORE;
    }

    public function add(DomainEvent ...$events): void
    {
        \array_walk($events, [$this, 'insert']);
    }

    public function get(Uuid $aggregateId): array
    {
        return $this->findByAggregateId($aggregateId);
    }

    public function getSince(Uuid $aggregateId, DateTimeValueObject $since): array
    {
        return $this->findByAggregateIdSince($aggregateId, $since);
    }
}
