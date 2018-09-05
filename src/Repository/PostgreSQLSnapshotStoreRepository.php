<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Repository;

use Pccomponentes\Ddd\Domain\Model\Snapshot;
use Pccomponentes\Ddd\Domain\Model\ValueObject\Uuid;
use Pccomponentes\Ddd\Infrastructure\Repository\SnapshotStoreRepository;

class PostgreSQLSnapshotStoreRepository extends PostgreSQLBaseAggregateRepository implements SnapshotStoreRepository
{
    protected function table(): string
    {
        return PostgreSQLSchemaCreation::SNAPSHOT_STORE;
    }

    public function add(Snapshot $snapshot): void
    {
        $this->insert($snapshot);
    }

    public function get(Uuid $aggregateId): ?Snapshot
    {
        return $this->findOneByAggregateId($aggregateId);
    }

    public function remove(Snapshot $snapshot): void
    {
        $this->delete($snapshot);
    }
}
