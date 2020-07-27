<?php
declare(strict_types=1);

namespace PcComponentes\DddPostgreSQL\Repository;

use PcComponentes\Ddd\Domain\Model\Snapshot;
use PcComponentes\Ddd\Domain\Model\ValueObject\Uuid;
use PcComponentes\Ddd\Infrastructure\Repository\SnapshotStoreRepository;
use PcComponentes\DddPostgreSQL\Migration\PostgresMessageStoreDefinition;

final class PostgresSnapshotStoreRepository extends PostgresBaseAggregateRepository implements SnapshotStoreRepository
{
    public function set(Snapshot $snapshot): void
    {
        $this->forceInsert($snapshot);
    }

    public function get(Uuid $aggregateId): ?Snapshot
    {
        return $this->findOneByAggregateId($aggregateId);
    }

    public function remove(Snapshot $snapshot): void
    {
        // TODO: Implement remove() method.
    }

    protected function tableName(): string
    {
        return PostgresMessageStoreDefinition::SNAPSHOT_STORE;
    }
}
