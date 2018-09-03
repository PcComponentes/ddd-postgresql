<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Repository;

use Pccomponentes\Ddd\Domain\Model\Snapshot;
use Pccomponentes\Ddd\Domain\Model\ValueObject\Uuid;
use Pccomponentes\Ddd\Infrastructure\Repository\SnapshotStoreRepository;
use Pccomponentes\Ddd\Util\Serializer\SnapshotUnserializable;

class PostgreSQLSnapshotStoreRepository extends PostgreSQLBaseAggregateRepository implements SnapshotStoreRepository
{
    private const TABLE = 'snapshot_store';
    private $unserializer;

    final public function __construct(\PDO $connection, SnapshotUnserializable $unserializer)
    {
        parent::__construct($connection);
        $this->unserializer = $unserializer;
    }

    public function add(Snapshot $snapshot): void
    {
        $this->insertStatement($snapshot, self::TABLE)->execute();
    }

    public function get(Uuid $aggregateId): ?Snapshot
    {
        $stm = $this->getStatement($aggregateId,self::TABLE);
        $stm->execute();
        $snapshot = $stm->fetch(\PDO::FETCH_ASSOC);

        return $snapshot ? $this->unserializer->unserialize($snapshot) : null;
    }

    public function remove(Snapshot $snapshot): void
    {
        $this->removeStatement($snapshot->aggregateId(), self::TABLE)->execute();
    }
}
