<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Repository;

use Pccomponentes\Ddd\Domain\Model\ValueObject\Uuid;
use Pccomponentes\Ddd\Infrastructure\Repository\ProjectionRepository;
use Pccomponentes\Ddd\Projection\Model\Projection;
use Pccomponentes\Ddd\Util\Serializer\ProjectionUnserializable;

abstract class PostgreSQLProjectionRepository extends PostgreSQLBaseAggregateRepository implements ProjectionRepository
{
    private $unserializer;
    private $tableName;

    public function __construct(\PDO $connection, ProjectionUnserializable $unserializer, string $tableName)
    {
        parent::__construct($connection);
        $this->unserializer = $unserializer;
        $this->tableName = $tableName;
    }

    protected function unserializer(): ProjectionUnserializable
    {
        return $this->unserializer;
    }

    public function add(Projection $projection): void
    {
        $this->forceInsertStatement($projection, $this->tableName)->execute();
    }

    public function find(Uuid $aggregateId): ?Projection
    {
        $stm = $this->getStatement($aggregateId, $this->tableName);
        $stm->execute();
        $projection = $stm->fetch(\PDO::FETCH_ASSOC);

        return $projection ? $this->unserializer->unserialize($projection) : null;
    }

    public function remove(Projection $projection): void
    {
        $this->removeStatement($projection->aggregateId(), $this->tableName)->execute();
    }
}
