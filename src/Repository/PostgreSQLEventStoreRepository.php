<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Repository;

use Pccomponentes\Ddd\Domain\Model\DomainEvent;
use Pccomponentes\Ddd\Domain\Model\ValueObject\DateTimeValueObject;
use Pccomponentes\Ddd\Domain\Model\ValueObject\Uuid;
use Pccomponentes\Ddd\Infrastructure\Repository\EventStoreRepository;
use Pccomponentes\Ddd\Util\Serializer\DomainEventUnserializable;

class PostgreSQLEventStoreRepository extends PostgreSQLBaseAggregateRepository implements EventStoreRepository
{
    private const TABLE = 'event_store';
    private $unserializer;

    final public function __construct(\PDO $connection, DomainEventUnserializable $unserializer)
    {
        parent::__construct($connection);
        $this->unserializer = $unserializer;
    }

    public function add(DomainEvent ...$events): void
    {
        \array_walk($events, [$this, 'insertEvent']);
    }

    public function get(Uuid $aggregateId): array
    {
        $stm = $this->getStatement($aggregateId, self::TABLE);
        return $this->execute($stm);
    }

    public function getSince(Uuid $aggregateId, DateTimeValueObject $since): array
    {
        $stm = $this->getSinceStatement($aggregateId, $since, self::TABLE);
        return $this->execute($stm);
    }

    private function execute(\PDOStatement $statement): array
    {
        $statement->execute();
        $mapped = [];
        while ($event = $statement->fetch(\PDO::FETCH_ASSOC)) {
            $mapped[] = $this->unserializer->unserialize($event);
        }

        return $mapped;
    }

    private function insertEvent(DomainEvent $event): void
    {
        $this->insertStatement($event, self::TABLE)->execute();
    }
}
