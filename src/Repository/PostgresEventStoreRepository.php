<?php declare(strict_types=1);

namespace PcComponentes\DddPostgreSQL\Repository;

use PcComponentes\Ddd\Domain\Model\DomainEvent;
use PcComponentes\Ddd\Domain\Model\ValueObject\DateTimeValueObject;
use PcComponentes\Ddd\Domain\Model\ValueObject\Uuid;
use PcComponentes\DddPostgreSQL\Migration\PostgresMessageStoreDefinition;

final class PostgresEventStoreRepository extends PostgresBaseAggregateRepository implements EventStoreRepository
{
    public function add(DomainEvent ...$events): void
    {
        foreach ($events as $theEvent) {
            $this->insert($theEvent);
        }
    }

    public function get(Uuid $aggregateId): array
    {
        return $this->findByAggregateId($aggregateId);
    }

    public function getPaginated(Uuid $aggregateId, int $page, int $pageSize): array
    {
        return $this->queryByAggregateIdPaginated($aggregateId, $page, $pageSize);
    }

    public function getGivenEventsByAggregate(Uuid $aggregateId, int $page, int $pageSize, string ...$events): array
    {
        return $this->queryGivenEventsByAggregateIdPaginated($aggregateId, $page, $pageSize, ...$events);
    }

    public function getSince(Uuid $aggregateId, DateTimeValueObject $since): array
    {
        return $this->findByAggregateIdSince($aggregateId, $since);
    }

    public function getSinceVersion(Uuid $aggregateId, int $aggregateVersion): array
    {
        return $this->findByAggregateIdSinceVersion($aggregateId, $aggregateVersion);
    }

    public function getByMessageId(Uuid $messageId): ?DomainEvent
    {
        return $this->findByMessageId($messageId);
    }

    public function getByMessageName(string $messageName): array
    {
        return [];
    }

    public function getByMessageNameSince(string $messageName, DateTimeValueObject $since): array
    {
        return [];
    }

    public function countEventsFor(Uuid $aggregateId): int
    {
        return $this->countByAggregateId($aggregateId);
    }

    public function countEventsForSince(Uuid $aggregateId, DateTimeValueObject $since): int
    {
        return $this->countByAggregateIdSince($aggregateId, $since);
    }

    public function countEventsForSinceVersion(Uuid $aggregateId, int $aggregateVersion): int
    {
        return $this->countByAggregateIdSinceVersion($aggregateId, $aggregateVersion);
    }

    public function getAll(int $page, int $pageSize): array
    {
        $offset = $page * $pageSize;

        $stmt = $this->connection->prepare(
            \sprintf(
                'select message_id, aggregate_id, aggregate_version, occurred_on, message_name, payload
                from %s
                LIMIT :pageSize OFFSET :offset',
                $this->tableName(),
            ),
        );
        $stmt->bindValue('pageSize', $pageSize, \PDO::PARAM_INT);
        $stmt->bindValue('offset', $offset, \PDO::PARAM_INT);
        $this->execute($stmt);

        return $stmt->fetchAll();
    }

    protected function tableName(): string
    {
        return PostgresMessageStoreDefinition::EVENT_STORE;
    }
}
