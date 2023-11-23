<?php
declare(strict_types=1);

namespace PcComponentes\DddPostgreSQL\Repository;

use Doctrine\DBAL\ParameterType;
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

    public function getPaginated(Uuid $aggregateId, int $offset, int $limit): array
    {
        return $this->queryByAggregateIdPaginated($aggregateId, $offset, $limit);
    }

    public function getGivenEventsByAggregate(Uuid $aggregateId, int $offset, int $limit, string ...$events): array
    {
        return $this->queryGivenEventsByAggregateIdPaginated($aggregateId, $offset, $limit, ...$events);
    }

    public function getEventsFilteredByAggregate(Uuid $aggregateId, int $offset, int $limit, string ...$events): array
    {
        return $this->queryEventsFilteredByAggregateIdPaginated($aggregateId, $offset, $limit, ...$events);
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

    public function countGivenEventsByAggregate(Uuid $aggregateId, string ...$events): int
    {
        return $this->countGivenEventsByAggregateId($aggregateId, ...$events);
    }

    public function countEventsFilteredByAggregate(Uuid $aggregateId, string ...$events): int
    {
        return $this->countFilteredEventsByAggregateId($aggregateId, ...$events);
    }

    public function countEventsForSince(Uuid $aggregateId, DateTimeValueObject $since): int
    {
        return $this->countByAggregateIdSince($aggregateId, $since);
    }

    public function countEventsForSinceVersion(Uuid $aggregateId, int $aggregateVersion): int
    {
        return $this->countByAggregateIdSinceVersion($aggregateId, $aggregateVersion);
    }

    public function getAll(int $offset, int $limit): array
    {
        $stmt = $this->connection->prepare(
            \sprintf(
                'select message_id, aggregate_id, aggregate_version, occurred_on, message_name, payload
                from %s
                LIMIT :limit OFFSET :offset',
                $this->tableName(),
            ),
        );
        $stmt->bindValue('limit', $limit, ParameterType::INTEGER);
        $stmt->bindValue('offset', $offset, ParameterType::INTEGER);
        $result = $stmt->executeQuery();

        return $result->fetchAllAssociative();
    }

    protected function tableName(): string
    {
        return PostgresMessageStoreDefinition::EVENT_STORE;
    }
}
