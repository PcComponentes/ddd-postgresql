<?php
declare(strict_types=1);

namespace PcComponentes\DddPostgreSQL\Repository;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Statement;
use PcComponentes\Ddd\Domain\Model\ValueObject\DateTimeValueObject;
use PcComponentes\Ddd\Domain\Model\ValueObject\Uuid;
use PcComponentes\Ddd\Util\Message\AggregateMessage;
use PcComponentes\Ddd\Util\Message\Serialization\JsonApi\AggregateMessageStream;
use PcComponentes\Ddd\Util\Message\Serialization\JsonApi\AggregateMessageStreamDeserializer;

abstract class PostgresBaseAggregateRepository
{
    protected Connection $connection;
    protected AggregateMessageStreamDeserializer $deserializer;
    private string $occurredOnFormat;

    final public function __construct(
        Connection $connection,
        AggregateMessageStreamDeserializer $deserializer,
        string $occurredOnFormat = 'U',
    ) {
        $this->connection = $connection;
        $this->deserializer = $deserializer;
        $this->occurredOnFormat = $occurredOnFormat;
    }

    abstract protected function tableName(): string;

    protected function insert(AggregateMessage $message): void
    {
        $stmt = $this->connection->prepare(
            \sprintf(
                'insert into %s
                (message_id, aggregate_id, aggregate_version, occurred_on, message_name, payload)
                values
                (:message_id, :aggregate_id, :aggregate_version, :occurred_on, :message_name, :payload)',
                $this->tableName(),
            ),
        );

        $this->bindAggregateMessageValues($message, $stmt);

        $stmt->executeQuery();
    }

    protected function forceInsert(AggregateMessage $message): void
    {
        $stmt = $this->connection->prepare(
            \sprintf(
                'INSERT INTO %s (message_id, aggregate_id, aggregate_version, occurred_on, message_name, payload)
                VALUES
                (:message_id, :aggregate_id, :aggregate_version, :occurred_on, :message_name, :payload)
                ON CONFLICT (aggregate_id) DO UPDATE SET
                message_id = :message_id,
                message_name = :message_name,
                aggregate_version = :aggregate_version,
                payload = :payload,
                occurred_on = :occurred_on',
                $this->tableName(),
            ),
        );

        $this->bindAggregateMessageValues($message, $stmt);

        $stmt->executeQuery();
    }

    protected function findByAggregateIdSince(Uuid $aggregateId, DateTimeValueObject $since): array
    {
        $stmt = $this->connection->prepare(
            \sprintf(
                'SELECT message_id, aggregate_id, aggregate_version, occurred_on, message_name, payload FROM %s
                WHERE aggregate_id = :aggregate_id AND occurred_on > :occurred_on
                ORDER BY occurred_on ASC, aggregate_version ASC',
                $this->tableName(),
            ),
        );
        $value = $aggregateId->value();
        $timestamp = $this->mapDatetime($since);
        $stmt->bindValue('aggregate_id', $value, ParameterType::STRING);
        $stmt->bindValue('occurred_on', $timestamp, ParameterType::STRING);
        $result = $stmt->executeQuery();

        $messages = $result->fetchAllAssociative();
        $results = [];

        foreach ($messages as $message) {
            $results[] = $this->deserializer->unserialize($this->convertResultToStream($message));
        }

        return $results;
    }

    protected function findByAggregateIdSinceVersion(Uuid $aggregateId, int $aggregateVersion): array
    {
        $stmt = $this->connection->prepare(
            \sprintf(
                'SELECT message_id, aggregate_id, aggregate_version, occurred_on, message_name, payload FROM %s
                WHERE aggregate_id = :aggregate_id AND aggregate_version > :aggregate_version
                ORDER BY aggregate_version ASC',
                $this->tableName(),
            ),
        );
        $stmt->bindValue('aggregate_id', $aggregateId->value(), ParameterType::STRING);
        $stmt->bindValue('aggregate_version', $aggregateVersion, ParameterType::INTEGER);
        $result = $stmt->executeQuery();

        $messages = $result->fetchAllAssociative();
        $results = [];

        foreach ($messages as $message) {
            $results[] = $this->deserializer->unserialize($this->convertResultToStream($message));
        }

        return $results;
    }

    protected function findByAggregateId(Uuid $aggregateId): array
    {
        $result = $this->queryByAggregateId($aggregateId);

        $events = $result->fetchAllAssociative();
        $results = [];

        foreach ($events as $event) {
            $results[] = $this->deserializer->unserialize(
                $this->convertResultToStream($event),
            );
        }

        return $results;
    }

    protected function findByMessageId(Uuid $messageId): ?AggregateMessage
    {
        $result = $this->queryByMessageId($messageId);

        $message = $result->fetchAssociative();

        return $message
            ? $this->deserializer->unserialize($this->convertResultToStream($message))
            : null;
    }

    protected function findOneByAggregateId(Uuid $aggregateId): ?AggregateMessage
    {
        $result = $this->queryByAggregateId($aggregateId);

        $message = $result->fetchAssociative();

        return $message
            ? $this->deserializer->unserialize($this->convertResultToStream($message))
            : null;
    }

    protected function countByAggregateId(Uuid $aggregateId): int
    {
        $stmt = $this->connection->prepare(
            \sprintf(
                'select COUNT(message_id) as count
                from %s
                where aggregate_id = :aggregateId',
                $this->tableName(),
            ),
        );
        $stmt->bindValue('aggregateId', $aggregateId->value(), ParameterType::STRING);

        $result = $stmt->executeQuery()->fetchAssociative();

        return $result['count'];
    }

    protected function countGivenEventsByAggregateId(Uuid $aggregateId, string ...$eventNames): int
    {
        $stmt = $this->connection
            ->createQueryBuilder()
            ->select('count(message_id) as count')
            ->from($this->tableName())
            ->where('aggregate_id = :aggregateId')
            ->andWhere('message_name IN (:eventNames)');

        $stmt->setParameter('aggregateId', $aggregateId->value(), ParameterType::STRING);
        $stmt->setParameter('eventNames', $eventNames, ArrayParameterType::STRING);

        return $stmt->executeQuery()->fetchOne();
    }

    protected function countFilteredEventsByAggregateId(Uuid $aggregateId, string ...$eventNames): int
    {
        $stmt = $this->connection
            ->createQueryBuilder()
            ->select('count(message_id) as count')
            ->from($this->tableName())
            ->where('aggregate_id = :aggregateId')
            ->andWhere('message_name NOT IN (:eventNames)');

        $stmt->setParameter('aggregateId', $aggregateId->value(), ParameterType::STRING);
        $stmt->setParameter('eventNames', $eventNames, ArrayParameterType::STRING);

        return $stmt->executeQuery()->fetchOne();
    }

    protected function countByAggregateIdSince(Uuid $aggregateId, DateTimeValueObject $since): int
    {
        $stmt = $this->connection->prepare(
            \sprintf(
                'select COUNT(message_id) as count
                from %s
                where aggregate_id = :aggregateId AND occurred_on > :occurred_on',
                $this->tableName(),
            ),
        );
        $stmt->bindValue('aggregateId', $aggregateId->value(), ParameterType::STRING);
        $stmt->bindValue('occurred_on', $this->mapDatetime($since), ParameterType::STRING);

        $result = $stmt->executeQuery()->fetchAssociative();

        return $result['count'];
    }

    protected function countByAggregateIdSinceVersion(Uuid $aggregateId, int $aggregateVersion)
    {
        $stmt = $this->connection->prepare(
            \sprintf(
                'select COUNT(message_id) as count
                from %s
                where aggregate_id = :aggregateId AND aggregate_version > :aggregateVersion',
                $this->tableName(),
            ),
        );
        $stmt->bindValue('aggregateId', $aggregateId->value(), ParameterType::STRING);
        $stmt->bindValue('aggregateVersion', $aggregateVersion, ParameterType::INTEGER);

        $result = $stmt->executeQuery()->fetchAssociative();

        return $result['count'];
    }

    protected function queryByAggregateIdPaginated(Uuid $aggregateId, int $offset, int $limit): array
    {
        $stmt = $this->connection->prepare(
            \sprintf(
                'select message_id, aggregate_id, aggregate_version, occurred_on, message_name, payload
                from %s
                where aggregate_id = :aggregateId
                ORDER BY occurred_on DESC, aggregate_version ASC
                LIMIT :limit
                OFFSET :offset',
                $this->tableName(),
            ),
        );

        $stmt->bindValue('aggregateId', $aggregateId->value(), ParameterType::STRING);
        $stmt->bindValue('limit', $limit, ParameterType::INTEGER);
        $stmt->bindValue('offset', $offset, ParameterType::INTEGER);
        $result = $stmt->executeQuery();

        $events = $result->fetchAllAssociative();

        foreach ($events as $key => $event) {
            $events[$key]['payload'] = \json_decode($event['payload'], true);
        }

        return $events;
    }

    protected function queryGivenEventsByAggregateIdPaginated(
        Uuid $aggregateId,
        int $offset,
        int $limit,
        string ...$eventNames
    ): array {
        $result = $this->connection
            ->createQueryBuilder()
            ->addSelect('a.message_id, a.aggregate_id, a.aggregate_version, a.occurred_on, a.message_name, a.payload')
            ->from($this->tableName(), 'a')
            ->where('a.aggregate_id = :aggregateId')
            ->andWhere('a.message_name IN (:eventNames)')
            ->setParameter('aggregateId', $aggregateId->value(), ParameterType::STRING)
            ->setParameter('eventNames', $eventNames, ArrayParameterType::STRING)
            ->setFirstResult($offset)
            ->setMaxResults($limit)
            ->orderBy('a.occurred_on', 'DESC')
            ->addOrderBy('a.aggregate_version', 'ASC')
            ->executeQuery();

        $events = $result->fetchAllAssociative();

        foreach ($events as $key => $event) {
            $events[$key]['payload'] = \json_decode($event['payload'], true);
        }

        return $events;
    }

    protected function queryEventsFilteredByAggregateIdPaginated(
        Uuid $aggregateId,
        int $offset,
        int $limit,
        string ...$eventNames
    ): array {
        $result = $this->connection
            ->createQueryBuilder()
            ->addSelect('a.message_id, a.aggregate_id, a.aggregate_version, a.occurred_on, a.message_name, a.payload')
            ->from($this->tableName(), 'a')
            ->where('a.aggregate_id = :aggregateId')
            ->andWhere('a.message_name NOT IN (:eventNames)')
            ->setParameter('aggregateId', $aggregateId->value(), ParameterType::STRING)
            ->setParameter('eventNames', $eventNames, ArrayParameterType::STRING)
            ->setFirstResult($offset)
            ->setMaxResults($limit)
            ->orderBy('a.occurred_on', 'DESC')
            ->addOrderBy('a.aggregate_version', 'ASC')
            ->executeQuery();

        $events = $result->fetchAllAssociative();

        foreach ($events as $key => $event) {
            $events[$key]['payload'] = \json_decode($event['payload'], true);
        }

        return $events;
    }

    private function bindAggregateMessageValues(AggregateMessage $message, Statement $stmt): void
    {
        $stmt->bindValue('message_id', $message->messageId()->value(), ParameterType::STRING);
        $stmt->bindValue('aggregate_id', $message->aggregateId()->value(), ParameterType::STRING);
        $stmt->bindValue('aggregate_version', $message->aggregateVersion(), ParameterType::INTEGER);
        $stmt->bindValue('occurred_on', $this->mapDatetime($message->occurredOn()), ParameterType::STRING);
        $stmt->bindValue('message_name', $message::messageName(), ParameterType::STRING);
        $stmt->bindValue(
            'payload',
            \json_encode($message->messagePayload(), \JSON_THROW_ON_ERROR, 512),
            ParameterType::STRING,
        );
    }

    private function convertResultToStream($event): AggregateMessageStream
    {
        return new AggregateMessageStream(
            $event['message_id'],
            $event['aggregate_id'],
            (float)$event['occurred_on'],
            $event['message_name'],
            (int)$event['aggregate_version'],
            $event['payload'],
        );
    }

    private function queryByAggregateId(Uuid $aggregateId): Result
    {
        $stmt = $this->connection->prepare(
            \sprintf(
                'select message_id, aggregate_id, aggregate_version, occurred_on, message_name, payload
                from %s
                where aggregate_id = :aggregateId
                ORDER BY occurred_on ASC, aggregate_version ASC',
                $this->tableName(),
            ),
        );
        $stmt->bindValue('aggregateId', $aggregateId->value(), ParameterType::STRING);

        return $stmt->executeQuery();
    }

    private function queryByMessageId(Uuid $messageId): Result
    {
        $stmt = $this->connection->prepare(
            \sprintf(
                'select message_id, aggregate_id, aggregate_version, occurred_on, message_name, payload
                from %s
                where message_id = :message_id',
                $this->tableName(),
            ),
        );
        $stmt->bindValue('message_id', $messageId->value(), ParameterType::STRING);

        return $stmt->executeQuery();
    }

    private function mapDateTime(\DateTimeInterface $occurredOn): string
    {
        return $occurredOn->format($this->occurredOnFormat);
    }
}
