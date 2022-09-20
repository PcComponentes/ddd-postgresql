<?php
declare(strict_types=1);

namespace PcComponentes\DddPostgreSQL\Repository;

use Doctrine\DBAL\Connection;
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
        string $occurredOnFormat = 'U'
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

        $this->execute($stmt);
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

        $this->execute($stmt);
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
        $stmt->bindParam(':aggregate_id', $value);
        $stmt->bindParam(':occurred_on', $timestamp);
        $this->execute($stmt);

        $messages = $stmt->fetchAll(\PDO::FETCH_ASSOC);
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
        $stmt->bindValue('aggregate_id', $aggregateId->value(), \PDO::PARAM_STR);
        $stmt->bindValue('aggregate_version', $aggregateVersion, \PDO::PARAM_INT);
        $this->execute($stmt);

        $messages = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $results = [];

        foreach ($messages as $message) {
            $results[] = $this->deserializer->unserialize($this->convertResultToStream($message));
        }

        return $results;
    }

    protected function findByAggregateId(Uuid $aggregateId): array
    {
        $stmt = $this->queryByAggregateId($aggregateId);

        $events = $stmt->fetchAll(\PDO::FETCH_ASSOC);
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
        $stmt = $this->queryByMessageId($messageId);

        $message = $stmt->fetch(\PDO::FETCH_ASSOC);

        return $message
            ? $this->deserializer->unserialize($this->convertResultToStream($message))
            : null;
    }

    protected function findOneByAggregateId(Uuid $aggregateId): ?AggregateMessage
    {
        $stmt = $this->queryByAggregateId($aggregateId);

        $message = $stmt->fetch(\PDO::FETCH_ASSOC);

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
        $stmt->bindValue('aggregateId', $aggregateId->value(), \PDO::PARAM_STR);
        $this->execute($stmt);

        $result = $stmt->fetch();

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

        $stmt->setParameter('aggregateId', $aggregateId->value(), \PDO::PARAM_STR);
        $stmt->setParameter('eventNames', $eventNames, Connection::PARAM_STR_ARRAY);

        return $stmt->execute()->fetchOne();
    }

    protected function countFilteredEventsByAggregateId(Uuid $aggregateId, string ...$eventNames): int
    {
        $stmt = $this->connection
            ->createQueryBuilder()
            ->select('count(message_id) as count')
            ->from($this->tableName())
            ->where('aggregate_id = :aggregateId')
            ->andWhere('message_name NOT IN (:eventNames)');

        $stmt->setParameter('aggregateId', $aggregateId->value(), \PDO::PARAM_STR);
        $stmt->setParameter('eventNames', $eventNames, Connection::PARAM_STR_ARRAY);

        return $stmt->execute()->fetchOne();
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
        $stmt->bindValue('aggregateId', $aggregateId->value(), \PDO::PARAM_STR);
        $stmt->bindValue('occurred_on', $this->mapDatetime($since), \PDO::PARAM_STR);
        $this->execute($stmt);

        $result = $stmt->fetch();

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
        $stmt->bindValue('aggregateId', $aggregateId->value(), \PDO::PARAM_STR);
        $stmt->bindValue('aggregateVersion', $aggregateVersion, \PDO::PARAM_INT);
        $this->execute($stmt);

        $result = $stmt->fetch();

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

        $stmt->bindValue('aggregateId', $aggregateId->value(), \PDO::PARAM_STR);
        $stmt->bindValue('limit', $limit, \PDO::PARAM_INT);
        $stmt->bindValue('offset', $offset, \PDO::PARAM_INT);
        $this->execute($stmt);

        $events = $stmt->fetchAll();

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
        $stmt = $this->connection
            ->createQueryBuilder()
            ->addSelect('a.message_id, a.aggregate_id, a.aggregate_version, a.occurred_on, a.message_name, a.payload')
            ->from($this->tableName(), 'a')
            ->where('a.aggregate_id = :aggregateId')
            ->andWhere('a.message_name IN (:eventNames)')
            ->setParameter('aggregateId', $aggregateId->value(), \PDO::PARAM_STR)
            ->setParameter('eventNames', $eventNames, Connection::PARAM_STR_ARRAY)
            ->setFirstResult($offset)
            ->setMaxResults($limit)
            ->orderBy('a.occurred_on', 'DESC')
            ->addOrderBy('a.aggregate_version', 'ASC')
            ->execute();

        $events = $stmt->fetchAll();

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
        $stmt = $this->connection
            ->createQueryBuilder()
            ->addSelect('a.message_id, a.aggregate_id, a.aggregate_version, a.occurred_on, a.message_name, a.payload')
            ->from($this->tableName(), 'a')
            ->where('a.aggregate_id = :aggregateId')
            ->andWhere('a.message_name NOT IN (:eventNames)')
            ->setParameter('aggregateId', $aggregateId->value(), \PDO::PARAM_STR)
            ->setParameter('eventNames', $eventNames, Connection::PARAM_STR_ARRAY)
            ->setFirstResult($offset)
            ->setMaxResults($limit)
            ->orderBy('a.occurred_on', 'DESC')
            ->addOrderBy('a.aggregate_version', 'ASC')
            ->execute();

        $events = $stmt->fetchAll();

        foreach ($events as $key => $event) {
            $events[$key]['payload'] = \json_decode($event['payload'], true);
        }

        return $events;
    }

    protected function execute(Statement $stmt): void
    {
        $result = $stmt->execute();

        if (false !== $result) {
            return;
        }

        $errorInfo = \json_encode($stmt->errorInfo(), \JSON_ERROR_NONE);
        $errorCode = (string) $stmt->errorCode();

        if (false === \is_string($errorInfo)) {
            $errorInfo = '';
        }

        throw new \RuntimeException(\sprintf('%s | %s', $errorInfo, $errorCode));
    }

    private function bindAggregateMessageValues(AggregateMessage $message, Statement $stmt): void
    {
        $stmt->bindValue('message_id', $message->messageId()->value(), \PDO::PARAM_STR);
        $stmt->bindValue('aggregate_id', $message->aggregateId()->value(), \PDO::PARAM_STR);
        $stmt->bindValue('aggregate_version', $message->aggregateVersion(), \PDO::PARAM_INT);
        $stmt->bindValue('occurred_on', $this->mapDatetime($message->occurredOn()), \PDO::PARAM_STR);
        $stmt->bindValue('message_name', $message::messageName(), \PDO::PARAM_STR);
        $stmt->bindValue(
            'payload',
            \json_encode($message->messagePayload(), \JSON_THROW_ON_ERROR, 512),
            \PDO::PARAM_STR,
        );
    }

    private function convertResultToStream($event): AggregateMessageStream
    {
        return new AggregateMessageStream(
            $event['message_id'],
            $event['aggregate_id'],
            (float) $event['occurred_on'],
            $event['message_name'],
            (int) $event['aggregate_version'],
            $event['payload'],
        );
    }

    private function queryByAggregateId(Uuid $aggregateId): Statement
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
        $stmt->bindValue('aggregateId', $aggregateId->value(), \PDO::PARAM_STR);
        $this->execute($stmt);

        return $stmt;
    }

    private function queryByMessageId(Uuid $messageId): Statement
    {
        $stmt = $this->connection->prepare(
            \sprintf(
                'select message_id, aggregate_id, aggregate_version, occurred_on, message_name, payload
                from %s
                where message_id = :message_id',
                $this->tableName(),
            ),
        );
        $stmt->bindValue('message_id', $messageId->value(), \PDO::PARAM_STR);
        $this->execute($stmt);

        return $stmt;
    }

    private function mapDateTime(\DateTimeInterface $occurredOn): string
    {
        return $occurredOn->format($this->occurredOnFormat);
    }
}
