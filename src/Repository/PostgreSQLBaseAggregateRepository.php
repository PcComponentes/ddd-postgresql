<?php
declare(strict_types=1);

namespace Pccomponentes\DddPostgreSql\Repository;

use Pccomponentes\Ddd\Domain\Model\ValueObject\DateTimeValueObject;
use Pccomponentes\Ddd\Domain\Model\ValueObject\Uuid;
use Pccomponentes\Ddd\Util\Message\AggregateMessage;

abstract class PostgreSQLBaseAggregateRepository
{
    private $connection;
    private $unserializer;

    final public function __construct(\PDO $connection, MessageUnserializer $unserializer)
    {
        $this->connection = $connection;
        $this->unserializer = $unserializer;
    }

    protected function findByAggregateId(Uuid $aggregateId): array
    {
        $aggregateIdStr = $aggregateId->value();
        $stm = $this->connection->prepare(
            \sprintf(
                'SELECT message_id, aggregate_id, name, version, payload, occurred_on FROM %s
                WHERE aggregate_id = :aggregate_id ORDER BY occurred_on ASC',
                $this->table()
            )
        );
        $stm->bindParam(':aggregate_id', $aggregateIdStr);
        $stm->execute();

        $mapped = [];
        while ($message = $stm->fetch(\PDO::FETCH_ASSOC)) {
            $message['payload'] = \json_decode($message['payload'], true);
            $mapped[] = $this->unserializer->unserialize($message);
        }

        return $mapped;
    }

    protected function findOneByAggregateId(Uuid $aggregateId): ?AggregateMessage
    {
        $aggregateIdStr = $aggregateId->value();
        $stm = $this->connection->prepare(
            \sprintf(
                'SELECT message_id, aggregate_id, name, version, payload, occurred_on FROM %s
                WHERE aggregate_id = :aggregate_id ORDER BY occurred_on ASC',
                $this->table()
            )
        );
        $stm->bindParam(':aggregate_id', $aggregateIdStr);
        $stm->execute();
        $message = $stm->fetch(\PDO::FETCH_ASSOC);

        if ($message) {
            $message['payload'] = \json_decode($message['payload'], true);
            return $this->unserializer->unserialize($message);
        }

        return null;
    }


    protected function findByAggregateIdSince(Uuid $aggregateId, DateTimeValueObject $since): array
    {
        $aggregateIdStr = $aggregateId->value();
        $sinceStr = $since->format(\DATE_ATOM);
        $stm = $this->connection->prepare(
            \sprintf(
                'SELECT message_id, aggregate_id, name, version, payload, occurred_on FROM %s
                 WHERE aggregate_id = :aggregate_id AND occurred_on > :occurred_on ORDER BY occurred_on ASC',
                $this->table()
            )
        );
        $stm->bindParam(':aggregate_id', $aggregateIdStr);
        $stm->bindParam(':occurred_on', $sinceStr);
        $stm->execute();

        $mapped = [];
        while ($message = $stm->fetch(\PDO::FETCH_ASSOC)) {
            $message['payload'] = \json_decode($message['payload'], true);
            $mapped[] = $this->unserializer->unserialize($message);
        }

        return $mapped;
    }

    protected function insert(AggregateMessage $message): void
    {
        $messageId = $message->messageId()->value();
        $aggregateId = $message->aggregateId()->value();
        $name = $message::messageName();
        $version  = $message::messageVersion();
        $payload = \json_encode($message->messagePayload());
        $occurredOn = $message->occurredOn()->format(\DATE_ATOM);

        $stm = $this->connection->prepare(
            \sprintf(
                'INSERT INTO %s (message_id, aggregate_id, name, version, payload, occurred_on)
                VALUES (:message_id, :aggregate_id, :name, :version, :payload, :occurred_on)',
                $this->table()
            )
        );
        $stm->bindParam(':message_id', $messageId);
        $stm->bindParam(':aggregate_id', $aggregateId);
        $stm->bindParam(':name', $name);
        $stm->bindParam(':payload', $payload);
        $stm->bindParam(':occurred_on', $occurredOn);
        $stm->bindParam(':version', $version);

        $stm->execute();
    }

    protected function forceInsert(AggregateMessage $message): void
    {
        $messageId = $message->messageId()->value();
        $aggregateId = $message->aggregateId()->value();
        $name = $message::messageName();
        $version  = $message::messageVersion();
        $payload = \json_encode($message->messagePayload());
        $occurredOn = $message->occurredOn()->format(\DATE_ATOM);

        $stm = $this->connection->prepare(
            \sprintf(
                'INSERT INTO %s (message_id, aggregate_id, name, version, payload, occurred_on)
                VALUES (:message_id, :aggregate_id, :name, :version, :payload, :occurred_on)
                ON CONFLICT (aggregate_id) DO UPDATE SET
                message_id = :message_id,
                name = :name,
                version= :version,
                payload = :payload,
                occurred_on = :occurred_on;',
                $this->table()
            )
        );
        $stm->bindParam(':message_id', $messageId);
        $stm->bindParam(':aggregate_id', $aggregateId);
        $stm->bindParam(':name', $name);
        $stm->bindParam(':payload', $payload);
        $stm->bindParam(':occurred_on', $occurredOn);
        $stm->bindParam(':version', $version);

        $stm->execute();
    }

    protected function delete(AggregateMessage $message): void
    {
        $aggregateIdStr = $message->aggregateId()->value();
        $stm = $this->connection->prepare(
            \sprintf(
                'DELETE FROM %s WHERE aggregate_id = :aggregate_id',
                $this->table()
            )
        );
        $stm->bindParam(':aggregate_id', $aggregateIdStr);

        $stm->execute();
    }

    abstract protected function table(): string;
}
