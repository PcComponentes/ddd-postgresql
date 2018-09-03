<?php
declare(strict_types=1);

namespace Pccomponentes\DddPostgreSql\Repository;

use Pccomponentes\Ddd\Domain\Model\ValueObject\DateTimeValueObject;
use Pccomponentes\Ddd\Domain\Model\ValueObject\Uuid;
use Pccomponentes\Ddd\Util\Message\AggregateMessage;

class PostgreSQLBaseAggregateRepository
{
    private $connection;

    public function __construct(\PDO $connection)
    {
        $this->connection = $connection;
    }

    protected function connection(): \PDO
    {
        return $this->connection;
    }

    protected function getStatement(Uuid $aggregateId, string $tableName): \PDOStatement
    {
        $aggregateIdStr = $aggregateId->value();
        $stm = $this->connection->prepare(
            \sprintf(
                'SELECT message_id, aggregate_id, name, version, payload, occurred_on FROM %s
                WHERE aggregate_id = :aggregate_id ORDER BY occurred_on ASC',
                $tableName
            )
        );
        $stm->bindParam(':aggregate_id', $aggregateIdStr);

        return $stm;
    }


    protected function getSinceStatement(
        Uuid $aggregateId,
        DateTimeValueObject $since,
        string $tableName
    ): \PDOStatement {
        $aggregateIdStr = $aggregateId->value();
        $sinceStr = $since->format(\DATE_ATOM);
        $stm = $this->connection->prepare(
            \sprintf(
                'SELECT message_id, aggregate_id, name, version, payload, occurred_on FROM %s
                 WHERE aggregate_id = :aggregate_id AND occurred_on > :occurred_on ORDER BY occurred_on ASC',
                $tableName
            )
        );
        $stm->bindParam(':aggregate_id', $aggregateIdStr);
        $stm->bindParam(':occurred_on', $sinceStr);

        return $stm;
    }

    protected function insertStatement(AggregateMessage $message, string $tableName): \PDOStatement
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
              $tableName
            )
        );
        $stm->bindParam(':message_id', $messageId);
        $stm->bindParam(':aggregate_id', $aggregateId);
        $stm->bindParam(':name', $name);
        $stm->bindParam(':payload', $payload);
        $stm->bindParam(':occurred_on', $occurredOn);
        $stm->bindParam(':version', $version);

        return $stm;
    }

    protected function forceInsertStatement(AggregateMessage $message, string $tableName): \PDOStatement
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
                $tableName
            )
        );
        $stm->bindParam(':message_id', $messageId);
        $stm->bindParam(':aggregate_id', $aggregateId);
        $stm->bindParam(':name', $name);
        $stm->bindParam(':payload', $payload);
        $stm->bindParam(':occurred_on', $occurredOn);
        $stm->bindParam(':version', $version);

        return $stm;
    }

    protected function removeStatement(Uuid $aggregateId, string $tableName): \PDOStatement
    {
        $aggregateIdStr = $aggregateId->value();
        $stm = $this->connection->prepare(
            \sprintf(
                'DELETE FROM %s WHERE aggregate_id = :aggregate_id',
                $tableName
            )
        );
        $stm->bindParam(':aggregate_id', $aggregateIdStr);

        return $stm;
    }
}
