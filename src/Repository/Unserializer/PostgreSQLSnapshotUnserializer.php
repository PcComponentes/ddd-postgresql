<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Repository\Unserializer;

use Pccomponentes\Ddd\Domain\Model\Snapshot;
use Pccomponentes\Ddd\Domain\Model\ValueObject\Uuid;
use Pccomponentes\Ddd\Util\Clock;
use Pccomponentes\Ddd\Util\Serializer\SnapshotUnserializable;
use Pccomponentes\DddPostgreSql\Repository\Unserializer\Inflector\NameInflector;

class PostgreSQLSnapshotUnserializer implements SnapshotUnserializable
{
    private $inflector;

    public function __construct(NameInflector $inflector)
    {
        $this->inflector = $inflector;
    }

    public function unserialize($snapshot): Snapshot
    {
        $className = $this->inflector->getClassName($snapshot['name'], $snapshot['version']);
        if (false === \class_exists($className)) {
            throw new \InvalidArgumentException(\sprintf('Class name %s not found', $className));
        }

        $payload = \json_decode($snapshot['payload'], true);
        return \call_user_func(
            $className . '::fromPayload',
            Uuid::from($snapshot['message_id']),
            Uuid::from($snapshot['aggregate_id']),
            Clock::from($snapshot['occurred_on']),
            $payload
        );
    }
}
