<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Repository\Unserializer;

use Pccomponentes\Ddd\Domain\Model\ValueObject\Uuid;
use Pccomponentes\Ddd\Projection\Model\Projection;
use Pccomponentes\Ddd\Util\Clock;
use Pccomponentes\Ddd\Util\Serializer\ProjectionUnserializable;
use Pccomponentes\DddPostgreSql\Repository\Unserializer\Inflector\NameInflector;

class PostgreSQLProjectionUnserializer implements ProjectionUnserializable
{
    private $inflector;

    public function __construct(NameInflector $inflector)
    {
        $this->inflector = $inflector;
    }

    public function unserialize($projection): Projection
    {
        $className = $this->inflector->getClassName($projection['name'], $projection['version']);
        if (false === \class_exists($className)) {
            throw new \InvalidArgumentException(\sprintf('Class name %s not found', $className));
        }

        $payload = \json_decode($projection['payload'], true);
        return \call_user_func(
            $className . '::fromPayload',
            Uuid::from($projection['message_id']),
            Uuid::from($projection['aggregate_id']),
            Clock::from($projection['occurred_on']),
            $payload
        );
    }
}
