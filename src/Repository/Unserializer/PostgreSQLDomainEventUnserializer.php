<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Repository\Unserializer;

use Pccomponentes\Ddd\Domain\Model\DomainEvent;
use Pccomponentes\Ddd\Domain\Model\ValueObject\Uuid;
use Pccomponentes\Ddd\Util\Clock;
use Pccomponentes\Ddd\Util\Serializer\DomainEventUnserializable;
use Pccomponentes\DddPostgreSql\Repository\Unserializer\Inflector\NameInflector;

class PostgreSQLDomainEventUnserializer implements DomainEventUnserializable
{
    private $inflector;

    public function __construct(NameInflector $inflector)
    {
        $this->inflector = $inflector;
    }

    public function unserialize($event): DomainEvent
    {
        $className = $this->inflector->getClassName($event['name'], $event['version']);
        if (false === \class_exists($className)) {
            throw new \InvalidArgumentException(\sprintf('Class name %s not found', $className));
        }

        $payload = \json_decode($event['payload'], true);
        return \call_user_func(
            $className . '::fromPayload',
            Uuid::from($event['message_id']),
            Uuid::from($event['aggregate_id']),
            Clock::from($event['occurred_on']),
            $payload
        );
    }
}
