<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Repository;

use Pccomponentes\Ddd\Util\Message\AggregateMessage;

interface MessageUnserializer
{
    public function unserialize(array $message): AggregateMessage;
}
