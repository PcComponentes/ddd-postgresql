<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Repository\Unserializer\Inflector;

class PostgreSqlSimpleNameInflector implements NameInflector
{

    public function getClassName(string $name, string $version): string
    {
        return $name;
    }
}
