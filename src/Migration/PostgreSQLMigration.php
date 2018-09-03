<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Migration;

use Pccomponentes\Ddd\Infrastructure\Migration\Migration;
use Pccomponentes\DddPostgreSql\Builder\PDOBuilder;

abstract class PostgreSQLMigration implements Migration
{
    private $builder;

    public function __construct(PDOBuilder $builder)
    {
        $this->builder = $builder;
    }

    public function builder(): PDOBuilder
    {
        return $this->builder;
    }
}
