<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Migration;

use Pccomponentes\Ddd\Infrastructure\Migration\MigrationExecutor;
use Pccomponentes\DddPostgreSql\Builder\PDOBuilder;

class PostgreSQLMigrationExecutor extends MigrationExecutor
{
    private $builder;

    public function __construct(PostgreSQLLoader $loader, PDOBuilder $builder)
    {
        parent::__construct($loader);
        $this->builder = $builder;
    }

    protected function getInstanceArgs(): array
    {
        return [$this->builder];
    }
}
