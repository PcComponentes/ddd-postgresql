<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Migration;

use Pccomponentes\Ddd\Infrastructure\Migration\MigrationLoader;

class PostgreSQLLoader extends MigrationLoader
{
    public function __construct(string $dir)
    {
        parent::__construct(
            $dir,
            PostgreSQLMigration::class
        );
    }
}
