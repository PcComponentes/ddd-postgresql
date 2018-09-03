<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Builder;

class PDOBuilder
{
    private $host;
    private $port;
    private $user;
    private $password;
    private $dbname;
    private $schema;

    public function __construct(string $host, int $port, string $user, string $password)
    {
        $this->host = $host;
        $this->port = $port;
        $this->user = $user;
        $this->password = $password;
    }

    public function setDbName(?string $dbName): self
    {
        $this->dbname = $dbName;
        return $this;
    }

    public function setSchema(?string $schema): self
    {
        $this->schema = $schema;
        return $this;
    }

    public function build(?string $dbName = null, ?string $schema = null): \PDO
    {
        $parts = ["host={$this->host}", "port={$this->port}", "user={$this->user}", "password={$this->password}"];

        if ($dbName) {
            $parts[] = "dbname={$dbName}";
        }

        $connection = new \PDO('pgsql:' . implode(';', $parts));
        $connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        if ($schema) {
            $connection->exec("SET search_path TO {$schema}");
        }

        return $connection;
    }
}