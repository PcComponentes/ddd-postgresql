<?php
declare(strict_types=1);
namespace Pccomponentes\DddPostgreSql\Repository;

class PostgreSQLSchemaCreation
{
    private const EVENT_STORE = 'event_store';
    private const SNAPSHOT_STORE = 'snapshot_store';

    public static function createEventStore(): string
    {
        return \implode(
            ';',
            [
                self::createTable(self::EVENT_STORE),
                self::createIndex(self::EVENT_STORE, 'message_id'),
                self::createIndex(self::EVENT_STORE, 'name'),
                self::createUnique(self::EVENT_STORE, 'message_id')
            ]
        );
    }

    public static function dropEventStore(): string
    {
        return self::dropTable(self::EVENT_STORE);
    }

    public static function createSnapshotStore(): string
    {
        return \implode(
            ';',
            [
                self::createTable(self::SNAPSHOT_STORE),
                self::createIndex(self::SNAPSHOT_STORE, 'message_id'),
                self::createIndex(self::SNAPSHOT_STORE, 'name'),
                self::createIndex(self::SNAPSHOT_STORE, 'aggregate_id'),
                self::createUnique(self::SNAPSHOT_STORE, 'message_id'),
                self::createUnique(self::SNAPSHOT_STORE, 'aggregate_id')
            ]
        );
    }

    public static function dropSnapshotStore(): string
    {
        return self::dropTable(self::SNAPSHOT_STORE);
    }

    public static function createProjection(string $tableName): string
    {
        return \implode(
            ';',
            [
                self::createTable($tableName),
                self::createIndex($tableName, 'message_id'),
                self::createIndex($tableName, 'name'),
                self::createIndex($tableName, 'aggregate_id'),
                self::createUnique($tableName, 'message_id'),
                self::createUnique($tableName, 'aggregate_id')
            ]
        );
    }

    public static function dropProjection(string $tableName): string
    {
        return self::dropTable($tableName);
    }

    private static function createTable(string $table): string
    {
        $sql = 'CREATE TABLE %table% (
                    _id bigserial NOT NULL,
                    message_id uuid NOT NULL,
                    aggregate_id uuid NOT NULL,
                    name character varying(128) NOT NULL,
                    payload jsonb NOT NULL,
                    occurred_on timestamp NOT NULL,
                    version character varying(16) NOT NULL,
                    CONSTRAINT %table%_pkey PRIMARY KEY (_id)
                )';

        return str_replace('%table%', $table, $sql);
    }

    private static function createIndex(string $table, string $field): string
    {
        $sql = 'CREATE INDEX %table%_index_%field% ON %table% USING btree (%field%)';

        return str_replace(['%table%', '%field%'], [$table, $field], $sql);
    }

    private static function createUnique(string $table, string $field): string
    {
        $sql = 'ALTER TABLE %table% ADD CONSTRAINT %table%_unique_%field% UNIQUE (%field%)';

        return str_replace(['%table%', '%field%'], [$table, $field], $sql);
    }

    private static function dropTable(string $table): string
    {
        return str_replace('%table%', $table, 'DROP TABLE %table%;');
    }
}
