<?php declare(strict_types=1);

namespace PcComponentes\DddPostgreSQL\Migration;

final class PostgresMessageStoreDefinition
{
    public const EVENT_STORE = 'event_store';
    public const SNAPSHOT_STORE = 'snapshot_store';

    public static function eventStoreUpSchema(): string
    {
        return sprintf('
            CREATE TABLE %s (
                message_id UUID NOT NULL,
                aggregate_id UUID NOT NULL,
                aggregate_version INT NOT NULL,
                occurred_on BIGSERIAL NOT NULL,
                message_name character varying(128) NOT NULL,
                payload jsonb NOT NULL,
                PRIMARY KEY(message_id)
            );
            CREATE INDEX "event_store_message_name" ON "event_store" ("message_name");
            CREATE INDEX "event_store_aggregate_id" ON "event_store" ("aggregate_id");
        ',
            self::EVENT_STORE
        );
    }

    public static function snapshotStoreUpSchema(): string
    {
        return \sprintf('
            CREATE TABLE %s (
                message_id UUID NOT NULL,
                aggregate_id UUID NOT NULL,
                aggregate_version INT NOT NULL,
                occurred_on BIGSERIAL NOT NULL,
                message_name character varying(128) NOT NULL,
                payload jsonb NOT NULL,
                PRIMARY KEY(message_id)
            );
            CREATE INDEX "snapshot_store_message_name" ON "snapshot_store" ("message_name");
            CREATE INDEX "snapshot_store_aggregate_id" ON "snapshot_store" ("aggregate_id");
            ALTER TABLE snapshot_store ADD CONSTRAINT "snapshot_store_unique_aggregate_id" UNIQUE ("aggregate_id");
        ',
        self::EVENT_STORE
        );
    }

    public static function eventStoreDownSchema(): string
    {
        return \sprintf('DROP TABLE %s', self::EVENT_STORE);
    }

    public static function snapshotStoreDownSchema(): string
    {
        return \sprintf('DROP TABLE %s', self::SNAPSHOT_STORE);
    }
}
