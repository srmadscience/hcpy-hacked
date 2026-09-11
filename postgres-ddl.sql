-- PostgreSQL DDL for the records produced by hc2kafka.py.
--
-- This is the PostgreSQL port of crate-ddl.sql; keep the two in sync.
--
-- One table per Kafka topic (one per appliance, plus the bridge LWT topic).
-- The matching crate-sink-*.json JDBC sink connectors run with
-- "auto.create": "false", so these tables must exist before the connectors
-- start. Columns mirror the Avro schema registered by hc2kafka.py under the
-- "<topic>-value" subject.
--
-- History mode: the sinks use "insert.mode": "insert", so every reading is
-- appended and kept forever -- nothing is overwritten.
--
-- Primary key (device, property_raw, event_time): one row per property reading
-- per timestamp. A burst of different properties shares a timestamp but differs
-- by property_raw, so they coexist; only the exact same property reported twice
-- in the same millisecond would collide (and be rejected by the insert).
--
-- The tables are intentionally NOT partitioned, matching the CrateDB version:
-- the Confluent JDBC sink checks table existence via JDBC metadata before
-- inserting, and a bare PostgreSQL partitioned parent adds nothing here.
--
-- Differences from crate-ddl.sql:
--   * event_time is a STORED generated column (PostgreSQL has no virtual
--     generated columns) built with to_timestamp(), since casting a bigint
--     straight to timestamptz is not allowed.
--   * DATE_BIN() is PostgreSQL 14+; the day bucket uses date_trunc('day', ...)
--     instead, which is equivalent for a 1-day bin on the epoch origin.

CREATE SCHEMA IF NOT EXISTS homeconnect;

CREATE TABLE IF NOT EXISTS homeconnect.dishwasher (
    "device"       TEXT   NOT NULL,
    "category"     TEXT,
    "property"     TEXT,
    "property_raw" TEXT   NOT NULL,
    "name"         TEXT,
    "value"        TEXT,
    "value_double" DOUBLE PRECISION,
    "value_bool"   BOOLEAN,
    "event_type"   TEXT,
    "ts_epoch_ms"  BIGINT NOT NULL,
    "ts_iso"       TEXT,
    "event_time"   TIMESTAMP WITH TIME ZONE
                   GENERATED ALWAYS AS (to_timestamp("ts_epoch_ms" / 1000.0)) STORED,
    PRIMARY KEY ("device", "property_raw", "event_time")
);

CREATE TABLE IF NOT EXISTS homeconnect.bridge (
    "device"       TEXT   NOT NULL,
    "category"     TEXT,
    "property"     TEXT,
    "property_raw" TEXT   NOT NULL,
    "name"         TEXT,
    "value"        TEXT,
    "value_double" DOUBLE PRECISION,
    "value_bool"   BOOLEAN,
    "event_type"   TEXT,
    "ts_epoch_ms"  BIGINT NOT NULL,
    "ts_iso"       TEXT,
    "event_time"   TIMESTAMP WITH TIME ZONE
                   GENERATED ALWAYS AS (to_timestamp("ts_epoch_ms" / 1000.0)) STORED,
    PRIMARY KEY ("device", "property_raw", "event_time")
);

CREATE OR REPLACE VIEW homeconnect.activity_per_day AS
SELECT  date_trunc('day', d."event_time") AS wash_day, value, count(*) AS mins_active
  , CASE value
  WHEN 'Intensiv70' THEN 1.4 / 140
  WHEN 'Auto2' THEN 1.2 / 135
  WHEN 'Eco50' THEN 1.1 / 270
  WHEN 'Quick65' THEN 1.2 / 62
  WHEN 'PreRinse' THEN 0.05 / 15
  WHEN 'Quick45' THEN 0.85 / 44
  WHEN 'QuickD' THEN 0.9 / 55
  WHEN 'IntensivPower' THEN 1.45 / 145
  WHEN 'MachineCare' THEN 1.45 / 145
  ELSE 0 END * count(*) AS kwh
FROM "homeconnect"."dishwasher"  d
WHERE property = 'BSH.Common.Root.ActiveProgram'
AND value != 'None'
GROUP BY date_trunc('day', d."event_time"), value;

CREATE OR REPLACE VIEW homeconnect.kwh_per_day AS
SELECT wash_day, sum(kwh) AS kwh
FROM "homeconnect"."activity_per_day"
GROUP BY wash_day;
