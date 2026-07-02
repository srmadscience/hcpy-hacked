-- CrateDB DDL for the records produced by hc2kafka.py.
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
-- NOTE: the table is intentionally NOT partitioned. The Confluent JDBC sink
-- checks table existence via JDBC metadata before inserting, and an empty
-- PARTITIONED CrateDB table has no materialized partitions, so it is invisible
-- to that check and the sink fails with "table is missing". A plain table works.
--
-- event_time is generated from the epoch-millis long: it is the per-reading key
-- and a real timestamp for time-series queries. For monthly buckets in a query
-- use date_trunc('month', event_time).

CREATE TABLE IF NOT EXISTS homeconnect.dishwasher (
    "device"       TEXT,
    "category"     TEXT,
    "property"     TEXT,
    "property_raw" TEXT,
    "name"         TEXT,
    "value"        TEXT,
    "value_double" DOUBLE PRECISION,
    "value_bool"   BOOLEAN,
    "event_type"   TEXT,
    "ts_epoch_ms"  BIGINT,
    "ts_iso"       TEXT,
    "event_time"   TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS (CAST("ts_epoch_ms" AS TIMESTAMP WITH TIME ZONE)),
    PRIMARY KEY ("device", "property_raw", "event_time")
);

CREATE TABLE IF NOT EXISTS homeconnect.bridge (
    "device"       TEXT,
    "category"     TEXT,
    "property"     TEXT,
    "property_raw" TEXT,
    "name"         TEXT,
    "value"        TEXT,
    "value_double" DOUBLE PRECISION,
    "value_bool"   BOOLEAN,
    "event_type"   TEXT,
    "ts_epoch_ms"  BIGINT,
    "ts_iso"       TEXT,
    "event_time"   TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS (CAST("ts_epoch_ms" AS TIMESTAMP WITH TIME ZONE)),
    PRIMARY KEY ("device", "property_raw", "event_time")
);

CREATE OR REPLACE VIEW homeconnect.activity_per_day AS
SELECT  DATE_BIN('1 days'::INTERVAL, d."event_time", 0) wash_day ,value, count(*) mins_active
  , CASE value
  WHEN 'Intensiv70' ThEN 1.4 / 140
  WHEN 'Auto2' ThEN 1.2 / 135
  WHEN 'Eco50' ThEN 1.1 / 270
  WHEN 'Quick65' ThEN 1.2 / 62
  WHEN 'PreRinse' ThEN 0.05 / 15
  WHEN 'Quick45' ThEN 0.85 / 44
  WHEN 'QuickD' ThEN 0.9 / 55
  WHEN 'IntensivPower' ThEN 1.45 / 145
  WHEN 'MachineCare' ThEN 1.45 / 145
  ELSE 0 END * count(*) kwh
FROM "homeconnect"."dishwasher"  d
WHERE property = 'BSH.Common.Root.ActiveProgram'
AND value != 'None'
GROUP BY DATE_BIN('1 days'::INTERVAL, d."event_time", 0),value;

CREATE OR REPLACE VIEW homeconnect.kwh_per_day AS
SELECT wash_day, sum(kwh) kwh
FROM "homeconnect"."activity_per_day"
GROUP BY wash_day;
