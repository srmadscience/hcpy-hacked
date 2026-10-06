#!/usr/bin/env python3
# Watch the MQTT feed published by hc2mqtt.py, turn the terse topic/payload
# pairs back into comprehensible records, and produce them to Kafka as Avro
# (Confluent wire format) so a Kafka Connect sink can consume them directly.
#
# Inbound MQTT topics (see hc2mqtt.py) look like:
#   homeconnect/<device>/state/<property>   payload: plain string
#   homeconnect/<device>/event/<property>   payload: JSON {"event_type": ...}
#   homeconnect/<device>/LWT                payload: online|offline
#   homeconnect/LWT                         payload: online|offline (bridge)
#
# The <property> segment is the feature name lower-cased with '.' -> '_', e.g.
# "bsh_common_status_doorstate". We reload config/devices.json to map that back
# to the original "BSH.Common.Status.DoorState".
import json
import os
import socket
import ssl
import sys
import threading
import time
from datetime import datetime, timezone

import click
import click_config_file
import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from HCSocket import now
from outbox import Outbox, drain

# Avro schema for every record. Per-device topics each register this under their
# own "<topic>-value" subject; the schema itself is shared.
VALUE_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "HomeConnectEvent",
        "namespace": "hcpy",
        "fields": [
            {"name": "device", "type": "string"},
            {"name": "category", "type": "string"},  # state | event | connectivity
            {"name": "property", "type": "string"},  # BSH.Common.Status.DoorState
            {"name": "property_raw", "type": "string"},  # bsh_common_status_doorstate
            {"name": "name", "type": "string"},  # DoorState
            {"name": "value", "type": ["null", "string"], "default": None},
            {"name": "value_double", "type": ["null", "double"], "default": None},
            {"name": "value_bool", "type": ["null", "boolean"], "default": None},
            {"name": "event_type", "type": ["null", "string"], "default": None},
            {"name": "ts_epoch_ms", "type": "long"},
            {"name": "ts_iso", "type": "string"},
        ],
    }
)

_TRUE = {"true", "on", "1", "yes"}
_FALSE = {"false", "off", "0", "no"}


def kprint(*args):
    print(now(), *args, flush=True)


class KafkaSink:
    """Adapter over the confluent-kafka Producer that ``outbox.drain`` drives.

    It exposes the small interface ``drain`` expects
    (``produce_one`` / ``flush`` / ``reset_delivery_errors`` /
    ``delivery_errors``) plus ``kafka_up`` for the health probe. Avro
    serialization happens here, at produce time, so buffered records stay as
    plain dicts on disk until Kafka (and the Schema Registry) are back --
    serializing needs the registry, which is typically down whenever Kafka is.
    """

    def __init__(self, producer, avro_serializer, key_serializer, debug=False):
        self.producer = producer
        self.avro_serializer = avro_serializer
        self.key_serializer = key_serializer
        self.debug = debug
        self.delivery_errors = 0
        self.last_error = None

    def reset_delivery_errors(self):
        self.delivery_errors = 0

    def _on_delivery(self, err, msg):
        if err is not None:
            self.delivery_errors += 1
            self.last_error = err
            kprint(f"ERROR kafka delivery failed: {err}")
        elif self.debug:
            kprint(f"delivered to {msg.topic()} [{msg.partition()}]@{msg.offset()}")

    def produce_one(self, topic, key, value):
        """Serialize and queue one record. Synchronous failures bump
        ``delivery_errors`` rather than raising, so callers can buffer instead."""
        try:
            self.producer.produce(
                topic=topic,
                key=self.key_serializer(key, SerializationContext(topic, MessageField.KEY)),
                value=self.avro_serializer(
                    value, SerializationContext(topic, MessageField.VALUE)
                ),
                on_delivery=self._on_delivery,
            )
            self.producer.poll(0)
        except Exception as e:
            # A full local queue (BufferError) or a Schema Registry lookup
            # failure both surface synchronously here; treat either as
            # "Kafka unavailable" so the record gets buffered to the outbox.
            self.delivery_errors += 1
            self.last_error = e

    def flush(self, timeout=30.0):
        return self.producer.flush(timeout)

    def kafka_up(self, timeout=5.0):
        try:
            self.producer.list_topics(timeout=timeout)
            return True
        except Exception:
            return False


def build_name_lookup(devices_file):
    """Map each device's lower_underscored feature name back to the original.

    Returns {device_name: {"bsh_common_status_doorstate": "BSH.Common.Status.DoorState"}}
    """
    lookup = {}
    try:
        with open(devices_file, "r") as f:
            devices = json.load(f)
    except Exception as e:
        kprint(f"WARN could not load {devices_file} for name mapping: {e}")
        return lookup

    for device in devices:
        per_device = {}
        for feature in device.get("features", {}).values():
            name = feature.get("name")
            if not name:
                continue
            per_device[name.lower().replace(".", "_")] = name
        lookup[device.get("name")] = per_device
    return lookup


def coerce_value(value):
    """Best-effort typing of an MQTT string payload.

    Returns (value_str, value_double, value_bool). hc2mqtt publishes every state
    via str(value), so the canonical form is always a string; we additionally
    expose a typed view when the string is unambiguously numeric or boolean.
    """
    if value is None:
        return None, None, None

    value_str = str(value)
    lowered = value_str.strip().lower()

    value_bool = None
    if lowered in _TRUE:
        value_bool = True
    elif lowered in _FALSE:
        value_bool = False

    value_double = None
    try:
        # Avoid turning "01"/"007" style program codes into floats only when they
        # are clearly leading-zero codes; plain numbers still convert.
        if not (len(lowered) > 1 and lowered.startswith("0") and "." not in lowered):
            value_double = float(value_str)
    except (ValueError, TypeError):
        value_double = None

    return value_str, value_double, value_bool


def parse_topic(topic, mqtt_prefix):
    """Split an MQTT topic into (device, category, raw_property).

    Returns None for topics we don't care about.
    """
    remainder = topic
    if mqtt_prefix and remainder.startswith(mqtt_prefix):
        remainder = remainder[len(mqtt_prefix):]
    parts = [p for p in remainder.split("/") if p != ""]

    if parts == ["LWT"]:
        return ("_bridge", "connectivity", "LWT")
    if len(parts) == 2 and parts[1] == "LWT":
        return (parts[0], "connectivity", "LWT")
    if len(parts) == 3 and parts[1] in ("state", "event"):
        return (parts[0], parts[1], parts[2])
    return None


@click.command()
@click.option("-d", "--devices_file", default="config/devices.json")
@click.option("-h", "--mqtt_host", default="localhost")
@click.option("-p", "--mqtt_prefix", default="homeconnect/")
@click.option("--mqtt_port", default=1883, type=int)
@click.option("--mqtt_username")
@click.option("--mqtt_password")
@click.option("--mqtt_ssl", is_flag=True)
@click.option("--mqtt_cafile")
@click.option("--mqtt_certfile")
@click.option("--mqtt_keyfile")
@click.option("--kafka_clientname", default="hc2kafka")
@click.option("--kafka_bootstrap", default="badger:9092")
@click.option("--schema_registry_url", default="http://badger:8081")
@click.option("--kafka_topic_prefix", default="homeconnect.")
@click.option("--buffer_dir", default="config/outbox",
              help="Directory for store-and-forward buffering when Kafka is down.")
@click.option("--drain_interval", default=15.0, type=float,
              help="Seconds between attempts to replay the outbox backlog.")
@click.option("--kafka_probe_timeout", default=5.0, type=float,
              help="Timeout (s) for the metadata probe that checks Kafka is up.")
@click.option("--debug/--no-debug", default=False)
@click_config_file.configuration_option()
def hc2kafka(
    devices_file: str,
    mqtt_host: str,
    mqtt_prefix: str,
    mqtt_port: int,
    mqtt_username: str,
    mqtt_password: str,
    mqtt_ssl: bool,
    mqtt_cafile: str,
    mqtt_certfile: str,
    mqtt_keyfile: str,
    kafka_clientname: str,
    kafka_bootstrap: str,
    schema_registry_url: str,
    kafka_topic_prefix: str,
    buffer_dir: str,
    drain_interval: float,
    kafka_probe_timeout: float,
    debug: bool,
):
    name_lookup = build_name_lookup(devices_file)
    kprint(f"loaded name mappings for {len(name_lookup)} device(s)")

    # --- Kafka / Avro setup -------------------------------------------------
    schema_registry = SchemaRegistryClient({"url": schema_registry_url})
    avro_serializer = AvroSerializer(schema_registry, VALUE_SCHEMA)
    key_serializer = StringSerializer("utf_8")
    producer = Producer({"bootstrap.servers": kafka_bootstrap})
    sink = KafkaSink(producer, avro_serializer, key_serializer, debug=debug)

    # --- Store-and-forward outbox -------------------------------------------
    # If Kafka (or the Schema Registry) is unreachable, records are buffered to
    # disk and replayed later instead of being dropped. A background thread
    # drains the backlog whenever Kafka is back; while any backlog exists new
    # records are buffered too, so replay preserves per-device order.
    outbox = Outbox(buffer_dir)

    def topic_for(device):
        return f"{kafka_topic_prefix}{device}"

    def try_drain():
        if not outbox.pending_files():
            return
        if not sink.kafka_up(kafka_probe_timeout):
            return
        sent = drain(sink, outbox)
        if sent:
            kprint(
                f"drained {len(sent)} file(s), {sum(sent.values())} record(s) "
                f"from outbox {buffer_dir}"
            )
        remaining = len(outbox.pending_files())
        if remaining:
            kprint(f"WARN {remaining} outbox file(s) still pending")

    def drain_loop():
        while True:
            time.sleep(drain_interval)
            try:
                try_drain()
            except Exception as e:
                kprint(f"WARN outbox drain error: {e}")

    def readable_property(device, raw_property):
        if raw_property == "LWT":
            return "connectivity"
        return name_lookup.get(device, {}).get(raw_property, raw_property)

    # --- MQTT callbacks -----------------------------------------------------
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            sub = f"{mqtt_prefix}#"
            kprint(f"MQTT connected, subscribing to {sub}")
            client.subscribe(sub)
        elif rc == 5:
            kprint(f"ERROR MQTT connection failed: unauthorized - {rc}")
        else:
            kprint(f"ERROR MQTT connection failed: {rc}")

    def on_disconnect(client, userdata, rc):
        kprint(f"WARN MQTT disconnected: {rc}")

    def on_message(client, userdata, msg):
        try:
            parsed = parse_topic(msg.topic, mqtt_prefix)
            if parsed is None:
                if debug:
                    kprint(f"ignoring topic {msg.topic}")
                return
            device, category, raw_property = parsed
            payload = msg.payload.decode(errors="replace")

            event_type = None
            if category == "event":
                # event payloads are JSON like {"event_type": "..."}
                try:
                    parsed_payload = json.loads(payload)
                    if isinstance(parsed_payload, dict):
                        event_type = parsed_payload.get("event_type")
                        payload = event_type if event_type is not None else payload
                except ValueError:
                    pass

            value_str, value_double, value_bool = coerce_value(payload)
            prop = readable_property(device, raw_property)
            short_name = prop.split(".")[-1] if "." in prop else prop
            ts = datetime.now(timezone.utc)

            record = {
                "device": device,
                "category": category,
                "property": prop,
                "property_raw": raw_property,
                "name": short_name,
                "value": value_str,
                "value_double": value_double,
                "value_bool": value_bool,
                "event_type": event_type,
                "ts_epoch_ms": int(ts.timestamp() * 1000),
                "ts_iso": ts.isoformat(),
            }

            topic = topic_for(device)
            if outbox.pending_files():
                # A backlog is being replayed; buffer this record too so it
                # can't overtake the older ones still waiting on disk.
                outbox.write(topic, [(device, record)])
            else:
                before = sink.delivery_errors
                sink.produce_one(topic, device, record)
                if sink.delivery_errors > before:
                    kprint(f"WARN kafka produce failed, buffering: {sink.last_error}")
                    outbox.write(topic, [(device, record)])
            # Space records by >=1ms so every record gets a distinct ts_epoch_ms
            # (and therefore a distinct event_time). This prevents the database
            # primary key (device, property_raw, event_time) from colliding when
            # the same property is reported twice within the same millisecond.
            time.sleep(0.001)
            if debug:
                kprint(f"{topic} <- {record}")
        except Exception as e:
            print(now(), "ERROR processing", msg.topic, e, file=sys.stderr, flush=True)

    # Make the MQTT client-id unique per process. A fixed id means two instances
    # (e.g. the systemd service plus a manual run, or a restart overlap) connect
    # with the same id and the broker evicts each in turn -- an endless
    # connect/disconnect (rc 7) fight. host+pid guarantees no collision.
    mqtt_client_id = f"{kafka_clientname}-{socket.gethostname()}-{os.getpid()}"
    client = mqtt.Client(mqtt_client_id)
    if mqtt_username and mqtt_password:
        client.username_pw_set(mqtt_username, mqtt_password)
    if mqtt_ssl:
        if mqtt_cafile and mqtt_certfile and mqtt_keyfile:
            client.tls_set(
                ca_certs=mqtt_cafile,
                certfile=mqtt_certfile,
                keyfile=mqtt_keyfile,
                cert_reqs=ssl.CERT_REQUIRED,
            )
        else:
            client.tls_set(cert_reqs=ssl.CERT_NONE)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    pending = len(outbox.pending_files())
    if pending:
        kprint(f"outbox {buffer_dir} has {pending} pending file(s) from a previous run")

    # Replay backlog and keep retrying it on an interval, independent of whether
    # new MQTT messages are arriving.
    threading.Thread(target=drain_loop, name="outbox-drain", daemon=True).start()

    kprint(f"connecting to MQTT {mqtt_host}:{mqtt_port}, producing to {kafka_bootstrap}")
    client.connect(host=mqtt_host, port=mqtt_port, keepalive=70)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        kprint("shutting down, flushing producer...")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    hc2kafka(auto_envvar_prefix="HCKAFKA")
