# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

hcpy talks to Bosch-Siemens (Home Connect) appliances **directly over the local
network** — no cloud round-trip during normal operation — and bridges them to
MQTT (with optional Home Assistant autodiscovery). The cloud is only contacted
once, during login, to harvest the per-device encryption keys and the XML files
that describe each appliance's capabilities.

## Commands

```bash
# Dev environment
python3 -m venv venv && source venv/bin/activate
pip3 install -r requirements.txt          # runtime deps
pip3 install -r tests/requirements-test.txt   # adds pytest

# Tests (CI runs this on Python 3.11)
pytest tests/ -v
pytest tests/test_HADiscovery.py::TestHADiscovery::test_basic_discovery_publication -v   # single test

# Lint / format — enforced via pre-commit, line length 99
pre-commit run --all-files
# (black --line-length 99, flake8 --max-line-length 99, isort, autoflake, yamllint)

# One-time: authenticate to the cloud and write config/devices.json
./hc-login.py config/devices.json

# Run the bridge
./hc2mqtt.py --config config/config.ini

# Docker
docker-compose -f compose.yaml build
docker-compose -f compose.yaml run app /app/hc-login.py config/devices.json
docker-compose -f compose.yaml up
```

**macOS sslpsk note:** on Python < 3.13, `sslpsk` is required and needs brew's
openssl: `brew install openssl` then
`LDFLAGS="-L$(brew --prefix openssl)/lib" CFLAGS="-I$(brew --prefix openssl)/include" pip3 install sslpsk`.
On Python ≥ 3.13 `sslpsk` is **not** installed — native `ssl.HAS_PSK` is used instead (see `wrap_socket_psk`).

## Architecture

The whole system is driven by one artifact: **`config/devices.json`**, produced
by `hc-login.py` and consumed by everything else. Understand its shape first —
each device has `host`, `key` (PSK), optional `iv`, a `description`, and a
`features` dict keyed by **UID** (string). Each feature carries `name`,
`access`, `refCID`/`refDID` (the type codes that decide how a value is
interpreted and what HA entity it becomes), and optionally `values` (enum
index→label), `min`/`max`/`stepSize`.

Pipeline, from cloud to MQTT:

1. **`hc-login.py`** — OAuth/PKCE flow against Home Connect. The login URL is
   opened manually in a browser; the user pastes back the `code`/`state` from
   the `hcauth://auth` redirect (there's now a CAPTCHA, hence the manual step).
   Downloads each appliance's `*_FeatureMapping.xml` + `*_DeviceDescription.xml`
   zip and writes `devices.json`. **Run once**, or when devices change.

2. **`HCxml2json.py`** (`xml2json`) — collapses those two XML files into the
   flat UID-keyed `features` map. The `refCID`/`refDID`/enum resolution that
   downstream code relies on is established here.

3. **`HCSocket.py`** (`HCSocket`) — the encrypted transport. Two modes,
   auto-selected by whether the device has an `iv`:
   - **TLS-PSK** (`wss://…:443`, no `iv`): TLS with a pre-shared key and
     non-standard ciphers. Uses native Python TLS-PSK on 3.13+, falls back to
     monkey-patched `sslpsk` below that.
   - **AES-CBC + HMAC** (`ws://…:80`, has `iv`): plaintext websocket carrying a
     self-rolled encrypted frame format. `encrypt`/`decrypt` chain CBC state and
     HMAC across messages, so **frames are stateful** — `reset()` must run on
     every (re)connect or the stream desyncs.

4. **`HCDevice.py`** (`HCDevice`) — protocol/state layer over the socket.
   - `handle_message` dispatches the appliance's JSON envelope by
     `resource` + `action`. The handshake matters: the device POSTs
     `/ei/initialValues` first, which seeds `session_id` and `tx_msg_id`; we
     reply, then a background thread runs `reconnect()` which walks the device
     through `/ci/services`, auth, `/ni/info`, and the `/ro/*` value fetches.
   - `parse_values` turns raw `{uid, value}` events into `{name: value}`,
     resolving enums and booleans via `refCID`/`refDID`, and mapping program
     UIDs to program names.
   - **Outbound commands are validated before send**: `get()` routes POSTs to
     `/ro/values`, `/ro/activeProgram`, `/ro/selectedProgram` through
     `test_feature` / `test_program_data`, which check access level, enum/range
     validity, and translate friendly names→UIDs. New write paths should go
     through this validation, not around it.

5. **`hc2mqtt.py`** — the orchestrator and entrypoint. Spawns one `client_connect`
   **thread per device** (each owns its own `HCSocket`/`HCDevice` and reconnect
   loop), and runs a single shared paho-mqtt client. Maps appliance state →
   `{prefix}{device}/state/{name}` and events → `…/event/{name}` (names
   lowercased, `.`→`_`). Inbound MQTT on `…/set`, `…/activeProgram`,
   `…/selectedProgram` is decoded and handed to `HCDevice.get(...)`. Liveness is
   tracked with retained `LWT` topics at both the bridge and per-device level.

6. **`HADiscovery.py`** (`publish_ha_discovery`) — pure mapping layer from
   `features` to Home Assistant MQTT-discovery configs. The big
   `refCID`/`refDID` branch decides component type (sensor / binary_sensor /
   switch / number / select / button / event). Behaviour is tuned via
   **`discovery.yaml`** (`MAGIC_OVERRIDES`, `SKIP_ENTITIES`, `DISABLED_ENTITIES`,
   `EXPAND_NAME`, `ADDITIONAL_FEATURES`, …) — prefer adjusting that YAML over
   hardcoding device-specific behaviour. This is the only module with unit tests
   (`tests/`, fully mocked — no network or MQTT).

## Configuration & conventions

- **`config.ini`** options mirror the `hc2mqtt.py` click options 1:1; every
  option is also settable via `HCPY_`-prefixed env vars (`auto_envvar_prefix`),
  which is how the Home Assistant addon (`run.sh` + `bashio`) injects config.
- `refCID`/`refDID` pairs are the type system of this codebase. When adding
  appliance support, the question is almost always "what does this CID/DID map
  to?" — match the existing branches in `HCDevice.parse_values` and
  `HADiscovery.publish_ha_discovery`.
- `config/devices.json` contains live encryption keys — it's gitignored; never
  commit a real one. The `tests/` fixtures use obfuscated keys.
- Local-only beta project; the MQTT JSON format is explicitly "likely to change"
  and is a thin layer over the appliance XML.
