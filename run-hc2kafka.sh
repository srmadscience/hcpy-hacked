#!/usr/bin/env bash
# Start the hcpy MQTT -> Kafka (Avro) bridge (hc2kafka.py).
#
# Usage, from the hcpy directory:
#   ./run-hc2kafka.sh                 # uses config.ini
#   ./run-hc2kafka.sh --debug         # any extra args pass through to hc2kafka.py
#
set -euo pipefail

# Always work from the directory this script lives in, so the relative
# config.ini / venv / hc2kafka.py paths resolve no matter where it is called.
cd "$(dirname "$0")"

# Activate the project virtualenv if one is present (venv, then .venv).
if [ -f venv/bin/activate ]; then
    # shellcheck disable=SC1091
    source venv/bin/activate
elif [ -f .venv/bin/activate ]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi

# The bridge needs its Kafka/Avro dependencies.
if ! python3 -c "import confluent_kafka" 2>/dev/null; then
    echo "Kafka dependencies missing. Install them with:" >&2
    echo "    pip3 install -r requirements-kafka.txt" >&2
    exit 1
fi

# config.ini holds the MQTT broker + Kafka/Schema-Registry settings.
if [ ! -f config.ini ]; then
    echo "config.ini not found in $(pwd) -- create it first (see README)." >&2
    exit 1
fi

# exec so signals (Ctrl-C / SIGTERM) reach Python directly and extra args pass through.
exec python3 hc2kafka.py --config config.ini "$@"
