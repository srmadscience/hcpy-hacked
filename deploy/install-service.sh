#!/usr/bin/env bash
# Install an hcpy bridge (hc2kafka or hc2mqtt) as a systemd service on a
# Raspberry Pi (or any systemd Linux). Resolves the service user and the hcpy
# checkout location, substitutes them into deploy/<service>.service, installs
# a logrotate config for its log, and enables + (re)starts the unit.
#
# Usage, from anywhere:
#   sudo ./deploy/install-service.sh hc2kafka      # service runs as the
#                                                  # invoking (sudo) user
#   sudo SERVICE_USER=pi ./deploy/install-service.sh hc2mqtt
#   sudo WORKDIR=/home/pi/hcpy ./deploy/install-service.sh hc2mqtt
#
# WORKDIR defaults to the checkout this script lives in. Override it to run the
# service from a different checkout (it must hold config.ini, config/ and a
# venv/ or .venv/).
set -euo pipefail

SERVICE="${1:-}"
case "${SERVICE}" in
    hc2kafka | hc2mqtt) ;;
    *)
        echo "Usage: $0 hc2kafka|hc2mqtt" >&2
        exit 1
        ;;
esac
UNIT_NAME="${SERVICE}.service"

# Directory of this script -> the hcpy checkout (its parent).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKDIR="$(cd "${WORKDIR:-${SCRIPT_DIR}/..}" && pwd)"
TEMPLATE="${SCRIPT_DIR}/${UNIT_NAME}"

if [ ! -f "${TEMPLATE}" ]; then
    echo "Template ${TEMPLATE} not found." >&2
    exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
    echo "Please run with sudo (needed to write /etc/systemd/system)." >&2
    exit 1
fi

# Run the service as the human who invoked sudo, unless SERVICE_USER is set.
SERVICE_USER="${SERVICE_USER:-${SUDO_USER:-root}}"

# Resolve that user's home directory from the passwd database (not $HOME, which
# under sudo is still root's). The log lives at ~/run-<service>.log.
HOME_DIR="$(getent passwd "${SERVICE_USER}" | cut -d: -f6)"
if [ -z "${HOME_DIR}" ]; then
    echo "Could not resolve home directory for user '${SERVICE_USER}'." >&2
    exit 1
fi
LOGFILE="${HOME_DIR}/run-${SERVICE}.log"

# The checkout's virtualenv interpreter (hc2mqtt.service runs it directly;
# hc2kafka goes through run-hc2kafka.sh, which activates the venv itself).
if [ -x "${WORKDIR}/venv/bin/python3" ]; then
    PYTHON="${WORKDIR}/venv/bin/python3"
elif [ -x "${WORKDIR}/.venv/bin/python3" ]; then
    PYTHON="${WORKDIR}/.venv/bin/python3"
else
    PYTHON="$(command -v python3)"
fi

echo "Installing ${UNIT_NAME}:"
echo "    user    = ${SERVICE_USER}"
echo "    workdir = ${WORKDIR}"
echo "    python  = ${PYTHON}"
echo "    logfile = ${LOGFILE}"

if [ ! -f "${WORKDIR}/${SERVICE}.py" ] || [ ! -f "${WORKDIR}/config.ini" ]; then
    echo "${WORKDIR} needs ${SERVICE}.py and config.ini." >&2
    exit 1
fi

# Sanity-check the hc2kafka launcher is present and executable.
if [ "${SERVICE}" = "hc2kafka" ] && [ ! -x "${WORKDIR}/run-hc2kafka.sh" ]; then
    echo "Warning: ${WORKDIR}/run-hc2kafka.sh is not executable; fixing." >&2
    chmod +x "${WORKDIR}/run-hc2kafka.sh"
fi

# systemd's append: target must already exist and be writable by SERVICE_USER.
touch "${LOGFILE}"
chown "${SERVICE_USER}:${SERVICE_USER}" "${LOGFILE}"

DEST="/etc/systemd/system/${UNIT_NAME}"
sed -e "s#__USER__#${SERVICE_USER}#g" \
    -e "s#__WORKDIR__#${WORKDIR}#g" \
    -e "s#__PYTHON__#${PYTHON}#g" \
    -e "s#__HOME__#${HOME_DIR}#g" \
    -e "s#__LOGFILE__#${LOGFILE}#g" \
    "${TEMPLATE}" > "${DEST}"

# Install logrotate config so the log doesn't grow unbounded. copytruncate
# keeps the same inode the systemd append: stream is writing to, so we don't
# need to restart the service on rotation.
LOGROTATE_TEMPLATE="${SCRIPT_DIR}/hcpy.logrotate"
if [ -f "${LOGROTATE_TEMPLATE}" ]; then
    LOGROTATE_DEST="/etc/logrotate.d/${SERVICE}"
    sed -e "s#__USER__#${SERVICE_USER}#g" \
        -e "s#__LOGFILE__#${LOGFILE}#g" \
        "${LOGROTATE_TEMPLATE}" > "${LOGROTATE_DEST}"
    echo "    logrotate = ${LOGROTATE_DEST}"
fi

systemctl daemon-reload
systemctl enable "${UNIT_NAME}"
systemctl restart "${UNIT_NAME}"

echo
echo "Done. Useful commands:"
echo "    systemctl status ${SERVICE}"
echo "    tail -f ${LOGFILE}"
echo "    journalctl -u ${SERVICE} -f      # systemd-level start/stop/crash info"
echo "    sudo systemctl restart ${SERVICE}"
echo "    sudo logrotate -d /etc/logrotate.d/${SERVICE}   # dry-run rotation"
