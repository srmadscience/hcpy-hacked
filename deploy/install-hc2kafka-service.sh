#!/usr/bin/env bash
# Install the hc2kafka bridge as a systemd service on a Raspberry Pi (or any
# systemd Linux). Resolves the current user and the hcpy checkout location,
# substitutes them into deploy/hc2kafka.service, and enables the unit.
#
# Usage, from anywhere:
#   sudo ./deploy/install-hc2kafka-service.sh          # service runs as the
#                                                      # invoking (sudo) user
#   sudo SERVICE_USER=pi ./deploy/install-hc2kafka-service.sh
#
set -euo pipefail

UNIT_NAME="hc2kafka.service"

# Directory of this script -> the hcpy checkout (its parent).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKDIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
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
# under sudo is still root's). The log lives at ~/run-hc2kafka.log.
HOME_DIR="$(getent passwd "${SERVICE_USER}" | cut -d: -f6)"
if [ -z "${HOME_DIR}" ]; then
    echo "Could not resolve home directory for user '${SERVICE_USER}'." >&2
    exit 1
fi
LOGFILE="${HOME_DIR}/run-hc2kafka.log"

echo "Installing ${UNIT_NAME}:"
echo "    user    = ${SERVICE_USER}"
echo "    workdir = ${WORKDIR}"
echo "    logfile = ${LOGFILE}"

# Sanity-check the launcher is present and executable.
if [ ! -x "${WORKDIR}/run-hc2kafka.sh" ]; then
    echo "Warning: ${WORKDIR}/run-hc2kafka.sh is not executable; fixing." >&2
    chmod +x "${WORKDIR}/run-hc2kafka.sh"
fi

# systemd's append: target must already exist and be writable by SERVICE_USER.
touch "${LOGFILE}"
chown "${SERVICE_USER}:${SERVICE_USER}" "${LOGFILE}"

DEST="/etc/systemd/system/${UNIT_NAME}"
sed -e "s#__USER__#${SERVICE_USER}#g" \
    -e "s#__WORKDIR__#${WORKDIR}#g" \
    -e "s#__HOME__#${HOME_DIR}#g" \
    -e "s#__LOGFILE__#${LOGFILE}#g" \
    "${TEMPLATE}" > "${DEST}"

# Install logrotate config so ~/run-hc2kafka.log doesn't grow unbounded.
# copytruncate keeps the same inode the systemd append: stream is writing to,
# so we don't need to restart the service on rotation.
LOGROTATE_TEMPLATE="${SCRIPT_DIR}/hc2kafka.logrotate"
if [ -f "${LOGROTATE_TEMPLATE}" ]; then
    LOGROTATE_DEST="/etc/logrotate.d/hc2kafka"
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
echo "    systemctl status hc2kafka"
echo "    tail -f ${LOGFILE}"
echo "    journalctl -u hc2kafka -f      # systemd-level start/stop/crash info"
echo "    sudo systemctl restart hc2kafka"
echo "    sudo logrotate -d /etc/logrotate.d/hc2kafka   # dry-run rotation"
