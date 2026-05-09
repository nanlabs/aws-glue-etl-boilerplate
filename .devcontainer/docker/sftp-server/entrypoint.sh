#!/bin/sh
set -e

echo "🌐 SFTP Server Init"
echo "══════════════════════════════"

# All config from .env
KEYS_DIR="${SFTP_KEYS_DIR:-/keys}"
AUTHORIZED_KEYS="${KEYS_DIR}/authorized_keys"
USER="${SFTP_USER:-sftpuser}"

echo "Config:"
echo "  Keys: ${KEYS_DIR}"
echo "  User: ${USER}"

# Verify keys exist
if [ ! -f "${AUTHORIZED_KEYS}" ]; then
    echo "❌ Keys not found: ${AUTHORIZED_KEYS}"
    exit 1
fi

echo "✅ Keys found"

# Configure SSH
USER_HOME="/home/${USER}"
SSH_DIR="${USER_HOME}/.ssh"

mkdir -p "${SSH_DIR}"
cp "${AUTHORIZED_KEYS}" "${SSH_DIR}/authorized_keys"
chmod 700 "${SSH_DIR}"
chmod 600 "${SSH_DIR}/authorized_keys"
chown -R 1000:1000 "${SSH_DIR}"

echo "✅ SSH configured"
echo "══════════════════════════════"

# Start SFTP (original entrypoint)
exec /entrypoint "${USER}::1000:1000:upload"
