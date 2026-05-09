#!/bin/sh
set -e

echo "🔑 SFTP Keys Init Container"
echo "══════════════════════════════"

# All config comes from .env (via compose.yml environment)
# No hardcoded values here

KEYS_DIR="${SFTP_KEYS_DIR:-/keys}"
PRIVATE_KEY="${KEYS_DIR}/${SFTP_KEY_NAME}"
PUBLIC_KEY="${KEYS_DIR}/${SFTP_KEY_NAME}.pub"
AUTHORIZED_KEYS="${KEYS_DIR}/authorized_keys"

echo "Config:"
echo "  Keys directory: ${KEYS_DIR}"
echo "  Key name: ${SFTP_KEY_NAME}"
echo "  Key type: ${SFTP_KEY_TYPE}"

# Ensure directory exists
mkdir -p "${KEYS_DIR}"

# Generate keys if they don't exist
if [ ! -f "${PRIVATE_KEY}" ]; then
    echo "📝 Generating new ${SFTP_KEY_TYPE} key pair..."
    ssh-keygen -t "${SFTP_KEY_TYPE}" \
        -f "${PRIVATE_KEY}" \
        -N "" \
        -C "${SFTP_KEY_COMMENT}" \
        -q

    # Create authorized_keys from public key
    cp "${PUBLIC_KEY}" "${AUTHORIZED_KEYS}"

    echo "✅ Keys generated"
else
    echo "✅ Keys already exist"
fi

# Set permissions
# Private key: 644 (readable by all containers, but volume is read-only for consumers)
chmod 644 "${PRIVATE_KEY}" 2>/dev/null || true
chmod 644 "${PUBLIC_KEY}" 2>/dev/null || true
chmod 644 "${AUTHORIZED_KEYS}" 2>/dev/null || true

# Verify
if [ -f "${PRIVATE_KEY}" ] && [ -f "${AUTHORIZED_KEYS}" ]; then
    echo "✅ Verification passed"
    ls -lh "${KEYS_DIR}/" || true
    echo "══════════════════════════════"
    echo "✅ Keys ready"
    exit 0
else
    echo "❌ Verification failed"
    exit 1
fi
