# SFTP Keys Architecture

## Overview

This document describes the SSH key management architecture for SFTP authentication in the local development environment.

## Design Principles

1. **Separation of Concerns**: Init container generates, runtime containers consume
2. **Least Privilege**: Runtime containers mount keys read-only
3. **Path Consistency**: Standard paths based on container responsibility
4. **Environment Variables**: Centralized configuration via `.env` files

## Architecture

```
┌──────────────────────┐
│ sftp-keys-init       │  Generates SSH keys
│ Mount: /keys (RW)    │  ONE-TIME EXECUTION
└──────────────────────┘
          ↓
┌──────────────────────────────────────┐
│ Docker Volume: .docker-volumes/keys  │  Persistent storage
│ ├── sftp_key (private)                │  Outside host filesystem
│ ├── sftp_key.pub (public)             │  Shared across containers
│ └── authorized_keys                   │  Generated once, reused
└──────────────────────────────────────┘
          ↓ (read-only mounts)
┌──────────────────────┬──────────────────────┬──────────────────────┐
│ awsglue              │ localstack           │ sftp-server          │
│ /var/sftp/keys (RO)  │ /var/sftp/keys (RO)  │ /keys (RO)           │
│ - Reads private key  │ - Reads private key  │ - Uses authorized    │
│ - SFTP client auth   │ - Creates secrets    │   keys for auth      │
└──────────────────────┴──────────────────────┴──────────────────────┘
```

## Path Standards

### Generation Context (Write)
- **Path**: `/keys`
- **Containers**: `sftp-keys-init`, `sftp-server`
- **Rationale**: Standard SSH/SFTP key location, matches OpenSSH conventions
- **Permission**: Read-write for init, read-only for server

### Runtime Context (Read)
- **Path**: `/var/sftp/keys`
- **Containers**: `awsglue`, `localstack`
- **Rationale**:
  - Follows FHS (Filesystem Hierarchy Standard)
  - `/var` = variable runtime data
  - Namespace isolation (`/var/sftp/`) prevents conflicts
  - Not in `/tmp` (these are not temporary files)
- **Permission**: Read-only

## Environment Variables

### Primary Configuration (.env)
```bash
# SFTP Keys Configuration
SFTP_KEYS_DIR=/keys                    # Init container path
SFTP_KEYS_RUNTIME_DIR=/var/sftp/keys   # Runtime containers path
SFTP_KEY_NAME=sftp_key
SFTP_KEY_TYPE=ed25519
SFTP_KEY_COMMENT=sftp-dev@localstack
```

### LocalStack Environment (localstack.env)
```bash
# Used by init.d scripts when creating secrets
export SFTP_KEYS_DIR=/var/sftp/keys
```

## File Permissions

| File                 | Permission | Owner  | Usage                    |
|---------------------|------------|--------|--------------------------|
| sftp_key             | 600        | root   | Private key (SFTP client)|
| sftp_key.pub         | 644        | root   | Public key (reference)   |
| authorized_keys      | 644        | root   | Server authentication    |

## Container Mount Strategy

### sftp-keys-init
```yaml
volumes:
  - ./.docker-volumes/keys:${SFTP_KEYS_DIR:-/keys}
```
- **Purpose**: Generate keys once at startup
- **Access**: Read-write (creates files)
- **Lifecycle**: Runs once, exits successfully

### awsglue
```yaml
volumes:
  - ./.docker-volumes/keys:${SFTP_KEYS_RUNTIME_DIR:-/var/sftp/keys}:ro
```
- **Purpose**: Use private key for SFTP authentication
- **Access**: Read-only (security)
- **Dependency**: Waits for sftp-keys-init completion

### localstack
```yaml
volumes:
  - ./.docker-volumes/keys:${SFTP_KEYS_RUNTIME_DIR:-/var/sftp/keys}:ro
```
- **Purpose**: Read private key to create AWS Secrets Manager secret
- **Access**: Read-only (security)
- **Dependency**: Waits for sftp-keys-init completion

### sftp-server
```yaml
volumes:
  - ./.docker-volumes/keys:${SFTP_KEYS_DIR:-/keys}:ro
```
- **Purpose**: Use authorized_keys for SSH authentication
- **Access**: Read-only (security)
- **Dependency**: Waits for sftp-keys-init completion

## Security Considerations

1. **No Host Filesystem**: Keys stored in Docker volume, not on host
2. **Read-Only Mounts**: Runtime containers cannot modify keys
3. **Key Rotation**: Delete volume to regenerate keys
4. **Ed25519 Algorithm**: Modern, secure elliptic curve cryptography
5. **No Passphrases**: Automated workflows (acceptable for local dev)

## Usage

### First Run
```bash
# Keys generated automatically on first compose up
docker compose up -d

# Verify keys exist
docker compose exec sftp-server ls -la /keys/
```

### Key Rotation
```bash
# Stop all services
docker compose down

# Delete volume
docker volume rm devcontainer_keys

# Restart (keys will be regenerated)
docker compose up -d
```

### Troubleshooting

**Keys not found:**
```bash
# Check init container logs
docker compose logs sftp-keys-init

# Verify volume exists
docker volume inspect devcontainer_keys

# Check permissions
docker compose exec sftp-server ls -la /keys/
```

**SFTP authentication fails:**
```bash
# Verify keys in awsglue container
docker compose exec awsglue ls -la /var/sftp/keys/

# Check secret in LocalStack
source localstack.env
awslocal secretsmanager get-secret-value \
  --secret-id your-project/public-api
```

## References

- [Filesystem Hierarchy Standard](https://refspecs.linuxfoundation.org/FHS_3.0/fhs/index.html)
- [OpenSSH Key Format](https://www.openssh.com/txt/release-8.2)
- [Docker Volume Documentation](https://docs.docker.com/storage/volumes/)
- [Ed25519 Signature Scheme](https://ed25519.cr.yp.to/)
