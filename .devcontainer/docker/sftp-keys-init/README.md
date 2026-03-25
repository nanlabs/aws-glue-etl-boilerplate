# Init Containers

Init containers run ONCE before main services start. They perform setup tasks like generating SSH keys, creating configuration files, or preparing the environment.

## Structure

```
init/
├── sftp-keys/          # SFTP SSH keys generation
│   └── entrypoint.sh   # Key generation script
└── README.md           # This file
```

## How It Works

1. Init containers run with `restart: "no"` policy
2. They execute their task and exit
3. Main services wait with `depends_on: condition: service_completed_successfully`
4. Once init completes, main services start

## Configuration

All configuration comes from `.env` file. No hardcoded values in scripts or compose.yml.

Example from `.env`:
```bash
SFTP_KEYS_DIR=/keys
SFTP_KEY_NAME=sftp_key
SFTP_KEY_TYPE=ed25519
SFTP_KEY_COMMENT=awsglue-sftp-client
SFTP_USER=sftpuser
```

## Adding New Init Containers

1. Create directory: `init/<name>/`
2. Add script: `init/<name>/entrypoint.sh`
3. Make executable: `chmod +x init/<name>/entrypoint.sh`
4. Add to compose.yml:

```yaml
<name>-init:
  image: alpine:latest
  env_file:
    - .env
  volumes:
    - ./docker/init/<name>:/scripts:ro
  command: ["/bin/sh", "/scripts/entrypoint.sh"]
  restart: "no"
```

5. Add dependency to services:

```yaml
your-service:
  depends_on:
    <name>-init:
      condition: service_completed_successfully
```

## Best Practices

- ✅ Scripts must be idempotent (safe to run multiple times)
- ✅ Use environment variables from .env for configuration
- ✅ Fail fast with `set -e`
- ✅ Clear logging with echo statements
- ✅ Verify results before exiting
- ✅ Exit 0 on success, non-zero on failure
- ✅ Include operation summary in logs

## Testing Init Scripts

Run scripts directly to test:

```bash
# Test SFTP keys generation
docker run --rm \
  -v $(pwd)/.docker-volumes/keys:/keys \
  -e SFTP_KEYS_DIR=/keys \
  -e SFTP_KEY_NAME=sftp_key \
  -e SFTP_KEY_TYPE=ed25519 \
  -e SFTP_KEY_COMMENT=test \
  alpine:latest sh -c "
    apk add --no-cache openssh-keygen &&
    sh /scripts/entrypoint.sh
  "
```

## Common Use Cases

- **SSH Keys**: Generate keys for SFTP/SSH authentication
- **Database Migration**: Run schema migrations before app starts
- **Config Generation**: Create configuration files from templates
- **Data Seeding**: Load initial/sample data
- **Certificate Generation**: Create SSL/TLS certificates
- **Dependency Download**: Fetch required files or artifacts
