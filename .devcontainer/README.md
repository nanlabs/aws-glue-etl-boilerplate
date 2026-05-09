# Dev Container Configuration

This directory contains the complete development environment configuration for the AWS Glue Data Lake Jobs project.

## ⚠️ Development Approach

This project **EXCLUSIVELY** supports development through Dev Containers. There is no manual setup alternative.

## 📁 Directory Structure

```
.devcontainer/
├── devcontainer.json          # VS Code dev container configuration
├── compose.yml               # Docker services orchestration
├── docker/                   # Service-specific configurations
│   ├── awsglue/             # AWS Glue environment setup
│   │   ├── notebooks/       # Sample Jupyter notebooks
│   │   ├── spark-defaults.conf/ # Spark configuration
│   │   ├── start-development.sh
│   │   └── start-jupyter.sh
│   └── localstack/          # LocalStack AWS simulation
│       ├── init.d/          # Initialization scripts
│       └── s3/              # S3 bucket setup
└── README.md                # This file
```

## 🔧 Configuration Files

### `devcontainer.json`

VS Code dev container configuration including:
- Pre-installed extensions (Python, Jupyter, AWS Toolkit, etc.)
- Port forwarding (8888, 4040, 4566)
- Post-creation scripts
- User and workspace settings

### `compose.yml`

Docker Compose configuration with services:
- **awsglue**: Main development container with AWS Glue 5.0 simulation
- **localstack**: AWS services simulation (S3, Glue Catalog, IAM)

## 🚀 Services

### AWS Glue Container (`awsglue`)

- **Base Image**: Custom Dockerfile with AWS Glue 5.0
- **Python**: 3.11 with pipenv
- **Spark**: 3.5.4 with Iceberg support
- **Jupyter Lab**: Pre-configured with AWS Glue libraries
- **Volumes**: Project workspace and persistent Jupyter data

### LocalStack Container (`localstack`)

- **Image**: `localstack/localstack:3.0`
- **Services**: S3, Glue Data Catalog, IAM, STS
- **Initialization**: Automatic bucket creation and sample data
- **Persistence**: Data persists between container restarts

## 🔗 Networking

All services communicate through the `glue-iceberg-network` bridge network:
- Container-to-container communication via service names
- Host port forwarding for web interfaces

## 💾 Volumes

- **awsglue-jupyter**: Persistent Jupyter Lab workspace
- **localstack-data**: Persistent LocalStack data
- **spark-events**: Spark history server events
- **Host mounts**: AWS credentials and SSH keys (read-only)

## 🛠️ Customization

### Adding New Services

To add a new service to the dev container:

1. Update `compose.yml` with the new service
2. Add port forwarding in `devcontainer.json` if needed
3. Update initialization scripts if required

### Modifying VS Code Extensions

Edit the `extensions` array in `devcontainer.json`:

```json
"extensions": [
  "amazonwebservices.aws-toolkit-vscode",
  "ms-python.python",
  // Add new extensions here
]
```

### Environment Variables

Service-specific environment variables are configured in `compose.yml`. Global variables should be added to the root `.env` file.

## 🐛 Troubleshooting

### Container Build Issues

```bash
# Rebuild dev container from VS Code
# Command Palette → "Dev Containers: Rebuild Container"

# Or with CLI
devcontainer up --workspace-folder . --remove-existing-container
```

### Service Health Checks

```bash
# Inside the dev container
make services-status
make validate

# Check individual services
curl http://localhost:4566/health  # LocalStack
curl http://localhost:8888        # Jupyter Lab
```

### Port Conflicts

If ports are already in use on your host machine:

1. Stop conflicting services
2. Modify port mappings in `compose.yml`
3. Update forwarded ports in `devcontainer.json`

## 🔒 Security

### Credential Mounting

The dev container safely mounts host credentials as read-only:
- `~/.aws/` → `/home/hadoop/.aws/` (AWS credentials)
- `~/.ssh/` → `/home/hadoop/.ssh/` (SSH keys)

### Network Isolation

Services communicate through an isolated Docker network, preventing interference with host networking.

## 📝 Best Practices

1. **Never modify running containers**: Make changes to configuration files and rebuild
2. **Use volume mounts**: Persist important data in named volumes
3. **Environment parity**: Keep dev container configuration in sync with CI/CD environments
4. **Resource limits**: Consider adding resource constraints for large teams

## 🚀 Getting Started

1. **Clone repository**
   ```bash
   git clone <repository-url>
   cd nan_data_jobs
   ```

2. **Open in VS Code**
   ```bash
   code .
   ```

3. **Start dev container**
   - Click "Reopen in Container" when prompted
   - Or use Command Palette: "Dev Containers: Reopen in Container"

4. **Verify setup**
   ```bash
   make validate
   make services-status
   ```

---

**Note**: This dev container configuration is the ONLY supported development approach for this project. It ensures consistency, reliability, and reduces setup complexity for all team members.

## 🚀 Quick Start

1. **Open in VS Code**
   ```bash
   code .
   ```

2. **Launch Dev Container**
   - When prompted, click "Reopen in Container"
   - Or use Command Palette: `Ctrl/Cmd+Shift+P` → "Dev Containers: Reopen in Container"

3. **Automatic Setup**
   The dev container will automatically:
   - Install all Python dependencies
   - Start LocalStack (AWS services simulation)
   - Launch Jupyter Lab
   - Configure pre-commit hooks
   - Set up the development environment

## 📁 Directory Structure

```
.devcontainer/
├── devcontainer.json    # Dev container configuration
├── compose.yml          # Docker Compose services
├── docker/              # Moved from root docker/ directory
│   ├── awsglue/        # AWS Glue container configuration
│   ├── localstack/     # LocalStack initialization scripts
│   └── notebooks/      # Jupyter notebook samples
└── README.md           # This file
```

## 🔧 Configuration Files

### `devcontainer.json`

Main configuration file that defines:
- **Container settings**: Service name, workspace folder, user
- **VS Code extensions**: Pre-installed extensions for Python, Jupyter, AWS, etc.
- **Port forwarding**: Automatically forwards service ports (8888, 4040, 4566)
- **Post-creation commands**: Scripts that run after container creation
- **Services**: Automatically starts LocalStack and AWW Glue services

### `compose.yml`

Docker Compose configuration that defines:
- **awsglue service**: Main development container with Spark and AWS Glue
- **localstack service**: Local AWS services (S3, Glue Data Catalog, IAM)
- **Networks and volumes**: Shared networking and persistent storage

## 🌐 Available Services

Once the dev container is running, these services are automatically available:

| Service | URL | Purpose |
|---------|-----|---------|
| **Jupyter Lab** | <http://localhost:8888> | Interactive development and notebooks |
| **Spark UI** | <http://localhost:4040> | Spark job monitoring (when jobs run) |
| **LocalStack** | <http://localhost:4566> | Local AWS services simulation |

## 🔨 Pre-installed Extensions

The dev container comes with these VS Code extensions:

- **Amazon Web Services Toolkit** - AWS integration and deployment
- **Python** - Python language support and IntelliSense  
- **Pylance** - Advanced Python type checking and analysis
- **Jupyter** - Jupyter notebook support in VS Code
- **Docker** - Container management and debugging
- **Black Formatter** - Automatic Python code formatting
- **Ruff** - Fast Python linter and code checker
- **YAML** - YAML file editing and validation
- **Markdown** - Documentation editing support

## 🚀 Development Workflow

### Daily Development

```bash
# Everything runs inside the dev container automatically
make test           # Run tests
make format         # Format code  
make lint          # Check code quality

# Run Glue jobs
glue-spark-submit jobs/bronze/my_job.py

# Access services directly in browser
# - Jupyter: http://localhost:8888
# - Spark UI: http://localhost:4040 (when jobs run)
# - LocalStack: http://localhost:4566
```

### Working with Jobs

```bash
# Create new jobs in appropriate directory
jobs/
├── bronze/    # Raw data ingestion
├── silver/    # Data transformation  
├── gold/      # Business analytics
└── utils/     # Infrastructure jobs

# Test jobs locally
glue-spark-submit jobs/utils/pyspark_hello_world.py
```

## 📦 Volume Persistence

The dev container uses Docker volumes to persist data:

- **Source code**: Mounted from host filesystem (live editing)
- **Jupyter notebooks**: Persisted in `awsglue-jupyter` volume
- **LocalStack data**: Persisted in `localstack-data` volume  
- **Spark events**: Persisted in `spark-events` volume

## 🔧 Customization

### Adding New Extensions

Edit `.devcontainer/devcontainer.json`:

```json
{
  "customizations": {
    "vscode": {
      "extensions": [
        "existing.extensions",
        "new.extension.id"
      ]
    }
  }
}
```

### Changing Port Forwarding

Edit `.devcontainer/devcontainer.json`:

```json
{
  "forwardPorts": [8888, 4040, 4566, 9999]
}
```

### Adding New Services

Edit `.devcontainer/compose.yml` to add new Docker services.

## 🚨 Troubleshooting

### Container Won't Start

```bash
# Check Docker is running
docker --version

# Rebuild container
# Command Palette → "Dev Containers: Rebuild Container"
```

### Port Conflicts

```bash
# Check what's using ports
lsof -i :8888  # Jupyter
lsof -i :4566  # LocalStack

# Change ports in .devcontainer/compose.yml if needed
```

### Missing Dependencies

```bash
# Rebuild container to refresh dependencies
# Command Palette → "Dev Containers: Rebuild Container"
```

## 🔄 Migration from Docker Compose

If you were previously using `docker compose up` directly:

1. **Stop existing containers**:
   ```bash
   docker compose down
   ```

2. **Open in VS Code dev container** (recommended):
   ```bash
   code .
   # Click "Reopen in Container"
   ```

3. **Or continue with docker compose** (backward compatibility):
   ```bash
   make up  # Still works, but dev container is recommended
   ```

## 📚 Related Documentation

- [Development Setup Guide](../docs/DEV_SETUP.md) - Complete setup instructions
- [Main README](../README.md) - Project overview and quick start
- [Best Practices](../docs/BEST_PRACTICES.md) - Development guidelines

---

**🎯 For the best development experience, use the VS Code Dev Container!**
