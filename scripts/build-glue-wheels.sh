#!/bin/bash
set -e

# 🚀 AWS Glue Optimized Wheel Builder
# Creates uber wheels optimized for AWS Glue 5.0 following AWS best practices
# Based on: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html#glue-python-libraries-best-practices

# Default values
REQUIREMENTS_FILE="requirements.txt"
FINAL_WHEEL_OUTPUT_DIRECTORY="build/glue-wheels"
PACKAGE_NAME="nan_data_jobs_libs"
PACKAGE_VERSION="1.0.0"
GLUE_VERSION="5.0"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print functions
print_header() {
    echo -e "${BLUE}=========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=========================================${NC}"
}

print_step() {
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}----------------------------------------${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Help message
show_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "AWS Glue Optimized Wheel Builder - Creates uber wheels for AWS Glue 5.0"
    echo ""
    echo "Options:"
    echo "  -r, --requirements FILE   Path to requirements.txt file (default: requirements.txt)"
    echo "  -o, --wheel-output DIR    Output directory for final wheel (default: build/glue-wheels)"
    echo "  -n, --name NAME           Package name (default: nan_data_jobs_libs)"
    echo "  -v, --version VERSION     Package version (default: 1.0.0)"
    echo "  -g, --glue-version VER    Glue version (default: 5.0)"
    echo "  -h, --help                Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Use defaults"
    echo "  $0 -v 1.2.3                         # Custom version"
    echo "  $0 -r custom-reqs.txt -o dist       # Custom requirements and output"
    echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
    -r | --requirements)
        REQUIREMENTS_FILE="$2"
        shift 2
        ;;
    -o | --wheel-output)
        FINAL_WHEEL_OUTPUT_DIRECTORY="$2"
        shift 2
        ;;
    -n | --name)
        PACKAGE_NAME="$2"
        shift 2
        ;;
    -v | --version)
        PACKAGE_VERSION="$2"
        shift 2
        ;;
    -g | --glue-version)
        GLUE_VERSION="$2"
        shift 2
        ;;
    -h | --help)
        show_help
        exit 0
        ;;
    *)
        echo "Unknown option: $1"
        show_help
        exit 1
        ;;
    esac
done

# If package name has dashes, convert to underscores and notify user
if [[ "$PACKAGE_NAME" =~ "-" ]]; then
    print_warning "Package name '$PACKAGE_NAME' contains dashes. Converting to underscores."
    PACKAGE_NAME=$(echo "$PACKAGE_NAME" | tr '-' '_')
fi

UBER_WHEEL_NAME="${PACKAGE_NAME}-${PACKAGE_VERSION}-py3-none-any.whl"

# Validate version format (basic check)
if [[ ! "$PACKAGE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] && [[ ! "$PACKAGE_VERSION" =~ ^[0-9]+\.[0-9]+$ ]]; then
    print_warning "Version '$PACKAGE_VERSION' doesn't follow semantic versioning (x.y.z or x.y)"
fi

# Get relevant platform tags/python versions based on glue version
if [[ "$GLUE_VERSION" == "5.0" ]]; then
    PYTHON_VERSION="3.11"
    GLIBC_VERSION="2.34"
elif [[ "$GLUE_VERSION" == "4.0" ]]; then
    PYTHON_VERSION="3.10"
    GLIBC_VERSION="2.26"
elif [[ "$GLUE_VERSION" == "3.0" ]]; then
    PYTHON_VERSION="3.7"
    GLIBC_VERSION="2.26"
elif [[ "$GLUE_VERSION" == "2.0" ]]; then
    PYTHON_VERSION="3.7"
    GLIBC_VERSION="2.17"
elif [[ "$GLUE_VERSION" == "1.0" ]]; then
    PYTHON_VERSION="3.6"
    GLIBC_VERSION="2.17"
elif [[ "$GLUE_VERSION" == "0.9" ]]; then
    PYTHON_VERSION="2.7"
    GLIBC_VERSION="2.17"
else
    print_error "Unsupported glue version '$GLUE_VERSION'."
    exit 1
fi

print_info "Using Glue version $GLUE_VERSION"
print_info "Using Python version $PYTHON_VERSION"
print_info "Using glibc version $GLIBC_VERSION"

# Function to check glibc compatibility
is_glibc_compatible() {
    # assumes glibc version in the form of major.minor (ex: 2.17)
    # glue glibc must be >= platform glibc
    local glue_glibc_version="$GLIBC_VERSION"
    local platform_glibc_version="$1"

    # 2.27 (platform) can run on 2.27 (glue)
    if [[ "$platform_glibc_version" == "$glue_glibc_version" ]]; then
        return 0
    fi

    local glue_glibc_major="${glue_glibc_version%%.*}"
    local glue_glibc_minor="${glue_glibc_version#*.}"
    local platform_glibc_major="${platform_glibc_version%%.*}"
    local platform_glibc_minor="${platform_glibc_version#*.}"

    # 3.27 (platform) cannot run on 2.27 (glue)
    if [[ "$platform_glibc_major" -gt "$glue_glibc_major" ]]; then
        return 1
    fi

    # 2.34 (platform) cannot run on 2.27 (glue)
    if [[ "$platform_glibc_major" -eq "$glue_glibc_major" ]] && [[ "$platform_glibc_minor" -gt "$glue_glibc_minor" ]]; then
        return 1
    fi

    # 2.17 (platform) can run on 2.27 (glue)
    return 0
}

# Build platform flags
PIP_PLATFORM_FLAG=""
if is_glibc_compatible "2.17"; then
    PIP_PLATFORM_FLAG="${PIP_PLATFORM_FLAG} --platform manylinux2014_x86_64"
fi
if is_glibc_compatible "2.28"; then
    PIP_PLATFORM_FLAG="${PIP_PLATFORM_FLAG} --platform manylinux_2_28_x86_64"
fi
if is_glibc_compatible "2.34"; then
    PIP_PLATFORM_FLAG="${PIP_PLATFORM_FLAG} --platform manylinux_2_34_x86_64"
fi
if is_glibc_compatible "2.39"; then
    PIP_PLATFORM_FLAG="${PIP_PLATFORM_FLAG} --platform manylinux_2_39_x86_64"
fi

print_info "Using pip platform flags: $PIP_PLATFORM_FLAG"

# Generate requirements.txt if it doesn't exist
if [ ! -f "$REQUIREMENTS_FILE" ] && [ -f "Pipfile" ]; then
    print_info "Generating requirements.txt from Pipfile..."
    if command -v pipenv >/dev/null 2>&1; then
        pipenv requirements > "$REQUIREMENTS_FILE"
        print_success "Generated requirements.txt from Pipfile"
    else
        print_error "Pipfile found but pipenv not available. Please install pipenv or provide requirements.txt"
        exit 1
    fi
fi

# Check if requirements file exists
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    print_error "Requirements file '$REQUIREMENTS_FILE' not found."
    print_info "Please create a requirements.txt file or ensure Pipfile exists with pipenv installed."
    exit 1
fi

# Convert to absolute paths
REQUIREMENTS_FILE=$(realpath "$REQUIREMENTS_FILE")
FINAL_WHEEL_OUTPUT_DIRECTORY=$(realpath "$FINAL_WHEEL_OUTPUT_DIRECTORY")
TEMP_WORKING_DIR=$(mktemp -d)
VENV_DIR="${TEMP_WORKING_DIR}/.build_venv"
WHEEL_OUTPUT_DIRECTORY="${TEMP_WORKING_DIR}/wheelhouse"

# Cleanup function
cleanup() {
    print_info "Cleaning up temporary files..."
    rm -rf "$TEMP_WORKING_DIR"
}
trap cleanup EXIT

print_header "Building AWS Glue Uber Wheel for $PACKAGE_NAME"
print_info "Package: $PACKAGE_NAME v$PACKAGE_VERSION"
print_info "Requirements: $REQUIREMENTS_FILE"
print_info "Output: $FINAL_WHEEL_OUTPUT_DIRECTORY/$UBER_WHEEL_NAME"

# Determine Python executable to use consistently
PYTHON_EXEC=$(which python3 2>/dev/null || which python 2>/dev/null)
if [ -z "$PYTHON_EXEC" ]; then
    print_error "No Python executable found"
    exit 1
fi

print_info "Using Python: $PYTHON_EXEC"

# Install build requirements
print_step "Step 1/5: Installing build tools..."
"$PYTHON_EXEC" -m pip install --upgrade pip build wheel setuptools
print_success "Build tools installed successfully"
echo ""

# Create a virtual environment for building
print_step "Step 2/5: Creating build environment..."
"$PYTHON_EXEC" -m venv "$VENV_DIR"

# Check if virtual environment was created successfully
if [ ! -f "$VENV_DIR/bin/activate" ]; then
    print_error "Failed to create virtual environment"
    exit 1
fi

source "$VENV_DIR/bin/activate"

# Install pip-tools for dependency resolution
"$VENV_DIR/bin/pip" install pip-tools
print_success "Build environment created successfully"
echo ""

# Compile requirements to get all transitive dependencies
GLUE_PIP_ARGS="$PIP_PLATFORM_FLAG --python-version $PYTHON_VERSION --only-binary=:all:"
print_step "Step 3/5: Resolving all dependencies..."

if ! "$VENV_DIR/bin/pip-compile" --pip-args "$GLUE_PIP_ARGS" --no-emit-index-url --output-file "$TEMP_WORKING_DIR/.compiled_requirements.txt" "$REQUIREMENTS_FILE"; then
    print_error "Failed to resolve dependencies. Check for conflicts in $REQUIREMENTS_FILE"
    exit 1
fi

print_success "Dependencies resolved successfully"
echo ""

# Download all wheels for dependencies
print_step "Step 4/5: Downloading all dependency wheels..."
"$VENV_DIR/bin/pip" download -r "$TEMP_WORKING_DIR/.compiled_requirements.txt" -d "$WHEEL_OUTPUT_DIRECTORY" $GLUE_PIP_ARGS

# Check if any wheels were downloaded
if [ ! "$(ls -A "$WHEEL_OUTPUT_DIRECTORY")" ]; then
    print_error "No wheels were downloaded. Check your requirements file."
    exit 1
fi

# Count downloaded wheels (using find instead of ls for better handling)
WHEEL_COUNT=$(find "$WHEEL_OUTPUT_DIRECTORY" -name "*.whl" -type f | wc -l | tr -d ' ')
print_success "Downloaded $WHEEL_COUNT dependency wheels successfully"
echo ""

# Create a single uber wheel with all dependencies
print_step "Step 5/5: Creating uber wheel with all dependencies included..."

# Create a temporary directory for the uber wheel
UBER_WHEEL_DIR="$TEMP_WORKING_DIR/uber"
mkdir -p "$UBER_WHEEL_DIR"

# Create the setup.py file with custom install command
cat >"$UBER_WHEEL_DIR/setup.py" <<EOF
from setuptools import setup, find_packages
import setuptools.command.install
import os
import glob
import subprocess
import sys

setup(
    name='${PACKAGE_NAME}',
    version='${PACKAGE_VERSION}',
    description='AWS Glue optimized bundle containing dependencies for ${PACKAGE_NAME}',
    long_description='Optimized wheel bundle for AWS Glue 5.0 containing all dependencies for the ETL boilerplate.',
    author='Data Platform Team',
    author_email='data-platform@example.com',
    packages=['${PACKAGE_NAME}'],
    include_package_data=True,
    package_data={
        '${PACKAGE_NAME}': ['wheels/*.whl'],
    },
    python_requires='>=3.11',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.11',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
EOF

# Create a MANIFEST.in file to include all wheels
cat >"$UBER_WHEEL_DIR/MANIFEST.in" <<EOF
recursive-include ${PACKAGE_NAME}/wheels *.whl
EOF

# Create an __init__.py file that imports all the bundled wheel files
mkdir -p "$UBER_WHEEL_DIR/${PACKAGE_NAME}"
cat >"$UBER_WHEEL_DIR/${PACKAGE_NAME}/__init__.py" <<EOF
"""
${PACKAGE_NAME} - AWS Glue optimized dependencies bundle

This package contains all dependencies for the ETL boilerplate,
optimized for AWS Glue 5.0 (Python 3.11).

Usage:
    # Option 1: Manual installation
    import ${PACKAGE_NAME}
    ${PACKAGE_NAME}.load_wheels()

    # Option 2: Automatic installation (import and install)
    import ${PACKAGE_NAME}.auto

    # Option 3: With custom logging level
    import ${PACKAGE_NAME}
    import logging
    ${PACKAGE_NAME}.load_wheels(log_level=logging.DEBUG)
"""
from pathlib import Path
import logging
import subprocess
import sys

__version__ = "${PACKAGE_VERSION}"
__glue_version__ = "${GLUE_VERSION}"
__python_version__ = "${PYTHON_VERSION}"

def load_wheels(log_level=logging.INFO):
    """
    Install all bundled wheel dependencies for AWS Glue.

    Args:
        log_level: Logging level (default: logging.INFO)

    Returns:
        bool: True if all wheels installed successfully, False otherwise
    """
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[AWS Glue Wheel Installer] %(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(log_level)

    logger.info("Starting AWS Glue wheel installation process")
    logger.info(f"Package: ${PACKAGE_NAME} v${PACKAGE_VERSION}")
    logger.info(f"Target: AWS Glue ${GLUE_VERSION} (Python ${PYTHON_VERSION})")

    package_dir = Path(__file__).parent.absolute()
    wheels_dir = package_dir / "wheels"

    logger.debug(f"Package directory: {package_dir}")
    logger.debug(f"Looking for wheels in: {wheels_dir}")

    if not wheels_dir.exists():
        logger.error(f"Wheels directory not found: {wheels_dir}")
        return False

    wheel_files = list(wheels_dir.glob("*.whl"))
    if not wheel_files:
        logger.warning(f"No wheels found in: {wheels_dir}")
        return False

    logger.info(f"Found {len(wheel_files)} wheels to install")

    wheel_file_paths = [str(wheel_file) for wheel_file in wheel_files]
    logger.debug(f"Wheel files: {wheel_file_paths}")

    try:
        logger.info("Installing bundled dependencies...")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--no-deps", *wheel_file_paths],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info("✓ Successfully installed all bundled wheels")
        logger.debug(f"pip output: {result.stdout}")

        # Verify installation by trying to import key packages
        try:
            import boto3
            import pydantic
            logger.info("✓ Key packages verified successfully")
        except ImportError as e:
            logger.warning(f"Package verification failed: {e}")

    except subprocess.CalledProcessError as e:
        error_msg = "Failed to install wheel files"
        logger.error(f"✗ {error_msg}: {e}")
        if e.stderr:
            logger.error(f"Error details: {e.stderr}")
        return False

    logger.info("All wheels installed successfully for AWS Glue")
    return True

def get_info():
    """Get information about this wheel bundle."""
    return {
        "package_name": "${PACKAGE_NAME}",
        "package_version": "${PACKAGE_VERSION}",
        "glue_version": "${GLUE_VERSION}",
        "python_version": "${PYTHON_VERSION}",
        "wheel_count": ${WHEEL_COUNT}
    }
EOF

# Create auto-install module
cat >"$UBER_WHEEL_DIR/${PACKAGE_NAME}/auto.py" <<EOF
"""
${PACKAGE_NAME}.auto - Automatic wheel installation for AWS Glue

Import this module to automatically install all bundled dependencies:
    import ${PACKAGE_NAME}.auto

This is equivalent to:
    import ${PACKAGE_NAME}
    ${PACKAGE_NAME}.load_wheels()
"""
from ${PACKAGE_NAME} import load_wheels
import logging

# Auto-install on import
if not load_wheels(log_level=logging.INFO):
    raise RuntimeError("Failed to install AWS Glue wheel dependencies")
EOF

# Copy all wheels to the uber wheel directory
mkdir -p "$UBER_WHEEL_DIR/${PACKAGE_NAME}/wheels"
cp "$WHEEL_OUTPUT_DIRECTORY"/*.whl "$UBER_WHEEL_DIR/${PACKAGE_NAME}/wheels/"

# Build the uber wheel
print_info "Building uber wheel package..."

# Install build tools in the current environment
"$VENV_DIR/bin/pip" install build

if ! (cd "$UBER_WHEEL_DIR" && "$VENV_DIR/bin/python" -m build --skip-dependency-check --wheel --outdir .); then
    print_error "Failed to build uber wheel"
    exit 1
fi

# Ensure output directory exists
mkdir -p "$FINAL_WHEEL_OUTPUT_DIRECTORY"

# Copy the uber wheel to the output directory
FINAL_WHEEL_OUTPUT_PATH="$FINAL_WHEEL_OUTPUT_DIRECTORY/$UBER_WHEEL_NAME"

# Find the generated wheel (should be only one in the root directory)
GENERATED_WHEEL=$(find "$UBER_WHEEL_DIR" -maxdepth 1 -name "*.whl" -type f | head -1)

if [ -z "$GENERATED_WHEEL" ]; then
    print_error "No uber wheel was generated"
    exit 1
fi

cp "$GENERATED_WHEEL" "$FINAL_WHEEL_OUTPUT_PATH"

# Get final wheel size for user feedback
WHEEL_SIZE=$(du -h "$FINAL_WHEEL_OUTPUT_PATH" | cut -f1)

print_success "Uber wheel created successfully!"
echo ""

print_header "BUILD COMPLETED SUCCESSFULLY!"
print_success "Final wheel: $FINAL_WHEEL_OUTPUT_PATH"
print_info "Wheel size: $WHEEL_SIZE"
print_info "Dependencies included: $WHEEL_COUNT packages"
print_info "Target: AWS Glue $GLUE_VERSION (Python $PYTHON_VERSION)"
echo ""

print_header "Usage Instructions"
echo ""
print_info "📦 To upload to AWS Glue:"
echo "  1. Upload the wheel to S3:"
echo "     aws s3 cp $FINAL_WHEEL_OUTPUT_PATH s3://your-glue-assets-bucket/wheels/"
echo ""
echo "  2. In your Glue job, add as --additional-python-modules:"
echo "     s3://your-glue-assets-bucket/wheels/$UBER_WHEEL_NAME"
echo ""
print_info "🧪 To test locally:"
echo "  pip install $FINAL_WHEEL_OUTPUT_PATH"
echo ""
print_info "💻 To use in your Glue job code:"
echo "  # Option 1: Manual installation"
echo "  import $PACKAGE_NAME"
echo "  $PACKAGE_NAME.load_wheels()"
echo ""
echo "  # Option 2: Auto-install on import"
echo "  import $PACKAGE_NAME.auto"
echo ""
print_header "Wheel Bundle Complete!"
