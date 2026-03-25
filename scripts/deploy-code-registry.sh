#!/bin/bash
set -e

# 🚀 Code Registry Deployment Script
# Deploys code artifacts to the Code Registry S3 bucket using AWS profile (local) or OIDC (GitHub Actions)
# Based on .github/workflows/deploy-code-registry.yml

# Detect if running in GitHub Actions
IS_GITHUB_ACTIONS="${GITHUB_ACTIONS:-false}"

# Default values
AWS_PROFILE="${AWS_PROFILE:-nan-infra-platform-tools-terraform-execution}"
AWS_REGION="${AWS_REGION:-us-west-2}"
CODE_REGISTRY_BUCKET="${CODE_REGISTRY_BUCKET:-nan-infra-platform-tools-glue-code-artifacts}"
TARGET="${TARGET:-latest}"

# In GitHub Actions, use environment variables if available
if [ "$IS_GITHUB_ACTIONS" = "true" ]; then
    # Override with GitHub Actions environment variables if set
    AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-west-2}}"
    # Don't use AWS_PROFILE in GitHub Actions (uses OIDC instead)
    USE_AWS_PROFILE=false
else
    USE_AWS_PROFILE=true
fi

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
    echo "Deploy code artifacts to Code Registry S3 bucket"
    echo ""
    echo "Options:"
    echo "  -p, --profile PROFILE    AWS profile to use (default: nan-infra-platform-tools-terraform-execution)"
    echo "  -r, --region REGION      AWS region (default: us-west-2)"
    echo "  -b, --bucket BUCKET      S3 bucket name (default: nan-infra-platform-tools-glue-code-artifacts)"
    echo "  -t, --target TARGET      Deployment target: 'latest' or version tag like 'v1.2.3' (default: latest)"
    echo "  -n, --no-build           Skip building packages (assumes build/ directory exists)"
    echo "  -h, --help               Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  AWS_PROFILE              AWS profile to use"
    echo "  AWS_REGION               AWS region"
    echo "  CODE_REGISTRY_BUCKET     S3 bucket name"
    echo "  TARGET                   Deployment target"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Deploy latest with defaults"
    echo "  $0 -t v1.2.3                        # Deploy version v1.2.3"
    echo "  $0 -p my-profile -t latest          # Use custom profile"
    echo "  $0 -n                                # Skip build, deploy existing artifacts"
    echo ""
}

# Parse command line arguments
SKIP_BUILD=false
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
    -p | --profile)
        AWS_PROFILE="$2"
        shift 2
        ;;
    -r | --region)
        AWS_REGION="$2"
        shift 2
        ;;
    -b | --bucket)
        CODE_REGISTRY_BUCKET="$2"
        shift 2
        ;;
    -t | --target)
        TARGET="$2"
        shift 2
        ;;
    -n | --no-build)
        SKIP_BUILD=true
        shift
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

print_header "Code Registry Deployment"
if [ "$USE_AWS_PROFILE" = "true" ]; then
    print_info "AWS Profile: $AWS_PROFILE"
else
    print_info "Authentication: GitHub Actions OIDC"
fi
print_info "AWS Region: $AWS_REGION"
print_info "Bucket: $CODE_REGISTRY_BUCKET"
print_info "Target: $TARGET"
echo ""

# Verify AWS CLI is installed
if ! command -v aws >/dev/null 2>&1; then
    print_error "AWS CLI is not installed. Please install it and try again."
    exit 1
fi

# Build AWS CLI base command (with or without profile)
if [ "$USE_AWS_PROFILE" = "true" ]; then
    AWS_CMD_BASE="aws --profile $AWS_PROFILE --region $AWS_REGION"

    # Verify AWS profile exists
    if ! aws configure list-profiles | grep -q "^${AWS_PROFILE}$"; then
        print_error "AWS profile '$AWS_PROFILE' not found."
        print_info "Available profiles:"
        aws configure list-profiles
        exit 1
    fi
else
    AWS_CMD_BASE="aws --region $AWS_REGION"
fi

# Verify AWS credentials
print_step "Verifying AWS credentials..."
if ! $AWS_CMD_BASE sts get-caller-identity >/dev/null 2>&1; then
    if [ "$USE_AWS_PROFILE" = "true" ]; then
        print_error "Failed to verify AWS credentials for profile '$AWS_PROFILE'"
        print_info "Please ensure your AWS credentials are configured correctly."
    else
        print_error "Failed to verify AWS credentials"
        print_info "Please ensure AWS credentials are configured via OIDC or environment variables."
    fi
    exit 1
fi

CALLER_IDENTITY=$($AWS_CMD_BASE sts get-caller-identity --output json)
ACCOUNT_ID=$(echo "$CALLER_IDENTITY" | grep -o '"Account": "[^"]*' | cut -d'"' -f4)
print_success "Authenticated as: $(echo "$CALLER_IDENTITY" | grep -o '"Arn": "[^"]*' | cut -d'"' -f4)"
print_info "Account ID: $ACCOUNT_ID"
echo ""

# Verify bucket exists and is accessible
print_step "Verifying S3 bucket access..."
if ! $AWS_CMD_BASE s3 ls "s3://${CODE_REGISTRY_BUCKET}" >/dev/null 2>&1; then
    print_error "Cannot access bucket 's3://${CODE_REGISTRY_BUCKET}'"
    print_info "Please verify the bucket exists and you have permissions to access it."
    exit 1
fi
print_success "Bucket access verified"
echo ""

# Build packages if not skipped
if [ "$SKIP_BUILD" = false ]; then
    print_step "Building packages..."

    # Check if Makefile exists
    if [ ! -f "Makefile" ]; then
        print_error "Makefile not found. Please run this script from the project root."
        exit 1
    fi

    # Check if pipenv is available
    if ! command -v pipenv >/dev/null 2>&1; then
        print_error "pipenv is not installed. Please install it and try again."
        exit 1
    fi

    print_info "Running: make package"
    make package

    print_info "Running: make glue-wheels"
    make glue-wheels

    print_success "Packages built successfully"
    echo ""
else
    print_warning "Skipping build step (using existing build/ directory)"

    # Verify build directory exists
    if [ ! -d "build" ]; then
        print_error "build/ directory not found. Please run 'make package' and 'make glue-wheels' first."
        exit 1
    fi

    echo ""
fi

# Verify required build artifacts exist
print_step "Verifying build artifacts..."
REQUIRED_FILES=(
    "build/wheels.tar.gz"
    "build/wheels-all.zip"
    "build/requirements.txt"
    "build/libs.zip"
)

MISSING_FILES=()
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        MISSING_FILES+=("$file")
    fi
done

if [ ${#MISSING_FILES[@]} -gt 0 ]; then
    print_error "Missing required build artifacts:"
    for file in "${MISSING_FILES[@]}"; do
        echo "  - $file"
    done
    print_info "Please run 'make package' to build the artifacts."
    exit 1
fi

# Check for wheels directory
if [ ! -d "build/wheels" ] || [ -z "$(ls -A build/wheels/*.whl 2>/dev/null)" ]; then
    print_error "No wheel files found in build/wheels/"
    print_info "Please run 'make package' to build the wheels."
    exit 1
fi

print_success "All required artifacts found"
echo ""

# Deploy based on target
print_step "Deploying to Code Registry (target: $TARGET)..."

if [ "$TARGET" = "latest" ]; then
    print_info "Deploying latest version..."

    # Deploy wheels archives
    $AWS_CMD_BASE s3 cp build/wheels.tar.gz "s3://${CODE_REGISTRY_BUCKET}/wheels/libs-wheels-latest.tar.gz"
    print_success "Uploaded wheels.tar.gz"

    $AWS_CMD_BASE s3 cp build/wheels-all.zip "s3://${CODE_REGISTRY_BUCKET}/wheels/libs-wheels-latest.zip"
    print_success "Uploaded wheels-all.zip"

    # Sync wheels directory
    $AWS_CMD_BASE s3 sync build/wheels/ "s3://${CODE_REGISTRY_BUCKET}/wheels/latest/" --delete
    print_success "Synced wheels directory"

    # Deploy requirements
    $AWS_CMD_BASE s3 cp build/requirements.txt "s3://${CODE_REGISTRY_BUCKET}/dependencies/requirements-latest.txt"
    print_success "Uploaded requirements.txt"

    # Deploy AWS Glue optimized uber wheel (if exists)
    GLUE_WHEEL="build/glue-wheels/aws-glue-etl-boilerplate-libs-1.0.0-py3-none-any.whl"
    if [ -f "$GLUE_WHEEL" ]; then
        $AWS_CMD_BASE s3 cp "$GLUE_WHEEL" "s3://${CODE_REGISTRY_BUCKET}/wheels/aws-glue-etl-boilerplate-libs-latest.whl"
        print_success "Uploaded AWS Glue uber wheel"
    else
        print_warning "AWS Glue uber wheel not found (skipping)"
    fi

    # Deploy libs.zip
    $AWS_CMD_BASE s3 cp build/libs.zip "s3://${CODE_REGISTRY_BUCKET}/libs/libs-latest.zip"
    print_success "Uploaded libs.zip"

    # Deploy job scripts
    if [ -d "jobs" ]; then
        $AWS_CMD_BASE s3 sync jobs/ "s3://${CODE_REGISTRY_BUCKET}/scripts/latest/jobs/" --delete
        print_success "Synced job scripts"
    else
        print_warning "jobs/ directory not found (skipping)"
    fi

else
    print_info "Deploying version $TARGET..."

    # Deploy wheels archives
    $AWS_CMD_BASE s3 cp build/wheels.tar.gz "s3://${CODE_REGISTRY_BUCKET}/wheels/libs-wheels-${TARGET}.tar.gz"
    print_success "Uploaded wheels.tar.gz"

    $AWS_CMD_BASE s3 cp build/wheels-all.zip "s3://${CODE_REGISTRY_BUCKET}/wheels/libs-wheels-${TARGET}.zip"
    print_success "Uploaded wheels-all.zip"

    # Sync wheels directory
    $AWS_CMD_BASE s3 sync build/wheels/ "s3://${CODE_REGISTRY_BUCKET}/wheels/${TARGET}/" --delete
    print_success "Synced wheels directory"

    # Deploy requirements
    $AWS_CMD_BASE s3 cp build/requirements.txt "s3://${CODE_REGISTRY_BUCKET}/dependencies/requirements-${TARGET}.txt"
    print_success "Uploaded requirements.txt"

    # Deploy AWS Glue optimized uber wheel (if exists)
    GLUE_WHEEL="build/glue-wheels/aws-glue-etl-boilerplate-libs-1.0.0-py3-none-any.whl"
    if [ -f "$GLUE_WHEEL" ]; then
        $AWS_CMD_BASE s3 cp "$GLUE_WHEEL" "s3://${CODE_REGISTRY_BUCKET}/wheels/aws-glue-etl-boilerplate-libs-${TARGET}.whl"
        print_success "Uploaded AWS Glue uber wheel"
    else
        print_warning "AWS Glue uber wheel not found (skipping)"
    fi

    # Deploy libs.zip
    $AWS_CMD_BASE s3 cp build/libs.zip "s3://${CODE_REGISTRY_BUCKET}/libs/libs-${TARGET}.zip"
    print_success "Uploaded libs.zip"

    # Deploy job scripts
    if [ -d "jobs" ]; then
        $AWS_CMD_BASE s3 sync jobs/ "s3://${CODE_REGISTRY_BUCKET}/scripts/${TARGET}/jobs/"
        print_success "Synced job scripts"
    else
        print_warning "jobs/ directory not found (skipping)"
    fi
fi

echo ""
print_header "DEPLOYMENT COMPLETED SUCCESSFULLY!"
print_success "Target: $TARGET"
print_success "Bucket: s3://${CODE_REGISTRY_BUCKET}"
print_info "Deployment artifacts are now available in the Code Registry"
echo ""
