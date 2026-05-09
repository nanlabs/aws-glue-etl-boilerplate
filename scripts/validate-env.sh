#!/usr/bin/env bash

# 🔍 Dev Container Environment Validation Script for AWS Glue Data Lake Jobs
# This script validates that the dev container environment is properly configured

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print functions
print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
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

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if service is accessible (for services within container network)
check_internal_service() {
    local port="$1"
    local endpoint="$2"
    local description="$3"

    if curl -s -m 5 "$endpoint" >/dev/null 2>&1; then
        print_success "$description is running and accessible internally"
        return 0
    else
        print_warning "$description may not be ready yet (internal: $endpoint)"
        return 1
    fi
}

# Check if service is accessible from host (for user access)
check_host_service() {
    local port="$1"
    local description="$2"

    if curl -s -m 5 -o /dev/null "http://localhost:$port" 2>/dev/null; then
        print_success "$description is accessible from host on port $port"
        return 0
    else
        print_info "$description may not be accessible from host on port $port (this is normal if services are starting)"
        return 0  # Return 0 instead of 1 because this is not a critical failure
    fi
}

# Main validation function
main() {
    print_header "AWS Glue Data Lake Jobs - Dev Container Environment Validation"

    local all_checks_passed=true

    # Check if we're in a dev container
    print_header "Environment Detection"

    if [[ "${REMOTE_CONTAINERS:-}" == "true" ]] || [[ "${CODESPACES:-}" == "true" ]] || [[ -f "/.dockerenv" ]]; then
        print_success "Running in dev container environment"
    else
        print_warning "Not detected as dev container environment"
        print_info "Expected to run inside VS Code Dev Container"
    fi

    # Check essential development tools
    print_header "Development Tools"

    if command_exists python3; then
        print_success "Python 3 is available"
        python3 --version
    else
        print_error "Python 3 is not available"
        all_checks_passed=false
    fi

    if command_exists pip; then
        print_success "pip is available"
    else
        print_error "pip is not available"
        all_checks_passed=false
    fi

    if command_exists make; then
        print_success "Make is available"
    else
        print_warning "Make is not available (optional but recommended)"
    fi

    if command_exists aws; then
        print_success "AWS CLI is available"
        aws --version
    else
        print_error "AWS CLI is not available"
        all_checks_passed=false
    fi

    if command_exists curl; then
        print_success "curl is available"
    else
        print_error "curl is not available"
        all_checks_passed=false
    fi

    if command_exists git; then
        print_success "Git is available"
        git --version
    else
        print_error "Git is not available"
        all_checks_passed=false
    fi

    # Check environment files
    print_header "Environment Configuration"

    if [ -f ".env" ]; then
        print_success ".env file exists"
    else
        print_error ".env file is missing"
        print_info "Run: cp .env.example .env"
        all_checks_passed=false
    fi

    if [ -f ".env.example" ]; then
        print_success ".env.example template exists"
    else
        print_warning ".env.example template is missing"
    fi

    if [ -f ".envrc.example" ]; then
        print_success ".envrc.example template exists"
    else
        print_warning ".envrc.example template is missing"
    fi

    # Check dev container configuration
    print_header "Dev Container Configuration"

    if [ -f ".devcontainer/devcontainer.json" ]; then
        print_success "devcontainer.json exists"
    else
        print_error "devcontainer.json is missing"
        all_checks_passed=false
    fi

    if [ -f ".devcontainer/compose.yml" ]; then
        print_success "Dev container compose.yml exists"
    else
        print_error "Dev container compose.yml is missing"
        all_checks_passed=false
    fi

    # Check project structure
    print_header "Project Structure"

    local required_dirs=("jobs" "libs" "tests" "docs" "scripts")
    for dir in "${required_dirs[@]}"; do
        if [ -d "$dir" ]; then
            print_success "$dir/ directory exists"
        else
            print_error "$dir/ directory is missing"
            all_checks_passed=false
        fi
    done

    # Check service connectivity (internal container network)
    print_header "Service Connectivity (Internal)"

    # Check LocalStack internal connectivity
    check_internal_service "4566" "http://localstack:4566/_localstack/health" "LocalStack"

    # Check service endpoints from host (for user access)
    print_header "Service Endpoints (Host Access)"

    print_info "These services should be accessible from your host browser:"
    echo "  - LocalStack: http://localhost:4566"
    echo "  - Jupyter Lab: http://localhost:8888"
    echo ""

    # Check host accessibility (optional - may fail if services are starting)
    check_host_service "4566" "LocalStack"
    check_host_service "8888" "Jupyter Lab"

    # Environment variables check
    print_header "Environment Variables"

    if [ -n "${AWS_REGION:-}" ]; then
        print_success "AWS_REGION is set to: ${AWS_REGION}"
    else
        print_warning "AWS_REGION is not set"
    fi

    if [ -n "${ENVIRONMENT:-}" ]; then
        print_success "ENVIRONMENT is set to: ${ENVIRONMENT}"
    else
        print_warning "ENVIRONMENT is not set"
    fi

    # Python packages check
    print_header "Python Environment"

    if python3 -c "import pyspark" 2>/dev/null; then
        print_success "PySpark is available"
    else
        print_warning "PySpark is not available"
    fi

    if python3 -c "import boto3" 2>/dev/null; then
        print_success "boto3 is available"
    else
        print_warning "boto3 is not available"
    fi

    # Summary
    print_header "Validation Summary"

    if [ "$all_checks_passed" = true ]; then
        print_success "All critical checks passed! ✨"
        print_info "Your dev container environment is ready for development."
        echo ""
        print_info "Next steps:"
        echo "  1. Access Jupyter Lab: http://localhost:8888"
        echo "  2. Run tests: make test"
        echo "  3. Check services: make services-status"
        echo ""
    else
        print_error "Some checks failed!"
        print_info "Please review the errors above and fix them before proceeding."
        echo ""
        print_info "For help:"
        echo "  - Check docs/DEV_SETUP.md"
        echo "  - Ensure you're in VS Code Dev Container"
        echo "  - Services may need time to start up"
        echo ""
        exit 1
    fi
}

# Run main function
main "$@"
