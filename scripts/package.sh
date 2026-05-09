#!/bin/bash
set -e

# Check for required tools
if ! command -v pipenv >/dev/null 2>&1; then
    echo "❌ Error: pipenv is not installed. Please install pipenv and try again." >&2
    exit 1
fi

if ! command -v pip >/dev/null 2>&1; then
    echo "❌ Error: pip is not installed. Please install pip and try again." >&2
    exit 1
fi

# Install wheel if not present
if ! python -c "import wheel" >/dev/null 2>&1; then
    echo "📦 Installing wheel package..."
    pip install wheel
fi

echo "🏗️ Building Glue job artifacts with dual packaging (wheels + ZIP)..."

# Create build directories
mkdir -p build/wheels
mkdir -p build/libs
mkdir -p build/dist
mkdir -p build/temp

# Clean previous builds
rm -rf build/wheels/*
rm -rf build/libs/*
rm -rf build/dist/*
rm -rf build/temp/*

echo "📦 Creating libs wheel package..."

# Build wheel using the setup.py in the repo root
echo "🔧 Building wheel from setup.py..."
python setup.py bdist_wheel --dist-dir build/wheels/

echo "📦 Creating libs ZIP package..."

# Create libs.zip with libs/ folder at root (works for both PySpark and PyShell)
echo "🔧 Building libs.zip (contains libs/ folder)..."
zip -r build/libs.zip libs/ -x "**/__pycache__/*" "**/*.pyc" "**/.*"

# Note: Both PySpark and PyShell jobs need the same structure (libs/ folder at root)
# to support imports like: from libs.base import PyShellJobBase

echo "📦 Creating dependencies wheel bundle..."

# Generate requirements and download wheels for dependencies
echo "📋 Generating requirements.txt..."
pipenv requirements > build/requirements.txt

# Download all dependencies as wheels
echo "📥 Downloading dependency wheels..."
pip download -r build/requirements.txt -d build/wheels/ --prefer-binary

echo "📦 Packaging job scripts..."

# Create jobs.zip for job scripts (these stay as ZIP since they're scripts, not packages)
zip -r build/jobs.zip jobs/ -x "**/__pycache__/*" "**/*.pyc" "**/.*"

# Create individual job packages
echo "📦 Creating individual job packages..."
for job_dir in jobs/*/; do
    if [ -d "$job_dir" ]; then
        job_name=$(basename "$job_dir")
        echo "  - Packaging $job_name jobs..."
        zip -r "build/${job_name}_jobs.zip" "$job_dir" -x "**/__pycache__/*" "**/*.pyc" "**/.*"
    fi
done

# Copy migration files
if [ -d "migrations" ]; then
    echo "📦 Copying migration files..."
    cp -r migrations build/
fi

# Create wheels archive for easy deployment
echo "📦 Creating wheels archive..."
cd build/wheels
tar -czf ../wheels.tar.gz *.whl
cd ../..

# Create a single ZIP with all wheels for AWS Glue convenience
echo "📦 Creating all-wheels ZIP for AWS Glue..."
cd build/wheels
zip -r ../wheels-all.zip *.whl
cd ../..

# Clean up temp directory
rm -rf build/temp

echo "✅ Build completed! Artifacts in build/ directory:"
echo ""
echo "📦 Python Wheels (Modern):"
echo "  - build/wheels/          - Individual wheel files"
echo "  - build/wheels.tar.gz    - Compressed wheels archive"
echo "  - build/wheels-all.zip   - AWS Glue-compatible wheels bundle"
echo ""
echo "📦 ZIP Libraries (Direct Import):"
echo "  - build/libs.zip         - For ALL Glue jobs (PySpark & PyShell)"
echo ""
echo "📜 Job Scripts:"
echo "  - build/jobs.zip         - All job scripts"
echo "  - build/*_jobs.zip       - Individual job layer packages"
echo ""
echo "🗃️ Other:"
echo "  - build/requirements.txt - Dependencies list"
if [ -d "build/migrations" ]; then
    echo "  - build/migrations/      - Database migration files"
fi
echo ""
echo "💡 Note: libs.zip works for both PySpark and PyShell jobs"
echo "   Use with: --extra-py-files s3://bucket/path/libs.zip"

echo ""
echo "🔍 Wheel packages created:"
ls -la build/wheels/
