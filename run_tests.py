#!/usr/bin/env python3
"""
Test runner for the AWS Glue Data Lake Jobs project.

This script provides a comprehensive test runner with different test categories
and reporting options.
"""

import argparse
import subprocess
import sys


def run_command(cmd, description=""):
    """Run a command and handle errors."""
    print(f"\n{'=' * 60}")
    print(f"Running: {description or ' '.join(cmd)}")
    print(f"{'=' * 60}")

    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Run tests for AWS Glue Data Lake Jobs"
    )
    parser.add_argument(
        "--type",
        choices=["unit", "integration", "all"],
        default="all",
        help="Type of tests to run (default: all)",
    )
    parser.add_argument(
        "--coverage", action="store_true", help="Run tests with coverage reporting"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Run tests with verbose output"
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Run tests in parallel (requires pytest-xdist)",
    )
    parser.add_argument(
        "--marker", help="Run tests with specific marker (e.g., spark, aws, validation)"
    )
    parser.add_argument("--file", help="Run specific test file")

    args = parser.parse_args()

    # Base pytest command
    cmd = ["python", "-m", "pytest"]

    # Add verbosity
    if args.verbose:
        cmd.extend(["-v", "--tb=long"])

    # Add coverage
    if args.coverage:
        cmd.extend(
            ["--cov=libs", "--cov=jobs", "--cov-report=html", "--cov-report=term"]
        )

    # Add parallel execution
    if args.parallel:
        cmd.extend(["-n", "auto"])

    # Add specific marker
    if args.marker:
        cmd.extend(["-m", args.marker])

    # Add test type
    if args.type == "unit":
        cmd.append("tests/unit/")
    elif args.type == "integration":
        cmd.extend(["--with-integration", "tests/integration/"])
    elif args.file:
        cmd.append(args.file)
    else:
        cmd.append("tests/")

    # Run the tests
    success = run_command(cmd, f"Running {args.type} tests")

    if success:
        print(f"\n✅ {args.type.title()} tests completed successfully!")
        if args.coverage:
            print("📊 Coverage report generated in htmlcov/")
    else:
        print(f"\n❌ {args.type.title()} tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
