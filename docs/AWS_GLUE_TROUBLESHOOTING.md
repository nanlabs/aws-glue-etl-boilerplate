# AWS Glue Troubleshooting Guide

## Overview

Quick reference for common AWS Glue issues. For detailed runbooks and step-by-step procedures, see **[ClickUp: Troubleshooting Playbook](link-to-clickup)**.

📋 **Complete troubleshooting guide**: [ClickUp: Troubleshooting Playbook](link-to-clickup)

## Quick Triage Checklist

1. Switch to AWS context (unset LocalStack variables, set correct SSO profile)
2. Confirm credentials: `aws sts get-caller-identity`
3. Verify job defaults: `aws glue get-job --job-name <name> | jq '.Job.DefaultArguments'`
4. Inspect latest run in CloudWatch
5. If reproducible locally, fix before redeploying

📋 **Detailed triage procedures**: [ClickUp: Troubleshooting Playbook](link-to-clickup)

## Frequent Symptoms at a Glance

| Symptom | Likely cause | Primary fix |
| --- | --- | --- |
| `ModuleNotFoundError: No module named 'pydantic'` | PyShell job missing third-party dependencies | Add `--additional-python-modules` with pinned versions |
| `ModuleNotFoundError: No module named 'libs'` | `libs.zip` not attached | Attach `--extra-py-files` pointing to the published zip |
| `InternalFailure ... service 'glue'` | LocalStack endpoint still configured | Unset `AWS_ENDPOINT_URL_GLUE`, reset AWS profile |
| `CommandFailedException ... wheel installation failed` | Incompatible wheel | Install from PyPI via `--additional-python-modules` |
| `InvalidSignatureException ... Signature expired` | AWS SSO session expired | Re-run `aws sso login` and retry |
| `Role should not be null or empty` | Job update missing `Role` attribute | Fetch current role and include it in `aws glue update-job` |

## Quick Fixes

### Missing Dependencies

**PyShell**: Add to `--additional-python-modules`
```bash
aws glue update-job --job-name <name> \
  --job-update '{"DefaultArguments": {"--additional-python-modules": "pydantic==2.5.3"}}'
```

**Internal code**: Rebuild `libs.zip` following [PACKAGING.md](./PACKAGING.md)

📋 **Detailed dependency troubleshooting**: [ClickUp: Dependency Troubleshooting](link-to-clickup)

### LocalStack Endpoint Conflict

**Symptom**: `InternalFailure ... service 'glue'` or secrets not found  
**Fix**:
- For local development: Ensure `STAGE=local` and all service endpoints point to LocalStack
- For AWS Glue: Unset all LocalStack endpoints (`AWS_ENDPOINT_URL_*`), reset AWS profile
- Verify secrets are synced: Run `make prepare-localstack` to sync from AWS real to LocalStack

**Verifying Secret Synchronization**:
```bash
# Check if secrets exist in LocalStack
awslocal secretsmanager list-secrets

# Manually sync secrets if needed
make prepare-localstack
```

📋 **Environment switching guide**: [ClickUp: Environment Setup](link-to-clickup)

### AWS SSO Expired

**Symptom**: `InvalidSignatureException ... Signature expired`  
**Fix**: `aws sso login --profile <workload-profile>`

📋 **AWS authentication guide**: [ClickUp: AWS Authentication](link-to-clickup)

## Debugging Workflow

1. **Inspect configuration**: `aws glue get-job --job-name <name>`
2. **Check assets**: Verify job script and `libs.zip` in S3
3. **Review logs**: Check CloudWatch logs for latest run
4. **Reproduce locally**: Test with LocalStack (see [Development Guide](./DEVELOPMENT.md))
5. **Escalate**: Capture run ID, console links, log excerpts

📋 **Complete debugging workflow**: [ClickUp: Debugging Playbook](link-to-clickup)

## Related Documentation

- [Development Guide](./DEVELOPMENT.md) - Local testing and workflow
- [Job Development Guide](./JOB_DEVELOPMENT.md) - Building jobs
- [Deployment Guide](./DEPLOYMENT.md) - Deployment procedures
- [Packaging Reference](./PACKAGING.md) - Building artifacts

## Getting Help

📋 **For detailed troubleshooting**: [ClickUp: Troubleshooting Playbook](link-to-clickup)

If quick fixes don't resolve the issue:
1. Gather context (job name, run ID, logs)
2. Check ClickUp runbooks for detailed procedures
3. Escalate with collected details
