# Operations Guide

## Overview

This document provides quick reference for operational procedures. For detailed runbooks and step-by-step procedures, see **[ClickUp: Operations Playbook](link-to-clickup)**.

📋 **Complete operations guide**: [ClickUp: Operations Playbook](link-to-clickup)

## Quick Reference

### Job Configuration

Use the tooling in `scripts/` or Terraform modules in the infrastructure repo to provision Glue jobs. For parameter defaults, catalogs, and Iceberg setup, follow the **[Deployment Guide](./DEPLOYMENT.md)** and the job-specific instructions in `docs/sources/`.

### Monitoring

**CloudWatch Logs**:
- PyShell jobs: `/aws-glue/python-jobs/{output,error}`
- Spark jobs: `/aws-glue/jobs/`

**CloudWatch Metrics**: Duration, processed records, error rate, resource usage

**Spark UI**: Available from the job run page in AWS Console

📋 **Monitoring dashboards and detailed procedures**: [ClickUp: Monitoring & Observability](link-to-clickup)

### Troubleshooting

Quick reference: **[AWS_GLUE_TROUBLESHOOTING.md](./AWS_GLUE_TROUBLESHOOTING.md)**

📋 **Complete troubleshooting runbooks**: [ClickUp: Troubleshooting Playbook](link-to-clickup)

### Performance Optimization

Performance tuning patterns (partitioning, worker sizing, caching) are documented in **[BEST_PRACTICES.md](./BEST_PRACTICES.md)**.

📋 **Detailed performance tuning procedures**: [ClickUp: Performance Optimization](link-to-clickup)

### Maintenance & Backups

📋 **Maintenance procedures**: [ClickUp: Maintenance & Backups](link-to-clickup)

### Security

Security posture (IAM, secrets, network controls) is tracked in **[SECRETS_MANAGEMENT.md](./SECRETS_MANAGEMENT.md)**.

📋 **Security procedures**: [ClickUp: Security Best Practices](link-to-clickup)

### Incident Response

📋 **Incident response runbooks**: [ClickUp: Incident Response](link-to-clickup)

### AWS Profiles & Access

Environment-specific profile setup: **[DEVELOPMENT.md](./DEVELOPMENT.md)**

📋 **AWS access procedures**: [ClickUp: AWS Access & Authentication](link-to-clickup)

## Related Documentation

- [Best Practices](./BEST_PRACTICES.md)
- [Development Guide](./DEVELOPMENT.md)
- [Testing Guidelines](./TESTING.md)
- [Deployment Guide](./DEPLOYMENT.md)
