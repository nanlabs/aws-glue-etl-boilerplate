# AI Agents Guidelines

## Overview

This document outlines the operational principles, execution protocols, and guardrails for AI agents working within the NaNLABS Data Warehouse platform. These guidelines ensure consistent, secure, and effective AI assistance while maintaining code quality and project standards.

## Core Principles

### 1. Documentation-First Approach

- **MANDATORY**: Always reference the `docs/` directory before implementation.
- Start with `docs/README.md` for complete documentation index
- Follow established patterns and guidelines documented in `docs/BEST_PRACTICES.md`


### 2. Security and Confidentiality

- **Data Protection**: Never expose sensitive data, credentials, or proprietary information
- **Secrets Management**: Use AWS Secrets Manager for all credential access
- **Access Control**: Follow least privilege principles for all operations
- **Audit Trail**: Maintain clear logs of all AI-assisted changes

### 3. Code Quality Standards

- **Type Safety**: Use Pydantic for parameter validation and type hints
- **Error Handling**: Implement robust error handling with centralized logging
- **Testing**: Include unit tests (`tests/unit/`) and integration tests (`tests/integration/`)
- **Documentation**: Maintain clear docstrings and update relevant documentation

## Execution Protocols

### 1. Pre-Implementation Checklist

Before starting any task, AI agents must:

1. **Review Documentation**: Check `docs/README.md` and relevant documentation files
2. **Understand Context**: Identify the Medallion Architecture layer (Bronze/Silver/Gold)
3. **Check Dependencies**: Verify required libraries and configurations
4. **Assess Impact**: Consider downstream effects of changes
5. **Plan Testing**: Identify test cases and validation requirements

### 2. Implementation Guidelines

#### Code Structure
- Follow the established repository structure
- Use existing base classes and utilities from `libs/`
- Implement proper error handling and logging
- Maintain consistency with existing code patterns

#### Data Processing
- **Raw Layer**: Raw data, unaltered for audit purposes. Uses pyshell
- **Bronze Layer**: Raw data ingestion with minimal processing
- **Silver Layer**: Cleaned, validated, and enriched data
- **Gold Layer**: Business-ready, aggregated analytics tables

#### Configuration Management
- **Reference**: See [Configuration System](docs/README.md#configuration-system) for complete details
- Use declarative configuration with explicit parameter resolution
- Never hardcode sensitive information
- Follow the configuration patterns in `libs/common/config/`

### 3. Quality Assurance

#### Code Review Standards
- Ensure all code follows PEP 8 style guidelines
- Verify proper error handling and logging
- Check for security vulnerabilities
- Validate test coverage requirements

#### Testing Requirements
- Unit tests for all business logic
- Integration tests for data pipeline components
- Validation tests for data quality checks
- Performance tests for critical operations

## Guardrails and Limitations

### 1. Security Restrictions

- **No Hardcoded Credentials**: Never include passwords, API keys, or tokens in code
- **No Data Exposure**: Never log or output sensitive data
- **No Unauthorized Access**: Only use approved AWS services and permissions
- **No Data Modification**: Never modify production data without proper authorization

### 2. Operational Boundaries

- **Documentation Only**: All documentation must be in the `docs/` directory
- **No External Dependencies**: Avoid introducing new external dependencies without approval
- **No Breaking Changes**: Ensure backward compatibility for existing functionality
- **No Production Changes**: Never make direct changes to production systems

### 3. Data Handling

- **Encryption**: Use encryption for all sensitive data at rest and in transit
- **Retention**: Follow data retention policies and logging requirements
- **Quality**: Implement data quality checks and validation
- **Lineage**: Maintain data lineage and audit trails

## Error Handling and Recovery

### 1. Error Classification

- **Validation Errors**: Input data validation failures
- **Processing Errors**: Data transformation or processing failures
- **System Errors**: Infrastructure or service failures
- **Security Errors**: Authentication or authorization failures

### 2. Recovery Strategies

- **Graceful Degradation**: Implement fallback mechanisms
- **Retry Logic**: Use exponential backoff for transient failures
- **Circuit Breakers**: Prevent cascading failures
- **Monitoring**: Set up alerts for critical failures

## Monitoring and Observability

### 1. Logging Standards

- Use structured logging with consistent formats
- Include relevant context and correlation IDs
- Set appropriate log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Never log sensitive information

### 2. Metrics and Alerts

- Track key performance indicators
- Monitor data quality metrics
- Set up alerts for critical failures
- Regular performance reviews

## Best Practices

### 1. Development Workflow

- Start with documentation review
- Follow the established development patterns
- Implement comprehensive testing
- Update documentation as needed

### 2. Code Organization

- Use modular design with reusable components
- Follow the Medallion Architecture principles
- Implement proper abstraction layers
- Maintain clear separation of concerns
- Never create a new file without checking if that is already implemented or created.

### 3. Documentation Maintenance

- Keep documentation up to date
- Use clear and concise language
- Include examples and use cases
- Regular documentation reviews
- Ask before creating new docs
- Never create documentation outside the `docs/` directory
- Do not include large code examples, only snippets if really needed.

## Escalation Procedures

### 1. When to Escalate

- Security incidents or data breaches
- Production system failures
- Compliance violations
- Resource constraints or performance issues

### 2. Escalation Process

1. **Immediate Response**: Stop any potentially harmful operations
2. **Assessment**: Evaluate the scope and impact
3. **Notification**: Alert relevant stakeholders
4. **Documentation**: Record the incident and response
5. **Resolution**: Implement appropriate fixes
6. **Review**: Conduct post-incident analysis

## References

- [Development Guide](docs/DEVELOPMENT.md) - Development environment and workflow
- [Best Practices](docs/BEST_PRACTICES.md) - Coding standards and guidelines
- [Architecture](docs/ARCHITECTURE.md) - System architecture and design
- [Testing Guidelines](docs/TESTING.md) - Testing requirements and patterns
- [Operations Guide](docs/OPERATIONS.md) - Operational procedures and troubleshooting

## Contact and Support

For questions or issues related to AI agent operations:

- **Technical Issues**: Check the [Operations Guide](docs/OPERATIONS.md)
- **Security Concerns**: Follow escalation procedures immediately
- **Process Questions**: Review this document and related documentation
- **Emergency**: Use established emergency contact procedures

---

**Last Updated**: [Current Date]
**Version**: 1.0
**Review Cycle**: Quarterly
