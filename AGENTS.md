# AI Agents Guidelines

## Principles

- Keep implementations generic and reusable.
- Avoid source/vendor-specific hardcoding in shared modules.
- Prefer configuration via environment variables or Glue job arguments.
- Follow Medallion separation (`raw`, `bronze`, `silver`, `gold`).

## Security

- Never hardcode credentials or tokens.
- Do not expose sensitive values in logs.

## Quality

- Keep changes minimal and focused.
- Update tests/docs when behavior changes.
