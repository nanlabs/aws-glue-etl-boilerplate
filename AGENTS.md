# AI Agents Guidelines

## Instruction Priority

When instructions conflict, follow this order:

1. `AGENTS.md` (this file)
2. `CONTRIBUTING.md`
3. `README.md` and `docs/`

Escalate to a human when guidance is ambiguous or potentially destructive.

## Principles

- Keep implementations generic and reusable.
- Avoid source/vendor-specific hardcoding in shared modules.
- Prefer configuration via environment variables or Glue job arguments.
- Follow Medallion separation (`raw`, `bronze`, `silver`, `gold`).

## Automation Boundaries

- Prefer minimal and reversible edits.
- Do not run destructive operations on cloud resources without explicit confirmation.
- Do not modify deployment/release workflows beyond the task scope.
- Keep boilerplate defaults vendor-neutral unless the change explicitly targets this repo.

## Security

- Never hardcode credentials or tokens.
- Do not expose sensitive values in logs.

## Quality

- Keep changes minimal and focused.
- Update tests/docs when behavior changes.
- Include explicit validation evidence for the commands executed.

## NaNLABS Baseline (Recommended)

If available in the environment, use:

- `nan-doctor` to validate workstation health
- `nan-skills list` to discover available skills and helpers
- `nan-update-check` to verify baseline/tool updates

These commands are recommended but optional for external contributors.
