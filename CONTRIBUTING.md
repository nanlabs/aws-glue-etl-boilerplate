# Contributing

## Workflow

1. Create a feature branch.
2. Implement focused changes.
3. Run tests and checks.
4. Open a pull request with clear scope and validation notes.

## NaNLABS Local Baseline (Recommended)

When available in your workstation, run these checks before opening a PR:

```bash
nan-doctor
nan-skills list
nan-update-check
```

If `nan-*` commands are not installed, continue with the project checks (`make lint`, `make type-check`, tests).

## Validation Evidence

In each PR, include the commands you executed and a short result summary (pass/fail + key notes).

## Conventions

- Keep shared code source-agnostic.
- Use environment variables / Glue args for runtime configuration.
- Avoid introducing organization-specific identifiers into boilerplate defaults.
