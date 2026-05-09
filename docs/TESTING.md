# Testing

## Validation matrix

Required project checks:

```bash
make lint
make type-check
make test-unit
```

Recommended NaNLABS baseline checks (optional):

```bash
make check-env
make nan-health
```

When opening a PR, include the command list executed and a one-line result per command.

## Unit tests

Run:

```bash
make test-unit
```

## Integration tests

Run:

```bash
make test-integration
```

Current integration suite includes a smoke E2E flow for `public_api`.
Some future integration tests may require LocalStack and Spark runtime.

## Quality checks

Run:

```bash
make lint
make type-check
```

## Notes

- Keep tests source-agnostic when possible.
- Add source-specific tests under `tests/unit/jobs/` only for actively supported sample sources.
