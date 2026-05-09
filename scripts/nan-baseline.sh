#!/usr/bin/env bash
set -euo pipefail

# Wrapper for optional NaNLABS baseline commands.
# Exit code contract:
# - 0: command completed or optional command not available.
# - non-zero: command failed and strict mode is enabled.

log() {
  printf '[nan-baseline] %s\n' "$1"
}

run_optional() {
  local cmd="$1"
  shift || true

  if command -v "$cmd" >/dev/null 2>&1; then
    log "running: $cmd $*"
    if "$cmd" "$@"; then
      log "ok: $cmd"
    else
      local rc=$?
      if [ "${NAN_BASELINE_STRICT:-0}" = "1" ]; then
        log "error: $cmd failed with exit code $rc (strict mode)"
        return "$rc"
      fi
      log "warn: $cmd failed with exit code $rc (non-strict mode, continuing)"
    fi
  else
    log "skip: $cmd not found"
  fi
}

usage() {
  cat <<'EOF'
Usage:
  ./scripts/nan-baseline.sh doctor
  ./scripts/nan-baseline.sh update
  ./scripts/nan-baseline.sh skills
  ./scripts/nan-baseline.sh health

Commands:
  doctor  Run nan-doctor if installed.
  update  Run nan-update-check if installed.
  skills  Run nan-skills list if installed.
  health  Run doctor + update in sequence.

Environment:
  NAN_BASELINE_STRICT=1  Fail if an optional command exists but returns non-zero.
EOF
}

main() {
  local action="${1:-}"

  case "$action" in
    doctor)
      run_optional nan-doctor
      ;;
    update)
      run_optional nan-update-check
      ;;
    skills)
      run_optional nan-skills list
      ;;
    health)
      run_optional nan-doctor
      run_optional nan-update-check
      ;;
    *)
      usage
      exit 2
      ;;
  esac
}

main "$@"
