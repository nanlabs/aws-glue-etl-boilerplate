# Contributing

This is a private data warehouse repository. All contributions must respect confidentiality, least privilege, and traceability of every change.

Humans: this file is for you. AI assistants operate under AGENTS.md; do not migrate AI guardrails here.

## Source of Truth Documents (Do Not Duplicate)

Always reference, never restate. Update the doc if it is stale:

- [docs/DEVELOPMENT.md](./docs/DEVELOPMENT.md) – local environment setup
- [docs/DEPLOYMENT.md](./docs/DEPLOYMENT.md) – plan/apply & promotion flow
- [docs/BEST_PRACTICES.md](./docs/BEST_PRACTICES.md) – security & quality gates
- [docs/CLICKUP_WORKFLOW.md](./docs/CLICKUP_WORKFLOW.md) – work tracking & ticket conventions
- [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) – system architecture and design
- [docs/SECRETS_MANAGEMENT.md](./docs/SECRETS_MANAGEMENT.md) – credential access patterns
- [docs/MEDALLION_ARCHITECTURE.md](./docs/MEDALLION_ARCHITECTURE.md) – data lake architecture patterns

If something conflicts, the file in docs/ wins. Open a PR to reconcile.

## Work Tracking & Ticket Conventions

All non-trivial work maps to a ClickUp task ID: DWH-###.

For end-to-end workspace structure, ticket naming patterns, escalation triggers, and anti-patterns, refer to [docs/CLICKUP_WORKFLOW.md](./docs/CLICKUP_WORKFLOW.md) (kept concise—update that doc rather than expanding this section).

**Required:**
- PR title: DWH-123: concise description
- Commit messages reference the ticket (subject or body)
- One PR per atomic ticket whenever possible; if batching, enumerate all IDs
- No opportunistic refactors inside functional change PRs—open a separate ticket

## Task Lifecycle / DoR / DoD (Summary)

Full details live in the internal ClickUp document: "Data Warehouse – Task Lifecycle" (includes subpages: Definition of Ready, Definition of Done, Workflow States). Do not recreate the full content here.

**Definition of Ready (excerpted checklist):**
- Clear objective (business or infra outcome)
- Acceptance criteria testable & unambiguous
- Security / compliance impact noted (or explicitly N/A)
- Dependencies identified & unblocked
- Rollback / mitigation path identified for risky changes
- Architecture / design link (if complexity > trivial)

**Definition of Done (excerpted checklist):**
- Code merged to main
- Security posture unchanged or approved deltas documented
- Documentation updated (or explicitly N/A)
- Validation performed (e.g. sample query, policy diff, module output)
- Monitoring / tagging / encryption present & consistent
- No unresolved TODO comments

## Branching & Git Hygiene

PR description should include (trim if N/A):
- Ticket link(s) & summary
- Security-impact notes (IAM, KMS, S3 policies, networking) or "No security impact"
- Validation steps & evidence (concise: what you ran, expected vs observed)
- Docs updated or justification for omission
- Rollback strategy if risk > low
- Any scope limitations / postponed follow-ups

Attach only redacted logs / screenshots (never secrets, account IDs may be partially masked when externalized).

## Confidentiality & Data Handling

- Do not paste raw ARNs, account IDs, or policy JSON into external tools without approval
- No secrets / credentials in code, commit messages, or PR descriptions
- Use AWS managed services (Secrets Manager / SSM Parameter Store) per [docs/SECRETS_MANAGEMENT.md](./docs/SECRETS_MANAGEMENT.md)
- All storage encrypted (KMS) & logging (CloudTrail, Config) preserved—never remove without an approved ticket

## Data Processing Expectations

Use established patterns from [docs/MEDALLION_ARCHITECTURE.md](./docs/MEDALLION_ARCHITECTURE.md):

- **Bronze Layer**: Raw data ingestion with minimal processing
- **Silver Layer**: Cleaned, validated, and enriched data
- **Gold Layer**: Business-ready, aggregated analytics tables

Limit scope—avoid broad changes unless performing coordinated data architecture review.

## Module Authoring

Before creating a new module:

- Check for existing equivalent in `libs/` or vendor modules
- Add concise README (inputs / outputs / example) + note any assumptions
- Pin provider + external module versions (no floating main)
- Keep variable semantics explicit; avoid multi-mode booleans

## Commit Messages

Adopt a conventional, descriptive style:

```
feat(DWH-321): add glue crawler for new dataset
fix(DWH-222): scope KMS key policy principal
chore(DWH-405): refactor shared tagging locals
```

Body (optional): rationale, validation summary, risk & rollback.

## Reviews & Approvals

Minimum: 1 reviewer; 2 reviewers for security-impacting changes (IAM expansion, cross-account roles, network exposure, encryption policy changes).

**Reviewer checklist:**
- No unmanaged destroys
- Tagging & naming conform to strategy
- Encryption / logging unchanged or explicitly justified
- IAM: no broad wildcards unless tightly condition-scoped
- Module boundaries respected (no copy-paste drift)

## Using AI Assistants

AI operational guardrails live in [AGENTS.md](./AGENTS.md)
- AI-generated changes still require human validation of security-sensitive resources
- Reject AI-suggested blast radius expansion without matching ticket scope

## Bug Reports & Enhancements (Private Context)

Prefer ClickUp tasks over GitHub Issues. If Issues are used (automation / visibility):

- Use templates in .github/ISSUE_TEMPLATE/
- Reference internal task; do not paste sensitive context

## Coding Guidelines (Minimal Summaries)

**Shell:**
- POSIX where practical, quote expansions, UPPER_CASE globals, meaningful exit codes, keep side-effects isolated

**Docker:**
- Pin base image digests/tags, multi-stage for build vs runtime, no secrets in layers, minimize image size

Refer to [docs/BEST_PRACTICES.md](./docs/BEST_PRACTICES.md) for deeper language & style specifics.

## Security & Compliance Quick Checks (Pre-PR)

Ask yourself:
- Did I disable or weaken encryption/logging? (If yes: ticket + justification)
- Any new IAM wildcard or broad principal? (Scope it or justify)
- Are KMS/S3 policy condition blocks still intact?
- Are all new resources tagged per strategy?

## Escalation / Blocking Triggers

Pause & request security / architect review if:
- Cross-account trust scope expands beyond ticket
- Network exposure broadens (e.g. 0.0.0.0/0 ingress) outside approved egress patterns
- Data processing patterns deviate from Medallion Architecture

## Glossary (Selected)

- **DoR**: Entry criteria ensuring work is actionable
- **DoD**: Exit criteria ensuring work is validated & complete
- **Drift**: Divergence between declared and actual cloud state
- **Medallion Architecture**: Bronze/Silver/Gold data processing layers

## Feedback & Continuous Improvement

Open a chore ticket to propose process changes; keep this file lean—prefer links over prose expansion.

## Code of Conduct

All contributors must follow the [Code of Conduct](./.github/CODE_OF_CONDUCT.md).

If anything here conflicts with a document under docs/, the docs/ file wins—submit a PR to reconcile.
