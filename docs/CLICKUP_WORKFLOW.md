# ClickUp Workflow & Integration (Human Guide)

This document summarizes how to interact with the ClickUp workspace for the NaNLABS Data Warehouse project. It intentionally
condenses prior verbose guidance. For AI usage guardrails see `AGENTS.md`.

**Primary ClickUp Space**: [NaNLABS - Data Warehouse](https://app.clickup.com/459857/v/s/90171135641)
**Workspace ID**: `459857`
**ClickUp Space ID**: `90171135641`

## 1. Core Principles

1. Search First: Never create a ticket before searching (Backlog, Epics, Decisions, Templates)
2. Template-First: Use list templates (tickets starting with `TEMPLATE` or with the tag `is-template`) for new items
3. Single Source of Truth: All substantive work, decisions, risks, and incidents must have a ClickUp ticket
4. Traceability: Every branch, commit, and PR references a `DWH-###` ticket
5. Separation: Long-lived institutional knowledge → Documentation lists; execution tasks → Backlog / Epics

## 2. Workspace Structure (High-Level)

| Area | Purpose | Key Lists / Artifacts | When to Use |
|------|---------|-----------------------|-------------|
| General Information | Account/team context | Team | Onboarding, roles, stakeholder lookup |
| Documentation | Persistent knowledge | Decisions & Agreements, Assumptions & Constraints, Lessons Learned, Practices, Operations, Spikes | Record decisions, constraints, standards, research outcomes |
| Private | Internal / sensitive material | Meeting Notes, OIRs (Opportunities / Issues / Risks) | Internal discussions, risk logging |
| Project Information (SOW #1) | Project context & tracking | Requirements, Meeting Notes, Milestones, Incidents, Overview, Closure Summary | Scope, milestone review, incident logging |
| Scrum Workflow (SOW #1) | Delivery execution | Backlog (primary), Epics, Sprint Planning Notes, Time Reporting | Feature work, sprint planning, epic tracking |
| Assessment | Evaluation & improvement | Engineering Follow Ups, Tech Unit, Management Unit | Follow-ups, capability assessments |

## 3. Ticket Types & Naming (Recommended)

| Type | Pattern | Example |
|------|--------|---------|
| Feature / Data Pipeline | `🔧 [Component] - Description` | `🔧 [Glue Job] - Implement Team Tailor candidates bronze processing` |
| Bug | `🐛 [BUG] - Description` | `🐛 [Glue Jobs] - Team Tailor API rate limit handling failure` |
| Docs | `📚 [DOCS] - Topic` | `📚 [Architecture] - Update medallion layer guidelines` |
| Decision | `📋 [DECISION] - Topic` | `📋 [DECISION] - Iceberg table partitioning strategy` |
| Agreement | `🤝 [AGREEMENT] - Topic` | `🤝 [AGREEMENT] - Data quality scoring methodology` |
| Epic | `🎯 [EPIC] - Theme` | `🎯 [EPIC] - Implement Team Tailor talent data pipeline` |

## 4. Definition of Ready (Snapshot)

A ticket is Ready when it has:

- Clear objective / outcome
- Acceptance criteria (testable, unambiguous)
- Dependencies resolved or stated
- Data security / compliance considerations noted (or explicitly N/A)
- Rollback / mitigation approach if risk > low
- Linked design / architecture (if complexity > trivial)
- Data quality requirements specified (if data processing)

## 5. Definition of Done (Snapshot)

Done when:

- Code merged & plan reviewed (if infrastructure)
- Validation performed & documented (data quality check, query validation, log check, etc.)
- Data security posture unchanged or approved delta documented
- Related docs / decisions updated or N/A
- No unresolved TODOs / follow-ups except separately ticketed
- Epic progress updated (if applicable)
- Data lineage updated (if data processing)

## 6. Development Lifecycle (Condensed)

| Phase | Actions |
|-------|---------|
| Plan | Search existing tickets; confirm template; refine AC & scope; identify medallion layer |
| Implement | Branch `feature/DWH-###-slug`; regular commits referencing ticket; update status & comments |
| Validate | Run tests/data quality checks; record evidence; link PR to ticket |
| Review | Address feedback; update ticket with decisions / deltas; validate data lineage |
| Close | Confirm DoD checklist; lessons learned if valuable; update epics |

## 7. Branch, Commit & PR Conventions

- Branch: `feature|fix|chore/DWH-###-short-slug`
- Commit subject: `DWH-###: concise change summary`
- PR title: `DWH-###: short description`
- PR body: link ticket, summarize plan diff (infra), validation steps, data security impact (N/A or explicit)

## 8. Tags (Common Examples)

`infrastructure`, `glue`, `iceberg`, `medallion-architecture`, `data-pipeline`, `bronze-layer`, `silver-layer`, `gold-layer`, `teamtailor`, `talent`, `data-quality`, `documentation`, `bug`, `enhancement`

Use minimal consistent tagging—avoid redundant synonyms.

## 9. Decision & Knowledge Capture

| Scenario | Where to Log |
|----------|--------------|
| Data architecture choice | Documentation > Decisions & Agreements |
| Data pipeline runbook | Documentation > Operations |
| Data quality risk identified | Private > OIRs |
| Data source research output | Documentation > Spikes |
| Post-data-incident notes | Project Information > Incidents |

## 10. Time Tracking (When Required)

Track when client reporting or cost allocation is in scope:

- Data pipeline / Glue job implementation
- Data source integration / spikes
- Data incident response
- Documentation above trivial edits

Provide succinct descriptions: `Implement Team Tailor candidates bronze processing (staging)`.

## 11. MCP Assistant Usage (Human Awareness)

AI automation must:

1. Search (`mcp_clickup_get_workspace_tasks`) before creating
2. Use templates where present
3. Include ticket IDs in generated branches / commits / PRs
4. Not create epics / structural changes without explicit instruction

## 12. Common Anti-Patterns (Avoid)

- Starting work without a ticket
- Overloading one ticket with multiple unrelated data sources or medallion layers
- Duplicating decision rationale in multiple tickets
- Creating custom formats instead of templates
- Unreferenced refactors (open separate `chore` tickets)
- Mixing data processing logic across medallion layers in single ticket

## 13. Escalation Triggers

Escalate (comment + mention stakeholders) if:

- Unplanned destructive data pipeline change emerges in plan
- Cross-account data access scope broadens unexpectedly
- Timeline risk threatens milestone
- Security review needed for data encryption / PII handling change
- Data quality issues threaten downstream analytics

## 14. Quick Reference Checklist

| Action | Check |
|--------|-------|
| Creating ticket | Searched & template used |
| Starting branch | Branch name includes `DWH-###` |
| Writing commits | All subjects reference ticket |
| Opening PR | Ticket linked + validation + data security note |
| Closing ticket | DoD satisfied + docs updated + data quality validated |

---
Keep this file lean. Propose improvements via `chore` ticket; do not embed sensitive or client-proprietary data.
