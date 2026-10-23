---
version: 1
slug: "admin-src-app-tsx"
primary_target: "admin/src/App.tsx"
related_targets: ["admin/src/style.css"]
---

# Administration workbench

Scope: `admin/src/App.tsx`, served at `/admin/`. Mode: Operate.

Operators inspect and conditionally edit real durable registers while following
node health, local accepted state, and correlated protocol messages. The chosen
composition is a balanced master/detail workbench: compact node strip, searchable
key inventory, established-value/ETag editor, local-state table, and bounded live
console below. On narrow screens these become a vertical task sequence with
independently scrollable tables.

The meaningful interaction is a conditional edit followed through the live
messages and each node's local observations. Preserve exact bytes and versions,
explicit unknown outcomes, separate local/quorum authority, stable drafts, and
the fixed multi-host SSE topology.

Product direction and composition were confirmed by the user before this session.
The build uses the restrained system-control language documented in DESIGN.md.
Authentication/deployment policy beyond the existing trusted-host boundary remains
an application-owner decision.
