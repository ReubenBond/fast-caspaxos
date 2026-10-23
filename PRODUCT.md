# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

People operating and debugging a Fast CASPaxos cluster, inspecting durable
registers and following consensus activity across its configured hosts.

## Product Purpose

The administration console provides a balanced workbench for keys and conditional
editing alongside live node and protocol-message diagnostics.

## Operating Context

The existing ASP.NET host serves the console. The browser discovers the fixed peer
configuration from that host and subscribes directly to every peer over SSE.
Operators use quorum-established register reads and per-node local snapshots side
by side, with their different authority explicitly labeled.

## Capabilities and Constraints

- Search and paginate logical keys; inspect complete bytes and opaque ETags.
- Create absent registers and conditionally update existing registers.
- Preserve binary values through Base64 and lossless UTF-8 views.
- Surface conflicts and ambiguous write outcomes with explicit reconciliation.
- Show each node's stream connectivity, reported storage health, local accepted
  state, promises, ballots, and bounded protocol-message previews.
- Preserve per-node order and boot generations. Show stream resets/history gaps.
  Cross-host wall clocks are observation times, not global causal ordering.
- Bound event retention and refresh work. Pausing the timeline retains live state
  subscriptions.
- The approved implementation uses TypeScript, React, Vite, Vitest, React Testing
  Library, and Playwright. Tests control timing and races deterministically.
- Administrative deployment authorization remains an operator-owned boundary.
  Configured host/admin origins govern direct browser access.
- Open decision: future authentication/deployment policy beyond existing host
  configuration.

## Brand Commitments

Fast CASPaxos. Task-oriented, precise language; restrained palette; consistent
system typography and controls; dense, legible information.

## Evidence on Hand

The repository contains runnable SQLite-backed hosts and REST/SSE implementations.
Preview demonstrations use real keys in isolated temporary databases and identify
their demo content.

## Product Principles

- Preserve the distinction between local observations and quorum-established truth.
- Preserve exact keys, bytes, versions, and conditional-write intent.
- Make uncertain outcomes actionable and visible.
- Keep diagnostic work bounded under high event rates.

## Accessibility & Inclusion

Semantic controls and tables, visible keyboard focus, keyboard-operable navigation,
explicit loading/error/empty/stale states, constrained long-content overflow, and
responsive layouts that preserve operator tasks.
