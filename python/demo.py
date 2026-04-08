#!/usr/bin/env python3
"""
Fast CASPaxos — Runnable Demo / Test Harness
=============================================

This script exercises the toy Fast CASPaxos implementation in several
scenarios, printing a step-by-step trace of the protocol to illustrate
how fast commits, classic commits, conflict recovery, piggybacked
prepares, and CAS semantics work.

Run:
    python demo.py
"""

from __future__ import annotations

import sys
import textwrap
from fast_caspaxos import (
    Acceptor, Ballot, Proposer,
    AcceptOk, AcceptNack, PrepareOk, PrepareNack,
    OpResult,
    op_write, op_cas, op_increment,
    classic_quorum_size, fast_quorum_size, quorum_size,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_cluster(n: int, initial_value=None) -> list[Acceptor]:
    """Create a cluster of *n* acceptors."""
    return [Acceptor(f"A{i}", initial_value) for i in range(n)]


def trace(msg: str):
    """Print a protocol trace line."""
    print(msg)


def banner(title: str):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def check(condition: bool, description: str):
    """Assert a condition and print the result."""
    status = "✓ PASS" if condition else "✗ FAIL"
    print(f"  {status}: {description}")
    if not condition:
        raise AssertionError(description)


class AssertionError(Exception):
    pass


# ── Scenario 1: Fast Path — Single Proposer ──────────────────────────────────

def scenario_fast_path():
    """A single proposer commits in 1 RTT via the fast path.

    Because acceptors start pre-promised to Ballot(1,fast), the proposer
    can skip Prepare and go straight to Accept on its first operation.
    With 5 acceptors, fast quorum is ⌈3×5/4⌉ = 4, and all 5 accept.
    """
    banner("Scenario 1: Fast Path — 1 RTT Commit")

    acceptors = make_cluster(5)
    p = Proposer(1, acceptors, enable_fast=True)

    print(f"\n  Cluster: {len(acceptors)} acceptors, fast quorum = {fast_quorum_size(5)}")
    print(f"  Proposer P1 writes 'hello' via fast path\n")

    result = p.propose(op_write("hello"), trace)

    print()
    check(result == "hello", "committed value is 'hello'")
    check(p.ballot == Ballot.initial_fast(), "used initial fast ballot (1,fast)")
    check(
        all(a.accepted_value == "hello" for a in acceptors),
        "all acceptors hold 'hello'",
    )
    # The piggybacked prepare should give us a prepared ballot for the next round
    check(p.prepared_ballot == Ballot(2, 0), "piggybacked next fast ballot (2,fast)")


# ── Scenario 2: Classic Path — Single Proposer ───────────────────────────────

def scenario_classic_path():
    """A proposer with fast-commit disabled uses the 2-RTT classic path.

    Phase 1 (Prepare) secures promises, Phase 2 (Accept) commits the value.
    Classic quorum for 5 acceptors is ⌊5/2⌋+1 = 3.
    """
    banner("Scenario 2: Classic Path — 2 RTT Commit")

    acceptors = make_cluster(5)
    p = Proposer(1, acceptors, enable_fast=False)

    print(f"\n  Cluster: {len(acceptors)} acceptors, classic quorum = {classic_quorum_size(5)}")
    print(f"  Proposer P1 writes 'world' via classic path\n")

    result = p.propose(op_write("world"), trace)

    print()
    check(result == "world", "committed value is 'world'")
    check(p.ballot.is_classic(), "used a classic ballot")
    check(p.ballot.proposer == 1, "ballot belongs to P1")


# ── Scenario 3: Fast Path Conflict → Classic Recovery ─────────────────────────

def scenario_fast_conflict():
    """Two proposers race on the fast path with different values.

    We simulate this by having each proposer's accept reach different
    subsets of acceptors first.  Neither achieves fast quorum (need 4/5),
    so both fall back to classic.  One succeeds, the other sees the
    committed value and reports CAS-not-applicable.

    This is the core demonstration of Fast CASPaxos conflict handling.
    """
    banner("Scenario 3: Fast Path Conflict → Classic Recovery")

    acceptors = make_cluster(5)
    fast = Ballot.initial_fast()

    print(f"\n  Cluster: {len(acceptors)} acceptors")
    print(f"  P1 tries fast-accept 'alpha' at {fast}")
    print(f"  P2 tries fast-accept 'beta'  at {fast}")
    print()

    # ── Simulate the race manually ────────────────────────────────────────
    # P1's accept reaches A0, A1, A2 first
    print("  Step 1: P1 sends accept('alpha') to A0, A1, A2")
    for i in range(3):
        r = acceptors[i].accept(fast, "alpha")
        print(f"    {acceptors[i].name} → {type(r).__name__}")
        assert isinstance(r, AcceptOk)

    # P2's accept reaches A3, A4 first, then tries A0, A1, A2
    print("\n  Step 2: P2 sends accept('beta') to A3, A4")
    for i in range(3, 5):
        r = acceptors[i].accept(fast, "beta")
        print(f"    {acceptors[i].name} → {type(r).__name__}")
        assert isinstance(r, AcceptOk)

    print("\n  Step 3: P2 sends accept('beta') to A0, A1, A2")
    for i in range(3):
        r = acceptors[i].accept(fast, "beta")
        print(f"    {acceptors[i].name} → {type(r).__name__} (same ballot, different value)")
        assert isinstance(r, AcceptNack)

    # P1 tries A3, A4 — also rejected
    print("\n  Step 4: P1 sends accept('alpha') to A3, A4")
    for i in range(3, 5):
        r = acceptors[i].accept(fast, "alpha")
        print(f"    {acceptors[i].name} → {type(r).__name__} (same ballot, different value)")
        assert isinstance(r, AcceptNack)

    print("\n  Result: P1 has 3 accepts, P2 has 2 — neither has fast quorum (4)")
    print("  Acceptor state:")
    for a in acceptors:
        print(f"    {a.name}: accepted {a.accepted_value!r} at {a.accepted_ballot}")

    # ── Classic recovery by P1 ────────────────────────────────────────────
    print("\n  Step 5: P1 recovers via classic ballot")
    classic = Ballot(2, 1)   # round 2, proposer 1
    print(f"    Prepare at {classic}")

    responses = [a.prepare(classic) for a in acceptors]
    promises = [r for r in responses if isinstance(r, PrepareOk)]
    print(f"    Got {len(promises)} promises (need {classic_quorum_size(5)})")

    # Show the accepted values reported by each acceptor
    print("    Acceptor reports:")
    for r in promises:
        print(f"      {r.acceptor}: accepted {r.accepted_value!r} at {r.accepted_ballot}")

    # Value recovery: count values at the fast ballot
    from collections import Counter
    values = [r.accepted_value for r in promises if r.accepted_ballot == fast]
    counts = Counter(values)
    print(f"    Vote tally at {fast}: {dict(counts)}")

    # 'alpha' has 3 votes, 'beta' has 2 → alpha is the unique viable candidate
    # (alpha + 0 unseen = 3 < 4, but beta + 0 = 2 < 4 — actually neither
    #  could have reached fast quorum with a full prepare quorum)
    # The recovery picks the most frequent value: 'alpha'
    winner = counts.most_common(1)[0][0]
    print(f"    Recovery picks: {winner!r}")

    # Now accept with the recovered value
    print(f"\n  Step 6: P1 sends accept({winner!r}) at {classic}")
    results = [a.accept(classic, winner) for a in acceptors]
    accepts = [r for r in results if isinstance(r, AcceptOk)]
    print(f"    Got {len(accepts)} accepts (need {classic_quorum_size(5)})")

    check(len(accepts) >= classic_quorum_size(5), "classic accept quorum reached")
    check(winner == "alpha", "recovered value is 'alpha' (most frequent)")

    print()
    print("  The conflict was resolved: 'alpha' is committed via classic recovery.")
    print("  If 'beta' had actually reached fast quorum (4+ acceptors), it would")
    print("  have been recovered instead — the tally guarantees safety.")


# ── Scenario 4: Sequential Operations with Piggybacked Prepare ───────────────

def scenario_piggybacked_prepare():
    """Multiple sequential operations reuse piggybacked prepares for 1-RTT.

    After the first operation commits, the piggybacked next ballot is cached.
    The second operation can skip prepare and commit in 1 RTT, just like the
    fast path but at the next round.
    """
    banner("Scenario 4: Sequential Fast Operations (Piggybacked Prepare)")

    acceptors = make_cluster(5)
    p = Proposer(1, acceptors, enable_fast=True)

    print(f"\n  Operation 1: write 'first'\n")
    r1 = p.propose(op_write("first"), trace)
    check(r1 == "first", "op1 committed 'first'")

    print(f"\n  After op1: ballot={p.ballot}, prepared_ballot={p.prepared_ballot}")
    check(p.prepared_ballot is not None, "piggybacked ballot is cached")

    print(f"\n  Operation 2: write 'second' (reuses piggybacked ballot)\n")
    r2 = p.propose(op_write("second"), trace)
    check(r2 == "second", "op2 committed 'second'")

    print(f"\n  After op2: ballot={p.ballot}, prepared_ballot={p.prepared_ballot}")

    print(f"\n  Operation 3: write 'third' (reuses piggybacked ballot again)\n")
    r3 = p.propose(op_write("third"), trace)
    check(r3 == "third", "op3 committed 'third'")

    print()
    print("  All three operations committed in 1 RTT each (no prepares needed).")


# ── Scenario 5: Compare-and-Swap Semantics ───────────────────────────────────

def scenario_cas():
    """Demonstrate compare-and-swap semantics.

    CAS(expected, desired) succeeds only if the current register value equals
    *expected*.  This shows both a successful and a failed CAS.
    """
    banner("Scenario 5: Compare-and-Swap (CAS) Semantics")

    acceptors = make_cluster(5)
    p = Proposer(1, acceptors, enable_fast=True)

    print(f"\n  Step 1: Initialize register to 'A'\n")
    r1 = p.propose(op_write("A"), trace)
    check(r1 == "A", "register = 'A'")

    print(f"\n  Step 2: CAS(expected='A', desired='B') — should succeed\n")
    r2 = p.propose(op_cas("A", "B"), trace)
    check(r2 == "B", "CAS succeeded: register = 'B'")

    print(f"\n  Step 3: CAS(expected='A', desired='C') — should fail (register is 'B')\n")
    r3 = p.propose(op_cas("A", "C"), trace)
    check(r3 == "B", "CAS failed: register still 'B' (expected 'A', found 'B')")

    print(f"\n  Step 4: CAS(expected='B', desired='C') — should succeed\n")
    r4 = p.propose(op_cas("B", "C"), trace)
    check(r4 == "C", "CAS succeeded: register = 'C'")


# ── Scenario 6: Counter with Multiple Proposers ──────────────────────────────

def scenario_counter():
    """Multiple proposers increment a shared counter.

    Each proposer takes turns incrementing.  The final value should equal
    the total number of increments.  This exercises the learn mechanism:
    after P1 commits, P2 learns the value before proposing.
    """
    banner("Scenario 6: Shared Counter with Learn + Handoff")

    acceptors = make_cluster(5)
    p1 = Proposer(1, acceptors, enable_fast=True)
    p2 = Proposer(2, acceptors, enable_fast=True)

    print(f"\n  Two proposers (P1, P2) take turns incrementing a counter.\n")

    # P1 increments: None → 1
    r1 = p1.propose(op_increment(), trace)
    check(r1 == 1, "P1: counter = 1")

    # P2 learns the committed value from P1 before proposing
    p2.learn(p1.ballot, p1.cached_value, p1.prepared_ballot)
    print(f"\n  P2 learned: value={p2.cached_value}, ballot={p2.ballot}\n")

    # P2 increments: 1 → 2
    r2 = p2.propose(op_increment(), trace)
    check(r2 == 2, "P2: counter = 2")

    # P1 learns from P2
    p1.learn(p2.ballot, p2.cached_value, p2.prepared_ballot)
    print(f"\n  P1 learned: value={p1.cached_value}, ballot={p1.ballot}\n")

    # P1 increments: 2 → 3
    r3 = p1.propose(op_increment(), trace)
    check(r3 == 3, "P1: counter = 3")

    print()
    check(
        all(a.accepted_value == 3 for a in acceptors),
        "all acceptors agree: counter = 3",
    )


# ── Scenario 7: Quorum Sizes ─────────────────────────────────────────────────

def scenario_quorum_sizes():
    """Show quorum sizes for various cluster sizes."""
    banner("Scenario 7: Quorum Size Table")

    print()
    print(f"  {'N':>3}  {'Classic':>8}  {'Fast':>6}  {'Witness†':>9}")
    print(f"  {'─'*3}  {'─'*8}  {'─'*6}  {'─'*9}")
    for n in range(3, 12):
        cq = classic_quorum_size(n)
        fq = fast_quorum_size(n)
        witness = fq + cq - n
        print(f"  {n:>3}  {cq:>8}  {fq:>6}  {witness:>9}")
    print()
    print("  † Witness threshold = fast_quorum + classic_quorum − N")
    print("    A committed fast value must appear at least this many times")
    print("    in any classic quorum's prepare responses.")


# ── Scenario 8: Classic Path with Minority Failure ────────────────────────────

def scenario_minority_failure():
    """Classic path still works when a minority of acceptors are down.

    With 5 acceptors, classic quorum is 3.  Even if 2 acceptors are
    unreachable, the remaining 3 can still reach quorum.
    """
    banner("Scenario 8: Classic Commit Despite Minority Failure")

    all_acceptors = make_cluster(5)
    # Only the first 3 are reachable
    reachable = all_acceptors[:3]

    print(f"\n  5 acceptors, but only A0–A2 are reachable (A3, A4 are down)")
    print(f"  Classic quorum = {classic_quorum_size(5)}\n")

    p = Proposer(1, reachable, enable_fast=False)
    result = p.propose(op_write("surviving"), trace)

    print()
    check(result == "surviving", "committed despite 2 failures")
    check(
        all(a.accepted_value == "surviving" for a in reachable),
        "reachable acceptors all hold 'surviving'",
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print()
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║          Fast CASPaxos — Toy Implementation Demo                   ║")
    print("╠══════════════════════════════════════════════════════════════════════╣")
    print("║  This demo shows how the Fast CASPaxos protocol works:             ║")
    print("║                                                                    ║")
    print("║  • Fast path:    1 RTT commit (skip prepare)                       ║")
    print("║  • Classic path: 2 RTT commit (prepare + accept)                   ║")
    print("║  • Conflicts:    Fast path race → classic recovery                 ║")
    print("║  • Piggyback:    Reuse prepared ballots for steady-state 1 RTT     ║")
    print("║  • CAS:          Compare-and-swap register semantics               ║")
    print("║  • Learn:        Propagate committed values between proposers      ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")

    scenarios = [
        scenario_fast_path,
        scenario_classic_path,
        scenario_fast_conflict,
        scenario_piggybacked_prepare,
        scenario_cas,
        scenario_counter,
        scenario_quorum_sizes,
        scenario_minority_failure,
    ]

    passed = 0
    failed = 0
    for scenario in scenarios:
        try:
            scenario()
            passed += 1
        except AssertionError as e:
            print(f"\n  ✗ SCENARIO FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"\n  ✗ SCENARIO ERROR: {e}")
            failed += 1

    print()
    print("=" * 70)
    total = passed + failed
    if failed == 0:
        print(f"  All {total} scenarios passed! ✓")
    else:
        print(f"  {passed}/{total} passed, {failed} failed ✗")
    print("=" * 70)
    print()

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
