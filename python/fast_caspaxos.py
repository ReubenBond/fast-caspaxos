"""
Fast CASPaxos — A Toy Implementation for Exposition
====================================================

Fast CASPaxos is a consensus protocol for a *rewritable register*:
clients propose operations (compare-and-swap style) and the protocol
ensures linearizable agreement even when multiple proposers race.

It extends CASPaxos (which itself extends Paxos) with a *fast path*
borrowed from Fast Paxos.  The key idea is encoding the protocol mode
directly in the ballot tuple:

    Ballot = (round, proposer_id)

    proposer_id == 0  →  fast (shared) ballot — any proposer may use it
    proposer_id >  0  →  classic (owned) ballot — belongs to one proposer

Quorum sizes
------------
    Classic quorum : ⌊N/2⌋ + 1   (strict majority)
    Fast quorum    : ⌈3N/4⌉       (three-quarters)

The larger fast quorum ensures that a later classic recovery round can
always determine which value — if any — was committed during a fast round.

Protocol sketch
---------------
1. A proposer picks a ballot (fast or classic).
2. If the ballot is not yet prepared, the proposer runs Phase 1 (Prepare)
   to learn the current register value and secure promises from a quorum.
3. The proposer applies the client operation to the recovered value.
4. It runs Phase 2 (Accept) to commit the new value.
5. If a quorum accepts, the value is committed.  Otherwise the proposer
   retries at a higher classic ballot.

The fast path skips Phase 1 entirely on the very first operation (or when
a piggybacked prepare from a previous round is reusable), giving 1-RTT
commits when there is no contention.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


# ── Ballot ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Ballot:
    """A ballot identifies a consensus round.

    Ordering is lexicographic on (round, proposer).
    proposer == 0 means a fast (shared) ballot; proposer > 0 means classic.
    """

    round: int = 0
    proposer: int = 0

    # -- construction helpers --------------------------------------------------

    @classmethod
    def zero(cls) -> Ballot:
        """The zero ballot — means 'nothing seen yet'."""
        return cls(0, 0)

    @classmethod
    def initial_fast(cls) -> Ballot:
        """The first fast ballot that any proposer may use."""
        return cls(1, 0)

    @classmethod
    def initial_classic(cls, proposer_id: int) -> Ballot:
        """The first classic ballot owned by *proposer_id*."""
        assert proposer_id > 0
        return cls(1, proposer_id)

    def next_round(self, proposer_id: int) -> Ballot:
        """Advance to the next round for *proposer_id* (0 = fast)."""
        return Ballot(self.round + 1, proposer_id)

    # -- predicates ------------------------------------------------------------

    def is_zero(self) -> bool:
        return self.round == 0 and self.proposer == 0

    def is_fast(self) -> bool:
        return self.proposer == 0 and self.round > 0

    def is_classic(self) -> bool:
        return self.proposer > 0

    # -- ordering (Python dataclass comparison uses field order) ----------------

    def __lt__(self, other: Ballot) -> bool:
        return (self.round, self.proposer) < (other.round, other.proposer)

    def __le__(self, other: Ballot) -> bool:
        return (self.round, self.proposer) <= (other.round, other.proposer)

    def __gt__(self, other: Ballot) -> bool:
        return (self.round, self.proposer) > (other.round, other.proposer)

    def __ge__(self, other: Ballot) -> bool:
        return (self.round, self.proposer) >= (other.round, other.proposer)

    def __repr__(self) -> str:
        kind = "fast" if self.is_fast() else ("zero" if self.is_zero() else f"P{self.proposer}")
        return f"Ballot({self.round},{kind})"


# ── Messages ──────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PrepareOk:
    """An acceptor promises not to accept any ballot lower than the one it promised."""
    acceptor: str
    accepted_ballot: Ballot
    accepted_value: Any

@dataclass(frozen=True)
class PrepareNack:
    """An acceptor rejects a prepare because it already promised/accepted a higher ballot."""
    acceptor: str
    conflict_ballot: Ballot
    accepted_ballot: Ballot
    accepted_value: Any

@dataclass(frozen=True)
class AcceptOk:
    """An acceptor accepted the value at the given ballot."""
    acceptor: str
    promised_ballot: Ballot      # may be higher than the accepted ballot if piggybacked

@dataclass(frozen=True)
class AcceptNack:
    """An acceptor rejects an accept because a higher ballot is already in play."""
    acceptor: str
    conflict_ballot: Ballot


# ── Quorum helpers ────────────────────────────────────────────────────────────

def classic_quorum_size(n: int) -> int:
    """Strict majority: ⌊n/2⌋ + 1."""
    return n // 2 + 1

def fast_quorum_size(n: int) -> int:
    """Three-quarters quorum: ⌈3n/4⌉."""
    return -(-3 * n // 4)            # ceiling division trick

def quorum_size(ballot: Ballot, n: int) -> int:
    """Return the quorum size appropriate for *ballot*."""
    return fast_quorum_size(n) if ballot.is_fast() else classic_quorum_size(n)


# ── Acceptor ──────────────────────────────────────────────────────────────────

class Acceptor:
    """The acceptor side of Fast CASPaxos.

    State:
        promised  — highest ballot this acceptor has promised not to go below
        accepted_ballot — ballot at which the last value was accepted
        accepted_value  — the value accepted at that ballot

    Fresh acceptors start with promised = InitialFast so that the very first
    fast ballot (1,0) can be used without an explicit prepare.
    """

    def __init__(self, name: str, initial_value: Any = None):
        self.name = name
        self.promised: Ballot = Ballot.initial_fast()
        self.accepted_ballot: Ballot = Ballot.zero()
        self.accepted_value: Any = initial_value

    @property
    def _max_ballot(self) -> Ballot:
        return max(self.promised, self.accepted_ballot)

    # -- Phase 1: Prepare ------------------------------------------------------

    def prepare(self, ballot: Ballot) -> PrepareOk | PrepareNack:
        """Handle a prepare request.

        If *ballot* is at least as high as any ballot we have seen, promise it
        and return our current accepted state.  Otherwise reject.
        """
        if ballot < self._max_ballot:
            return PrepareNack(
                acceptor=self.name,
                conflict_ballot=self._max_ballot,
                accepted_ballot=self.accepted_ballot,
                accepted_value=self.accepted_value,
            )

        self.promised = ballot
        return PrepareOk(
            acceptor=self.name,
            accepted_ballot=self.accepted_ballot,
            accepted_value=self.accepted_value,
        )

    # -- Phase 2: Accept -------------------------------------------------------

    def accept(
        self,
        ballot: Ballot,
        value: Any,
        next_prepared: Ballot | None = None,
    ) -> AcceptOk | AcceptNack:
        """Handle an accept request.

        Accepts the value at *ballot* if:
        • the request is an idempotent replay (same ballot AND same value), OR
        • *ballot* is strictly newer than anything we've seen.

        The optional *next_prepared* lets the proposer piggyback a promise for
        the next ballot, avoiding an extra prepare round-trip later.
        """
        # Idempotent replay — always safe
        if ballot == self.accepted_ballot and value == self.accepted_value:
            return AcceptOk(acceptor=self.name, promised_ballot=self.promised)

        # Stale ballot
        if ballot < self._max_ballot:
            return AcceptNack(acceptor=self.name, conflict_ballot=self._max_ballot)

        # Same ballot but different value — reject to prevent ambiguity
        if ballot == self.accepted_ballot:
            return AcceptNack(acceptor=self.name, conflict_ballot=self._max_ballot)

        # Accept
        self.accepted_ballot = ballot
        self.accepted_value = value

        # Piggyback: raise the promise for the next ballot if requested
        if next_prepared is not None and next_prepared > self.promised:
            self.promised = next_prepared

        return AcceptOk(acceptor=self.name, promised_ballot=self.promised)

    def __repr__(self) -> str:
        return (
            f"Acceptor({self.name}: promised={self.promised}, "
            f"accepted={self.accepted_ballot}, value={self.accepted_value!r})"
        )


# ── Operation result ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class OpResult:
    """Result of applying a CAS operation to the current register value."""
    success: bool
    value: Any
    not_applicable: bool = False    # CAS precondition not met (complete without change)


# ── Proposer ──────────────────────────────────────────────────────────────────

class Proposer:
    """The proposer side of Fast CASPaxos.

    A proposer drives the consensus protocol to commit a client operation.
    It maintains a local cache of the register value and the highest ballot
    it has used, plus an optional *prepared_ballot* from a piggybacked prepare.

    Parameters
    ----------
    proposer_id : int > 0
        Unique identifier for this proposer (used in classic ballots).
    acceptors : list[Acceptor]
        The set of acceptors in the cluster.
    enable_fast : bool
        If True, try the fast (shared) ballot path before falling back to
        classic.  If False, always use classic ballots (2-RTT).
    """

    def __init__(
        self,
        proposer_id: int,
        acceptors: list[Acceptor],
        enable_fast: bool = True,
    ):
        assert proposer_id > 0
        self.id = proposer_id
        self.acceptors = acceptors
        self.enable_fast = enable_fast
        self.ballot: Ballot = Ballot.zero()
        self.cached_value: Any = None
        self.prepared_ballot: Ballot | None = None

    # ── Main entry point ─────────────────────────────────────────────────────

    def propose(
        self,
        operation: Callable[[Any], OpResult],
        trace: Callable[[str], None] | None = None,
    ) -> Any:
        """Drive *operation* through the Fast CASPaxos protocol.

        *operation* is a function that takes the current register value and
        returns an OpResult.  This method runs the full consensus protocol
        (possibly retrying on conflicts) and returns the committed value.

        *trace* is an optional callback for observing the protocol steps.
        """
        log = trace or (lambda _: None)

        while True:
            ballot, need_prepare = self._choose_ballot()
            log(f"  [{self}] using {ballot}, need_prepare={need_prepare}")

            # Phase 1: Prepare (unless we can skip it)
            if need_prepare:
                ok, base_value = self._run_prepare(ballot, log)
                if not ok:
                    log(f"  [{self}] prepare failed, retrying")
                    continue
                self.cached_value = base_value

            # Apply the operation to the current value
            result = operation(self.cached_value)

            if result.not_applicable:
                log(f"  [{self}] operation not applicable, returning cached value")
                return self.cached_value

            if not result.success:
                log(f"  [{self}] operation failed, retrying")
                continue

            new_value = result.value

            # Phase 2: Accept
            committed = self._run_accept(ballot, new_value, log)
            if committed is not None:
                self.cached_value = committed
                log(f"  [{self}] committed {committed!r} at {ballot}")
                return committed

            log(f"  [{self}] accept rejected, retrying")
            # Loop retries with a higher classic ballot

    # ── Ballot selection ─────────────────────────────────────────────────────

    def _choose_ballot(self) -> tuple[Ballot, bool]:
        """Pick the next ballot and decide whether to run prepare.

        Returns (ballot, need_prepare).  Three cases:

        1. Reusable piggybacked prepared ballot → skip prepare.
        2. First operation with fast-commit enabled → use initial fast ballot,
           skip prepare (acceptors start pre-promised to the first fast ballot).
        3. Otherwise → advance to a classic ballot and run prepare.
        """
        # Case 1: reuse piggybacked prepared ballot
        if self.prepared_ballot is not None and self.prepared_ballot > self.ballot:
            ballot = self.prepared_ballot
            self.prepared_ballot = None
            self.ballot = ballot
            return ballot, False

        # Case 2: initial fast ballot
        if self.enable_fast and self.ballot < Ballot.initial_fast():
            self.ballot = Ballot.initial_fast()
            return self.ballot, False

        # Case 3: classic ballot
        self.ballot = self.ballot.next_round(self.id)
        return self.ballot, True

    # ── Phase 1: Prepare ─────────────────────────────────────────────────────

    def _run_prepare(
        self,
        ballot: Ballot,
        log: Callable[[str], None],
    ) -> tuple[bool, Any]:
        """Send prepare to all acceptors and collect responses.

        Returns (success, recovered_value).  On failure, bumps self.ballot past
        the highest conflicting ballot so the next retry is high enough.
        """
        n = len(self.acceptors)
        responses = [a.prepare(ballot) for a in self.acceptors]

        promises = [r for r in responses if isinstance(r, PrepareOk)]
        nacks = [r for r in responses if isinstance(r, PrepareNack)]

        log(f"  [{self}] prepare: {len(promises)} promises, {len(nacks)} nacks")

        if len(promises) < classic_quorum_size(n):
            # Not enough promises — bump past the highest conflict
            if nacks:
                max_conflict = max(r.conflict_ballot for r in nacks)
                if max_conflict > self.ballot:
                    self.ballot = max_conflict
            return False, None

        # Recover the current register value from acceptors' accepted state
        all_responses = promises + nacks   # nacks also carry accepted state
        value = self._resolve_value(all_responses, n, log)
        if value is _CONFLICT:
            log(f"  [{self}] fast round ambiguous, falling back to classic")
            return False, None

        return True, value

    def _resolve_value(
        self,
        responses: list,
        n: int,
        log: Callable[[str], None],
    ) -> Any:
        """Determine the current register value from prepare responses.

        Three cases:
        - No acceptor has accepted anything → use cached value (or None).
        - Highest accepted ballot is classic → use its value (it was serialized
          by a prior prepare, so there's no ambiguity).
        - Highest accepted ballot is fast → tally votes to determine whether
          a value was committed (and which one).
        """
        # Find the highest accepted ballot across all responses
        best_ballot = Ballot.zero()
        best_value = None
        for r in responses:
            ab = r.accepted_ballot
            av = r.accepted_value
            if ab > best_ballot:
                best_ballot = ab
                best_value = av

        if best_ballot.is_zero():
            return self.cached_value

        if best_ballot.is_classic():
            return best_value

        # ── Fast ballot recovery ──────────────────────────────────────────
        # Multiple proposers may have accepted different values at the same
        # fast ballot.  We must figure out which value (if any) was committed.
        values_at_best = [
            r.accepted_value
            for r in responses
            if r.accepted_ballot == best_ballot
        ]
        counts = Counter(values_at_best)

        # If only one distinct value, it's unambiguous
        if len(counts) <= 1:
            return best_value

        # Check: could more than one value still have reached fast quorum?
        # unseen = acceptors we didn't hear from in the prepare
        unseen = n - len(responses)
        fq = fast_quorum_size(n)
        viable = [v for v, c in counts.items() if c + unseen >= fq]

        if len(viable) > 1:
            # Ambiguous: more than one value might have been committed.
            # Must retry with a higher classic ballot.
            return _CONFLICT

        # Either 0 or 1 value could have been committed → pick most frequent
        return counts.most_common(1)[0][0]

    # ── Phase 2: Accept ──────────────────────────────────────────────────────

    def _run_accept(
        self,
        ballot: Ballot,
        value: Any,
        log: Callable[[str], None],
    ) -> Any | None:
        """Send accept to all acceptors and check for quorum.

        Returns the committed value on success, or None on failure (bumps
        self.ballot past the highest conflict for the retry).
        """
        n = len(self.acceptors)
        next_ballot = self._choose_piggyback_ballot(ballot)

        responses = [a.accept(ballot, value, next_ballot) for a in self.acceptors]

        accepts = [r for r in responses if isinstance(r, AcceptOk)]
        nacks = [r for r in responses if isinstance(r, AcceptNack)]

        needed = quorum_size(ballot, n)
        log(f"  [{self}] accept: {len(accepts)}/{needed} accepts, {len(nacks)} nacks")

        if len(accepts) >= needed:
            # Committed!  Check if the piggybacked prepare was echoed.
            if next_ballot is not None:
                echoed = sum(
                    1 for r in accepts if r.promised_ballot == next_ballot
                )
                if echoed >= quorum_size(next_ballot, n):
                    self.prepared_ballot = next_ballot
                    log(f"  [{self}] piggybacked {next_ballot} for next round")
            return value

        # Rejected — bump past highest conflict
        if nacks:
            max_conflict = max(r.conflict_ballot for r in nacks)
            if max_conflict > self.ballot:
                self.ballot = max_conflict
        return None

    # ── Piggybacked prepare ──────────────────────────────────────────────────

    def _choose_piggyback_ballot(self, current_ballot: Ballot) -> Ballot | None:
        """Choose the next ballot to piggyback in the accept message.

        If fast mode is enabled, piggyback the next fast ballot so any proposer
        can reuse it.  Otherwise, piggyback the next classic ballot for this
        proposer.
        """
        if self.enable_fast:
            return current_ballot.next_round(0)       # next fast ballot
        return current_ballot.next_round(self.id)     # next owned classic ballot

    # ── Learn (external value propagation) ────────────────────────────────────

    def learn(self, accepted_ballot: Ballot, value: Any, prepared: Ballot | None = None):
        """Learn a value committed by another proposer.

        This is *not* a consensus round — it is local state synchronization.
        Used when a committed value is broadcast to all proposers.
        """
        if accepted_ballot.is_zero() or accepted_ballot < self.ballot:
            return
        self.ballot = accepted_ballot
        self.cached_value = value
        if prepared is not None and (self.prepared_ballot is None or prepared > self.prepared_ballot):
            self.prepared_ballot = prepared

    def __repr__(self) -> str:
        return f"P{self.id}"


# Sentinel for ambiguous fast-round recovery
_CONFLICT = object()


# ── Convenience: operation constructors ───────────────────────────────────────

def op_write(new_value: Any) -> Callable[[Any], OpResult]:
    """Unconditional write: always succeeds."""
    def _op(current: Any) -> OpResult:
        return OpResult(success=True, value=new_value)
    return _op


def op_cas(expected: Any, desired: Any) -> Callable[[Any], OpResult]:
    """Compare-and-swap: succeeds only if the current value matches *expected*."""
    def _op(current: Any) -> OpResult:
        if current == expected:
            return OpResult(success=True, value=desired)
        return OpResult(success=False, value=current, not_applicable=True)
    return _op


def op_increment() -> Callable[[Any], OpResult]:
    """Increment an integer counter (treats None as 0)."""
    def _op(current: Any) -> OpResult:
        return OpResult(success=True, value=(current or 0) + 1)
    return _op
