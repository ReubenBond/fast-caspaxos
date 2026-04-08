MetaStore isn't solely for reconfiguration but its solely for metadata.

Fast CASPaxos doesn't eliminate the need for s coordinator altogether (just remove that part).

PaxosLease isn't a *simplified* paxos, its more complex. Modified would be more accurate.

"We call a " remove. "A consensus protocol is" is better.

"... are all leadered:" the subsequent description is specific to paxos (prepare and accept) Raft doesn't use that terminology.

"Designated coordinator" "leader" - mixed language. Stick with leader.

The mention of Rapid in the introduction doesn't have enough of a connection to Fast Paxos. Read the Rapid paper and explain briefly how Fast Paxos enables Rapid to achieve very fast, fault tolerant, low contention consensus on membership changes without a predesignated coordinator.

CASPaxos "with compare-and-swap semantics" is unnecessary and not strictly accurate.

1 RTT acronym not correctly introduced. Could use a footnote.

"We make the following contributions" are those actually contributions or is it just summarizing the table of contents? It would be better to explicitly do the latter and tell readers what each of the following sections are about.

Are we using "exposition" correctly here?

"Rely on sufficient periods of synchrony for liveness" is this correct and the best way to state this?

Paxos is leadered but it doesn't necessarily require failure detection before a new leader is chosen. In classic paxos, proposers race to both become a leader and then quickly propose a value, so the mention in 2.1 about needing to detect leader failure isn't really accurate. Paxos isn't prescriptive.

We mention that fast paxos has acceptors primed ahead of time in the abstract, introduction, and background - at least. Surely we dont need this much repetition.

Fast Paxos section says "when acceptors receive conflicting values, the protocol falls back", but acceptors dont communicate with each other directly. Its proposers that learn of the conflicting values.

"CASPaxos describes this as the "one-round trip optimization"" - no... we havent described piggybacking prepares at all yet, so this sentence is appearing too early in the paragraph. It hurts readability to order this before the description of the optimization.

Piggybacked prepare section seems to repeat itself (eg that the proposer learns the value after a successful accept).

Do a pass for repetition and keep it limited.  Some repetition is good,  since it can make the paper more understandable.

Confirm that piggybacked prepare is how multipaxos implements leaders.

Move CASPaxos cheatsheet earlier.

3 Fast CASPaxos.

"How concurrent values ubder a shared proposal are handled" maybe elaborate that they might be different values using the same ballot since they originate from distinct proposers.

"Leaderless 1 RTT commits" maybe change to "fast leaderless commits"