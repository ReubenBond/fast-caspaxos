Intro:
Reference RAPID in intro when talking about cluster membership.
Focus less on log replication in abstract?
Classic paxos decides a value in two phases, but there is a third phase for learners.
Single round "of communication between a proposer and acceptors"
New proposer must acquire, not re-aquire.
Add mention about stable leaders for performance - see paxos made live paper which has similar framing. Reduces contention and communication.
Does fast paxos call them "fast rounds"? Double check.
"Piggybacked prepare" not defined in introduction - rephrase eg to "same optimization which enables stable leadership, piggybacking the successive prepare phase onto each accept phase, ..."?
"Where 1 RTT commits are attractive" - they are always attractive. Rephrase.

Move significant parts of introduction into background, since most of it is actually background.

Background:

"Quorums are of size" -- restate in terms of how many faults can be tolerated? Indicate that we choose cardinality based quorums for exposition but as (fast) flexible paxos shows, quorum requirements can be relaxed as long as intersection requirements are met.

"rely on eventual leader stability" - not quite true. Reword.

"Both Multi-Paxos and Raft are leadered" - also include classic paxos.

"Under the same ballot" -- remove. This is our specific impl (using ballots) whereas Fast Paxos paper doesn't use ballots in fast rounds.

Fast Round quorum requirements can be relaxed via Fast Flexible Paxos.

2.2 CASPaxos
"Value is committed" - Change to decided.
Reword description: proposer observes the the value with the highest associated ballot number (or the bottom value if none), modifies this value to produce an updated value, and proposes the updated value to acceptors to update the register.

No "applies an update function" language: read modify write framing instead. The proposer observes the value, potentially modifies it, and proposes an updated value.

"Learners" not defined.

Ballot value, not ballot number.

"Most recently committed value" - the value is not necessarily committed, its the most recently accepted value amongst the acceptors in the quorum. Its guaranteed that if any value was committed beforehand, that the proposer that proposed this value must have observed that value before proposing the current value. Use symbols to refer to the previously committed and current values.

"Two fields per register" maybe two metadata fields per register instead.

Don't refer to authors by name outside of the bibliography (eg Rystsov, Howard, etc) - reword those sentences to avoid the need.

Refer to Prepare and Accept using code block style/capitalization to make it clear.

"Learned the register's safe value" safe -> current. "for the next round" -> for the next Accept round?

"without a separate leader election mechanism" - remove, since it doesn't add clarity or really much value.

Figure 1. Accept only -> Accept.

"CASPaxos already piggybacks" - it supports piggybacking but it doesn't necessarily piggyback. That is described as an optimization, not mandatory behavior.

"The only additional change is ..." -> not quite true. For a proposer to propose an updated value, they must have learned the current safe to use value of the register as well as the prepared fast ballot value. Each time a proposer commits a value, they tell other proposers. This might sound restrictive, but note that by definition linearizability necessitates that the existing state of an object is observed before an update is applied. If Proposers and Acceptors are colocated, then a flag stored at the acceptors can be used to indicate if its accepted value and ballot have been committed to a quorum, and a Learn RPC can be used to update all persisted state of the acceptor (following the same rules as Accept). We could modify Accept to include the flag for completeness. Proposers colocated with acceptors can read the committed flag and corresponding value and ballot directly from their local acceptor.

The note about cardinality based quorums and flexible paxos etc should be moved to an earlier section of the paper.

3.1 Piggybacked Prepare:

The tuple based ballot structure used here is never introduced. We should introduce it when discussing Paxos and indicate that it's used for ease of exposition. We can show equivalence to scalar ballots as described in Paxos Made Live. Paxos Made Live says you choose ballots by assigning each replica an integer, i, between 0 and n-1 where n is the number of replicas in the system, and then calculating the ballot as `round * n + i`. This only works if the number of replicas in the system is fixed. Two-component ballots (i.e, `(round, i)`) are like choosing n = int.MaxValue. We can keep a succinct definition of this. We should probably make it at least partially a footnote so as not to distract. What else should be made into a footnote?

Add a note about safety: proposers only perform the Accept phase for a given ballot (classic or fast) if they have learned the safe to use register value corresponding to that ballot. This can be achieved in 3 ways:
1. By performing a prepare phase
2. By performing an accept with a piggybacked prepare
3. By learning a committed value and corresponding prepared ballot from another proposer.
Proposers will only perform an Accept with either a shared fast round ballot or a ballot corresponding to their proposer id.

Figure 2:
Regular CASPaxos doesn't require id > 0, only Fast CASPaxos does. 'id' and 'p' are used interchangeably: Consolidate them?
Consider using None instead of the bottom value symbol.
Ballot structure not specified in ballot box.

The paper as a whole should focus less on the adaptive mode selection and more on the core mechanism. We should add a subsection about the potential for adaptive type.

"Special any message" -> use styling to indicate that "any" is a value not merely the emglish word any.

"Mutli-component ballots avoid this" no, but we can use multi component ballots for a similar (but dual) purpose: to signify to other proposers that an acceptors value is part of a fast round (and therefore fast round quorum and value selection rules apply), and to prevent proposers from blindly overwriting an acceptors value if they have not observed the current "safe to use" value.

3.2:

Ensure RAPID capitalization is consistent with the rapid paper. The property is almost-everywhere agreement, not everywhere agreement. Briefly explain how rapid achieves almost everywhere agreement.

Don't include a quote from lamport and dont name lamport. The correctness argument is not quite right (it is a non sequitur). A better argument would be... what? The note about sending phase2b messages to all learners doesn't apply to fast paxos, which is what we are discussing here. If acceptors broadcast their responses to all proposers then proposers that proposed distinct values could all believe they successfully committed their value - which is incorrect.

3.3:

Fast ballot rules: "Any safe value" here means any value with equal-highest vote count. It doesn't include "retrying with a new value" since any new value must be based on one of the previously accepted values (in this case with equal highest vote count).

Consider removing conflict recovery walk-through to save space?

3.4:

Classic round ballots cannot be reused to propose different values. A given classic ballot must correspond to at most one value at acceptors.

4 correctness:
Do a pass to ensure this section is correct and consistent with other sections.
Don't restate quorum cardinalities. We should do a pass to ensure that they are minimized. Eg once in the paper body and once in the 2 cheat sheet figures.

Add inductive proof?

TLA+ is not a formal proof, necessarily. See other papers to see how they address TLA+ use.

5 Mode Selection:

Piggybacked Prepare is a key part of Fast CASPaxos but its still just an optimization. We could make this section much smaller or remove it entirely and collapse it into a different section. The table is useful to keep, even if we should revise it.