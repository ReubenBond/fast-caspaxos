---------------------------- MODULE FastCASPaxos ----------------------------
EXTENDS Naturals, FiniteSets, TLC

\* A shorter Fast CASPaxos model in the style of the upstream CASPaxos.tla.
\* It keeps only the core mechanics:
\* - implicit proposers and bounded response slots instead of message history
\* - fast ballots as tuple ballots with proposer = 0
\* - classic recovery through prepare responses
\* - piggyback preparation of the next fast or proposer-owned classic ballot
\*   after every successful accept
\*
\* To stay small, fast and prepared classic proposals mutate a value that could
\* have been learned locally from the ballot that piggyback-prepared them, while
\* classic recovery may either re-propose the recovered value or choose a
\* successor of it.

CONSTANTS Values, Acceptors, Proposers, InitialValue, NextValues, VersionOf, MaxRound

ASSUME /\ IsFiniteSet(Values)
       /\ IsFiniteSet(Acceptors)
       /\ IsFiniteSet(Proposers)
       /\ Values # {}
       /\ Acceptors # {}
       /\ Proposers # {}
       /\ InitialValue \in Values
       /\ Proposers \subseteq Nat \ {0}
       /\ MaxRound \in Nat \ {0}
       /\ NextValues \in [Values -> SUBSET Values]
       /\ VersionOf \in [Values -> Nat]
       /\ \A v \in Values : NextValues[v] \subseteq Values
       /\ \A base \in Values :
            \A new \in NextValues[base] :
                VersionOf[base] < VersionOf[new]

ZeroBallot == [round |-> 0, proposer |-> 0]
InitialFastBallot == [round |-> 1, proposer |-> 0]

FastRounds == 1..MaxRound
ClassicRounds == 2..MaxRound

FastBallots ==
    {
        [round |-> r, proposer |-> 0] :
            r \in FastRounds
    }

ClassicBallots ==
    {
        [round |-> r, proposer |-> p] :
            r \in ClassicRounds,
            p \in Proposers
    }

NonZeroBallots == FastBallots \cup ClassicBallots
Ballots == NonZeroBallots \cup {ZeroBallot}
AcceptorSymmetry == Permutations(Acceptors)

IsFastBallot(b) == b \in FastBallots

IsClassicBallot(b) == b \in ClassicBallots

BallotLT(left, right) ==
    \/ left.round < right.round
    \/ /\ left.round = right.round
       /\ left.proposer < right.proposer

BallotLE(left, right) == left = right \/ BallotLT(left, right)

MaxBallot(setOfBallots) ==
    CHOOSE ballot \in setOfBallots :
        \A other \in setOfBallots : BallotLE(other, ballot)

FastPiggybackNextBallots(ballot) ==
    IF ballot.round < MaxRound
        THEN {[round |-> ballot.round + 1, proposer |-> 0]}
        ELSE {ZeroBallot}

ClassicPiggybackNextBallots(ballot) ==
    IF ballot.round < MaxRound
        THEN {[round |-> ballot.round + 1, proposer |-> ballot.proposer]}
        ELSE {ZeroBallot}

AcceptorCount == Cardinality(Acceptors)
ClassicQuorumSize == (AcceptorCount \div 2) + 1
FastQuorumSize == ((3 * AcceptorCount) + 3) \div 4
\* A classic recovery quorum only needs enough matching fast votes to guarantee
\* that some fast quorum could have supported that value. This count is the
\* minimum fast/classic quorum intersection size, so we can test witnesses by
\* cardinality instead of enumerating fast-quorum subsets.
FastWitnessThreshold == FastQuorumSize + ClassicQuorumSize - AcceptorCount

ClassicQuorums ==
    {Q \in SUBSET(Acceptors) : Cardinality(Q) = ClassicQuorumSize}

QuorumSize(ballot) ==
    IF IsFastBallot(ballot)
        THEN FastQuorumSize
        ELSE ClassicQuorumSize

PrepareResponseSlot ==
    [seen : BOOLEAN, accepted : Ballots, val : Values]

ClassicAcceptRequestSlot ==
    [sent : BOOLEAN, newVal : Values, nextBal : Ballots]

AcceptResponseSlot ==
    [seen : BOOLEAN, val : Values, preparedNext : Ballots]

NoPrepareResponse ==
    [seen |-> FALSE, accepted |-> ZeroBallot, val |-> InitialValue]

NoClassicAcceptRequest ==
    [sent |-> FALSE, newVal |-> InitialValue, nextBal |-> ZeroBallot]

NoAcceptResponse ==
    [seen |-> FALSE, val |-> InitialValue, preparedNext |-> ZeroBallot]

BlankPrepareResponses ==
    [ballot \in ClassicBallots |-> [a \in Acceptors |-> NoPrepareResponse]]

BlankClassicAcceptRequests ==
    [ballot \in ClassicBallots |-> NoClassicAcceptRequest]

BlankAcceptResponses ==
    [ballot \in NonZeroBallots |-> [a \in Acceptors |-> NoAcceptResponse]]

VARIABLES prepared,
          accepted,
          value,
          classicAcceptRequest,
          prepareResponse,
          acceptResponse,
          messageCount

vars ==
    <<prepared, accepted, value, classicAcceptRequest,
      prepareResponse, acceptResponse, messageCount>>

\* Once a classic accept request is installed, the preparatory slot history for
\* that ballot can no longer enable any new behavior. The safety wrapper uses
\* this projection to merge states that differ only by closed recovery history.
VisiblePrepareResponses ==
    [ballot \in ClassicBallots |->
        IF classicAcceptRequest[ballot].sent
            THEN [a \in Acceptors |-> NoPrepareResponse]
            ELSE prepareResponse[ballot]]

ViewWithoutClosedPrepareHistory ==
    <<prepared, accepted, value, classicAcceptRequest,
      VisiblePrepareResponses, acceptResponse, messageCount>>

RECURSIVE ReachablePairs(_)

TypeOK ==
    /\ prepared \in [Acceptors -> Ballots]
    /\ accepted \in [Acceptors -> Ballots]
    /\ value \in [Acceptors -> Values]
    /\ classicAcceptRequest \in [ClassicBallots -> ClassicAcceptRequestSlot]
    /\ prepareResponse \in [ClassicBallots -> [Acceptors -> PrepareResponseSlot]]
    /\ acceptResponse \in [NonZeroBallots -> [Acceptors -> AcceptResponseSlot]]
    /\ messageCount \in Nat

Init ==
    /\ prepared = [a \in Acceptors |-> InitialFastBallot]
    /\ accepted = [a \in Acceptors |-> ZeroBallot]
    /\ value = [a \in Acceptors |-> InitialValue]
    /\ classicAcceptRequest = BlankClassicAcceptRequests
    /\ prepareResponse = BlankPrepareResponses
    /\ acceptResponse = BlankAcceptResponses
    /\ messageCount = 0

PrepareResponders(ballot) ==
    {a \in Acceptors : prepareResponse[ballot][a].seen}

PrepareResponseQuorums(ballot) ==
    {Q \in ClassicQuorums : Q \subseteq PrepareResponders(ballot)}

AcceptedByIn(responseSlots, ballot, chosenVal) ==
    {a \in Acceptors :
        /\ responseSlots[ballot][a].seen
        /\ responseSlots[ballot][a].val = chosenVal}

PreparedValuesByIn(responseSlots, sourceBallot, nextBallot) ==
    {v \in Values :
        Cardinality({
            a \in Acceptors :
                /\ responseSlots[sourceBallot][a].seen
                /\ responseSlots[sourceBallot][a].val = v
                /\ responseSlots[sourceBallot][a].preparedNext = nextBallot
        }) >= QuorumSize(sourceBallot)}

AcceptedByQuorumIn(responseSlots, ballot, chosenVal) ==
    Cardinality(AcceptedByIn(responseSlots, ballot, chosenVal)) >= QuorumSize(ballot)

ChosenValuesAtIn(responseSlots, ballot) ==
    IF ballot = ZeroBallot
        THEN {InitialValue}
        ELSE {v \in Values : AcceptedByQuorumIn(responseSlots, ballot, v)}

CommittedBallotsIn(responseSlots) ==
    {b \in Ballots : ChosenValuesAtIn(responseSlots, b) # {}}

AcceptedBy(ballot, chosenVal) == AcceptedByIn(acceptResponse, ballot, chosenVal)

PreparedValuesBy(sourceBallot, nextBallot) ==
    PreparedValuesByIn(acceptResponse, sourceBallot, nextBallot)

AcceptedByQuorum(ballot, chosenVal) ==
    AcceptedByQuorumIn(acceptResponse, ballot, chosenVal)

ChosenValuesAt(ballot) == ChosenValuesAtIn(acceptResponse, ballot)

CommittedBallots == CommittedBallotsIn(acceptResponse)

ChosenValue(ballot) == CHOOSE v \in ChosenValuesAt(ballot) : TRUE

CommittedBefore(ballot) ==
    {b \in CommittedBallots : BallotLT(b, ballot)}

LatestCommittedBallotBefore(ballot) ==
    MaxBallot(CommittedBefore(ballot))

LatestCommittedValueBefore(ballot) ==
    ChosenValue(LatestCommittedBallotBefore(ballot))

RoundHasAccept(round) ==
    \E ballot \in NonZeroBallots :
        /\ ballot.round = round
        /\ \E a \in Acceptors : acceptResponse[ballot][a].seen

PiggybackSourceBallots(ballot) ==
    {source \in NonZeroBallots :
        ballot \in
            IF IsFastBallot(source)
                THEN FastPiggybackNextBallots(source)
                ELSE ClassicPiggybackNextBallots(source)}

PreparedBaseValues(ballot) ==
    UNION {PreparedValuesBy(source, ballot) : source \in PiggybackSourceBallots(ballot)}

\* A prepared ballot may only reuse the value that some proposer could have
\* learned by committing one of the source ballots that could have piggyback-
\* prepared it.
FastBallotBaseValues(ballot) ==
    IF ballot = InitialFastBallot
        THEN {InitialValue}
    ELSE IF IsFastBallot(ballot)
        THEN PreparedBaseValues(ballot)
        ELSE {}

FastBallotOpened(ballot) ==
    /\ IsFastBallot(ballot)
    /\ FastBallotBaseValues(ballot) # {}

PreparedClassicBaseValues(ballot) ==
    IF IsClassicBallot(ballot)
        THEN PreparedBaseValues(ballot)
        ELSE {}

ClassicBallotPrepared(ballot) ==
    /\ IsClassicBallot(ballot)
    /\ PreparedClassicBaseValues(ballot) # {}

ClassicBallotRecoverable(ballot) ==
    /\ IsClassicBallot(ballot)
    /\ ballot.round > 1
    /\ RoundHasAccept(ballot.round - 1)

HighestAcceptedBallot(Q, ballot) ==
    MaxBallot({prepareResponse[ballot][a].accepted : a \in Q})

HighestAcceptedResponders(Q, ballot) ==
    {a \in Q :
        prepareResponse[ballot][a].accepted = HighestAcceptedBallot(Q, ballot)}

HighestAcceptedValues(Q, ballot) ==
    {prepareResponse[ballot][a].val : a \in HighestAcceptedResponders(Q, ballot)}

FastVoteWitness(Q, recoveryBallot, acceptedBallot, chosenVal) ==
    Cardinality({
        a \in Q :
            /\ prepareResponse[recoveryBallot][a].accepted = acceptedBallot
            /\ prepareResponse[recoveryBallot][a].val = chosenVal
    }) >= FastWitnessThreshold

RecoverableBases(Q, ballot) ==
    LET highest == HighestAcceptedBallot(Q, ballot)
        valuesAtHighest == HighestAcceptedValues(Q, ballot)
        fastWinners ==
            {v \in valuesAtHighest : FastVoteWitness(Q, ballot, highest, v)}
    IN
        IF highest = ZeroBallot
            THEN {InitialValue}
        ELSE IF \/ IsClassicBallot(highest)
                \/ Cardinality(valuesAtHighest) = 1
            THEN valuesAtHighest
        ELSE IF fastWinners # {}
            THEN fastWinners
            ELSE {LatestCommittedValueBefore(highest)}

ClassicAcceptValues(Q, ballot) ==
    UNION {{base} \cup NextValues[base] : base \in RecoverableBases(Q, ballot)}

FastAcceptValues(ballot) ==
    IF FastBallotOpened(ballot)
        THEN UNION {NextValues[base] : base \in FastBallotBaseValues(ballot)}
        ELSE {}

PreparedClassicAcceptValues(ballot) ==
    IF ClassicBallotPrepared(ballot)
        THEN UNION {NextValues[base] : base \in PreparedClassicBaseValues(ballot)}
        ELSE {}

ReachablePairs(steps) ==
    IF steps = 0
        THEN {<<v, v>> : v \in Values}
        ELSE LET prev == ReachablePairs(steps - 1)
             IN prev \cup {
                    <<from, to>> \in Values \X Values :
                        \E mid \in Values :
                            /\ <<from, mid>> \in prev
                            /\ to \in NextValues[mid]}

Reachable(from, to) ==
    <<from, to>> \in ReachablePairs(Cardinality(Values) - 1)

PrepareRsp(a, ballot) ==
    /\ ClassicBallotRecoverable(ballot)
    \* Recovery reads stop once the ballot has been committed to a classic
    \* accept request; later prepare responses would only add inert history.
    /\ ~classicAcceptRequest[ballot].sent
    /\ ~prepareResponse[ballot][a].seen
    \* A piggyback-prepared acceptor still answers an explicit prepare for that
    \* same ballot; only strictly higher promises suppress the response.
    /\ BallotLE(prepared[a], ballot)
    /\ prepared' = [prepared EXCEPT ![a] = ballot]
    /\ prepareResponse' = [prepareResponse EXCEPT ![ballot][a] =
        [seen |-> TRUE, accepted |-> accepted[a], val |-> value[a]]]
    /\ messageCount' = messageCount + 1
    /\ UNCHANGED <<accepted, value, classicAcceptRequest, acceptResponse>>

InstallClassicAcceptRequest(ballot, chosenVal, nextBal) ==
    /\ ~classicAcceptRequest[ballot].sent
    /\ nextBal \in ClassicPiggybackNextBallots(ballot)
    /\ classicAcceptRequest' = [classicAcceptRequest EXCEPT ![ballot] =
        [sent |-> TRUE, newVal |-> chosenVal, nextBal |-> nextBal]]
    /\ messageCount' = messageCount + 1
    /\ UNCHANGED <<prepared, accepted, value, prepareResponse, acceptResponse>>

PreparedClassicAcceptReq(ballot, chosenVal, nextBal) ==
    /\ chosenVal \in PreparedClassicAcceptValues(ballot)
    /\ InstallClassicAcceptRequest(ballot, chosenVal, nextBal)

RecoveringClassicAcceptReq(ballot, Q, chosenVal, nextBal) ==
    /\ ClassicBallotRecoverable(ballot)
    /\ Q \in PrepareResponseQuorums(ballot)
    /\ chosenVal \in ClassicAcceptValues(Q, ballot)
    /\ InstallClassicAcceptRequest(ballot, chosenVal, nextBal)

AcceptRsp(a, ballot, chosenVal, nextBal) ==
    /\ ~acceptResponse[ballot][a].seen
    /\ \/ nextBal = ZeroBallot
       \/ BallotLT(ballot, nextBal)
    /\ BallotLE(prepared[a], ballot)
    /\ BallotLT(accepted[a], ballot)
    /\ prepared' = [prepared EXCEPT ![a] =
        IF nextBal = ZeroBallot
            THEN ballot
            ELSE nextBal]
    /\ accepted' = [accepted EXCEPT ![a] = ballot]
    /\ value' = [value EXCEPT ![a] = chosenVal]
    /\ acceptResponse' = [acceptResponse EXCEPT ![ballot][a] =
        [seen |-> TRUE, val |-> chosenVal, preparedNext |-> nextBal]]
    /\ messageCount' = messageCount + 1
    /\ UNCHANGED <<classicAcceptRequest, prepareResponse>>

\* Responses directly model delivery of one admissible fast proposal to one
\* acceptor, which keeps the fast path concurrent without retaining request
\* history in the state fingerprint.
FastAcceptRsp(a, ballot, chosenVal, nextBal) ==
    /\ chosenVal \in FastAcceptValues(ballot)
    /\ nextBal \in FastPiggybackNextBallots(ballot)
    /\ AcceptRsp(a, ballot, chosenVal, nextBal)

ClassicAcceptRsp(a, ballot) ==
    LET request == classicAcceptRequest[ballot]
    IN /\ request.sent
       /\ request.nextBal \in ClassicPiggybackNextBallots(ballot)
       /\ AcceptRsp(a, ballot, request.newVal, request.nextBal)

PrepareStep ==
    \E ballot \in ClassicBallots, a \in Acceptors :
        PrepareRsp(a, ballot)

AcceptReqStep ==
    \/ \E ballot \in ClassicBallots :
            \E chosenVal \in PreparedClassicAcceptValues(ballot) :
                \E nextBal \in ClassicPiggybackNextBallots(ballot) :
                    PreparedClassicAcceptReq(ballot, chosenVal, nextBal)
    \/ \E ballot \in ClassicBallots :
            \E Q \in PrepareResponseQuorums(ballot) :
                \E chosenVal \in ClassicAcceptValues(Q, ballot) :
                    \E nextBal \in ClassicPiggybackNextBallots(ballot) :
                        RecoveringClassicAcceptReq(ballot, Q, chosenVal, nextBal)

AcceptRspStep ==
    \/ \E ballot \in FastBallots :
            \E a \in Acceptors :
                \E chosenVal \in FastAcceptValues(ballot) :
                    \E nextBal \in FastPiggybackNextBallots(ballot) :
                        FastAcceptRsp(a, ballot, chosenVal, nextBal)
    \/ \E ballot \in ClassicBallots :
            \E a \in Acceptors :
                ClassicAcceptRsp(a, ballot)

Next ==
    \/ PrepareStep
    \/ AcceptReqStep
    \/ AcceptRspStep

Spec == Init /\ [][Next]_vars

FairSpec ==
    /\ Spec
    /\ WF_vars(PrepareStep)
    /\ WF_vars(AcceptReqStep)
    /\ WF_vars(AcceptRspStep)

AcceptedValuesAt(ballot) ==
    IF ballot = ZeroBallot
        THEN {InitialValue}
        ELSE {v \in Values :
                \E a \in Acceptors :
                    /\ acceptResponse[ballot][a].seen
                    /\ acceptResponse[ballot][a].val = v}

OnlyOneAcceptedValuePerClassicBallot ==
    \A ballot \in ClassicBallots :
        Cardinality(AcceptedValuesAt(ballot)) <= 1

OnlyOneChosenValuePerBallot ==
    \A ballot \in Ballots :
        Cardinality(ChosenValuesAt(ballot)) <= 1

AcceptedBallotPrepared ==
    \A a \in Acceptors :
        BallotLE(accepted[a], prepared[a])

PrepareResponsesPrecedePromisedBallot ==
    \A ballot \in ClassicBallots, a \in Acceptors :
        prepareResponse[ballot][a].seen =>
            BallotLT(prepareResponse[ballot][a].accepted, ballot)

ClassicAcceptResponsesHaveRequests ==
    \A ballot \in ClassicBallots, a \in Acceptors :
        acceptResponse[ballot][a].seen =>
            classicAcceptRequest[ballot].sent

PreparedNextBallotsAdvance ==
    \A ballot \in NonZeroBallots, a \in Acceptors :
        /\ acceptResponse[ballot][a].seen
        /\ acceptResponse[ballot][a].preparedNext # ZeroBallot
        => BallotLT(ballot, acceptResponse[ballot][a].preparedNext)

ChosenValuesReachable ==
    \A ballot \in CommittedBallots :
        \A chosenVal \in ChosenValuesAt(ballot) :
            Reachable(InitialValue, chosenVal)

ChosenValuesComparable ==
    \A left, right \in CommittedBallots :
        \A leftVal \in ChosenValuesAt(left) :
            \A rightVal \in ChosenValuesAt(right) :
                \/ Reachable(leftVal, rightVal)
                \/ Reachable(rightVal, leftVal)

NewPrepareResponses ==
    {<<ballot, a>> \in ClassicBallots \X Acceptors :
        /\ ~prepareResponse[ballot][a].seen
        /\ prepareResponse'[ballot][a].seen}

NewClassicAcceptRequests ==
    {ballot \in ClassicBallots :
        /\ ~classicAcceptRequest[ballot].sent
        /\ classicAcceptRequest'[ballot].sent}

NewAcceptResponses ==
    {<<ballot, a>> \in NonZeroBallots \X Acceptors :
        /\ ~acceptResponse[ballot][a].seen
        /\ acceptResponse'[ballot][a].seen}

TransitionItemsAdded ==
    Cardinality(NewPrepareResponses)
    + Cardinality(NewClassicAcceptRequests)
    + Cardinality(NewAcceptResponses)

SafetyTransitionOK ==
    \* The expensive history properties are checked on the one slot installed by
    \* this transition instead of being recomputed from the full accumulated
    \* slot state as invariants on every explored state.
    /\ \A ballot \in ClassicBallots :
            classicAcceptRequest[ballot].sent =>
                classicAcceptRequest'[ballot] = classicAcceptRequest[ballot]
    /\ \A ballot \in ClassicBallots, a \in Acceptors :
            prepareResponse[ballot][a].seen =>
                prepareResponse'[ballot][a] = prepareResponse[ballot][a]
    /\ \A ballot \in NonZeroBallots, a \in Acceptors :
            acceptResponse[ballot][a].seen =>
                acceptResponse'[ballot][a] = acceptResponse[ballot][a]
    /\ TransitionItemsAdded <= 1
    /\ messageCount' = messageCount + TransitionItemsAdded
    /\ \A pair \in NewPrepareResponses :
            LET ballot == pair[1]
                a == pair[2]
                rsp == prepareResponse'[ballot][a]
            IN /\ ClassicBallotRecoverable(ballot)
               /\ ~classicAcceptRequest[ballot].sent
               /\ BallotLE(prepared[a], ballot)
               /\ rsp.accepted = accepted[a]
               /\ rsp.val = value[a]
               /\ BallotLT(rsp.accepted, ballot)
    /\ \A ballot \in NewClassicAcceptRequests :
            LET request == classicAcceptRequest'[ballot]
            IN /\ request.nextBal \in ClassicPiggybackNextBallots(ballot)
               /\ \/ request.newVal \in PreparedClassicAcceptValues(ballot)
                  \/ \E Q \in PrepareResponseQuorums(ballot) :
                        request.newVal \in ClassicAcceptValues(Q, ballot)
    /\ \A pair \in NewAcceptResponses :
            LET ballot == pair[1]
                a == pair[2]
                rsp == acceptResponse'[ballot][a]
                oldChosen == ChosenValuesAtIn(acceptResponse, ballot)
                newChosen == ChosenValuesAtIn(acceptResponse', ballot)
                newlyChosen == newChosen \ oldChosen
            IN /\ BallotLE(prepared[a], ballot)
               /\ BallotLT(accepted[a], ballot)
               /\ (rsp.preparedNext = ZeroBallot
                   \/ BallotLT(ballot, rsp.preparedNext))
               /\ (\/ /\ IsFastBallot(ballot)
                      /\ rsp.val \in FastAcceptValues(ballot)
                      /\ rsp.preparedNext \in FastPiggybackNextBallots(ballot)
                   \/ /\ IsClassicBallot(ballot)
                      /\ classicAcceptRequest[ballot].sent
                      /\ rsp.val = classicAcceptRequest[ballot].newVal
                      /\ rsp.preparedNext = classicAcceptRequest[ballot].nextBal)
               /\ (~IsClassicBallot(ballot)
                   \/ \A prior \in Acceptors :
                          ~acceptResponse[ballot][prior].seen
                          \/ acceptResponse[ballot][prior].val = rsp.val)
               /\ Cardinality(newChosen) <= 1
               /\ \A v \in newlyChosen :
                     /\ Reachable(InitialValue, v)
                     /\ \A priorBallot \in CommittedBallotsIn(acceptResponse) :
                           \A oldVal \in ChosenValuesAtIn(acceptResponse, priorBallot) :
                               \/ Reachable(v, oldVal)
                               \/ Reachable(oldVal, v)

Safety ==
    /\ TypeOK
    /\ OnlyOneAcceptedValuePerClassicBallot
    /\ OnlyOneChosenValuePerBallot
    /\ AcceptedBallotPrepared
    /\ PrepareResponsesPrecedePromisedBallot
    /\ ClassicAcceptResponsesHaveRequests
    /\ PreparedNextBallotsAdvance
    /\ ChosenValuesReachable
    /\ ChosenValuesComparable

TransitionSafety ==
    [][Next => SafetyTransitionOK]_vars

Progress == <>(CommittedBallots \ {ZeroBallot} # {})

THEOREM Spec => []Safety
=============================================================================
