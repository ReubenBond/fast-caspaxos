------------------------ MODULE MCFastCASPaxosLiveness ------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS a1, a2, a3, a4, g, a, x

VARIABLES prepared,
          accepted,
          value,
          classicAcceptRequest,
          prepareResponse,
          acceptResponse,
          messageCount,
          done

MC == INSTANCE FastCASPaxos
WITH
    Acceptors <- {a1, a2, a3, a4},
    Proposers <- {1, 2},
    Values <- {g, a, x},
    InitialValue <- g,
    NextValues <- [v \in {g, a, x} |->
        IF v = g THEN {a, x}
        ELSE {}],
    VersionOf <- [v \in {g, a, x} |->
        IF v = g THEN 0
        ELSE 1],
    MaxRound <- 3,
    prepared <- prepared,
    accepted <- accepted,
    value <- value,
    classicAcceptRequest <- classicAcceptRequest,
    prepareResponse <- prepareResponse,
    acceptResponse <- acceptResponse,
    messageCount <- messageCount

BaseVars ==
    <<prepared, accepted, value, classicAcceptRequest,
      prepareResponse, acceptResponse, messageCount>>
Vars ==
    <<prepared, accepted, value, classicAcceptRequest,
      prepareResponse, acceptResponse, messageCount, done>>

Init ==
    /\ MC!Init
    /\ done = FALSE

CommitObserved == MC!CommittedBallots \ {MC!ZeroBallot} # {}

PrepareStep ==
    /\ ~done
    /\ MC!PrepareStep
    /\ done' = FALSE

AcceptReqStep ==
    /\ ~done
    /\ MC!AcceptReqStep
    /\ done' = FALSE

AcceptRspStep ==
    /\ ~done
    /\ MC!AcceptRspStep
    /\ done' = FALSE

CompleteAfterCommit ==
    \* Progress only asks for the first non-zero commit. After that point the
    \* wrapper can move to a terminal done state and stutter, which prevents TLC
    \* from exploring irrelevant post-success protocol histories.
    /\ ~done
    /\ CommitObserved
    /\ done' = TRUE
    /\ UNCHANGED BaseVars

Quiescent ==
    \* The base model has no explicit quiescent state, so the wrapper provides
    \* one after success to keep the liveness run finite.
    /\ done
    /\ UNCHANGED Vars

Next ==
    \/ PrepareStep
    \/ AcceptReqStep
    \/ AcceptRspStep
    \/ CompleteAfterCommit
    \/ Quiescent

Spec ==
    /\ Init
    /\ [][Next]_Vars

FairSpec ==
    /\ Spec
    /\ WF_Vars(PrepareStep)
    /\ WF_Vars(AcceptReqStep)
    /\ WF_Vars(AcceptRspStep)
    /\ WF_Vars(CompleteAfterCommit)

TypeOK ==
    /\ MC!TypeOK
    /\ done \in BOOLEAN

OnlyOneAcceptedValuePerClassicBallot == MC!OnlyOneAcceptedValuePerClassicBallot
OnlyOneChosenValuePerBallot == MC!OnlyOneChosenValuePerBallot
AcceptedBallotPrepared == MC!AcceptedBallotPrepared
ChosenValuesReachable == MC!ChosenValuesReachable
ChosenValuesComparable == MC!ChosenValuesComparable
Safety == MC!Safety
TransitionSafety == MC!TransitionSafety
Progress == <>done
MsgBound == messageCount <= 10

=============================================================================
