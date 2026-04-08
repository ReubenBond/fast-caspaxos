------------------------- MODULE MCFastCASPaxosSafety -------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS a1, a2, a3, a4, a5, g, a, x, b, ab

VARIABLES prepared,
          accepted,
          value,
          classicAcceptRequest,
          prepareResponse,
          acceptResponse,
          messageCount

MC == INSTANCE FastCASPaxos
WITH
    Acceptors <- {a1, a2, a3, a4, a5},
    Proposers <- {1, 2, 3},
    Values <- {g, a, x, b, ab},
    InitialValue <- g,
    NextValues <- [v \in {g, a, x, b, ab} |->
        IF v = g THEN {a, x, b}
        ELSE IF v = a THEN {ab}
        ELSE IF v = x THEN {ab}
        ELSE IF v = b THEN {ab}
        ELSE {}],
    VersionOf <- [v \in {g, a, x, b, ab} |->
        IF v = g THEN 0
        ELSE IF v = ab THEN 2
        ELSE 1],
    MaxRound <- 4,
    prepared <- prepared,
    accepted <- accepted,
    value <- value,
    classicAcceptRequest <- classicAcceptRequest,
    prepareResponse <- prepareResponse,
    acceptResponse <- acceptResponse,
    messageCount <- messageCount

Spec == MC!Spec
FairSpec == MC!FairSpec
AcceptorSymmetry == MC!AcceptorSymmetry
TypeOK == MC!TypeOK
OnlyOneAcceptedValuePerClassicBallot == MC!OnlyOneAcceptedValuePerClassicBallot
OnlyOneChosenValuePerBallot == MC!OnlyOneChosenValuePerBallot
AcceptedBallotPrepared == MC!AcceptedBallotPrepared
ChosenValuesReachable == MC!ChosenValuesReachable
ChosenValuesComparable == MC!ChosenValuesComparable
ViewWithoutClosedPrepareHistory == MC!ViewWithoutClosedPrepareHistory
Safety == MC!Safety
TransitionSafety == MC!TransitionSafety
Progress == MC!Progress
MsgBound == messageCount <= 6

=============================================================================
