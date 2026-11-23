#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

expect_violation() {
  local name="$1"
  local model="$2"
  local init_action="$3"
  local step_action="$4"
  local invariant="$5"
  local max_samples="${6:-3000}"
  local max_steps="${7:-60}"

  echo "=== Checking mutant: ${name} (model=${model}, invariant=${invariant}) ==="
  if quint run FastCASPaxosMC.qnt \
    --main "${model}" \
    --init "${init_action}" \
    --step "${step_action}" \
    --invariant "${invariant}" \
    --max-samples "${max_samples}" \
    --max-steps "${max_steps}" \
    --n-threads 2 \
    --seed 17 \
    --verbosity 1; then
    echo "ERROR: mutant ${name} failed to violate ${invariant}" >&2
    exit 1
  fi
  echo "OK: mutant ${name} violated ${invariant} as expected."
}

expect_violation "mutantFloorOnlyHeldKeys" "FastCASPaxosReconfigReplace" "init" "mutantFloorOnlyHeldKeys" "chosenChain" 3000 60
expect_violation "mutantPhase1InNewConfigBeforeComplete" "FastCASPaxosReconfigDisjoint" "init" "mutantPhase1InNewConfigBeforeComplete" "chosenChain" 3000 60
expect_violation "mutantHandoffSkipsNewConfigQuorum" "FastCASPaxosReconfigReplace" "init" "mutantHandoffSkipsNewConfigQuorum" "chosenChain" 3000 60
expect_violation "mutantImplicitFastInNewConfig" "FastCASPaxosReconfigReplace" "init" "mutantImplicitFastInNewConfig" "acceptedExtendsChosen" 3000 60
expect_violation "mutantNewVoterBeforeFloor" "FastCASPaxosReconfigReplace" "initMutantNewVoterBeforeFloor" "step" "noPreJoinAcceptance" 500 20
expect_violation "mutantSkipSingleChoice" "FastCASPaxosReconfigReplace" "init" "mutantSkipSingleChoice" "splitChosenChain" 3000 60

echo "=== Checking mutant: mutantUnscopedFastTally (directed 4-voter counterexample) ==="
quint test FastCASPaxosTests.qnt --main FastCASPaxosReconfig4VoterTests --max-samples 1 --seed 1
echo "All 7 reconfiguration mutants killed as expected."
