#!/usr/bin/env bash
# Runs every `action mutant*` in a Quint spec and requires each to violate an invariant.
#
# Usage: check-mutants.sh <spec.qnt> <main-module> <default-invariant> [extra quint run args...]
#
# A mutant may name the invariant it must break with a comment on the line directly above it:
#   // breaks: durability
#   action mutantFoo = { ... }
# Otherwise it must break <default-invariant>.
#
# A mutant counts as caught only when quint reports `[violation]` and `Invariant violated`.
# Any other failure (unknown step, parse or type error, crash) fails the check, so a typo
# or a broken spec can never pass as a caught mutant.
set -uo pipefail

spec=$1 main=$2 default_inv=$3
shift 3

# Scope mutant discovery to `module $main { ... }` when it defines `action mutant*`
# directly (supporting multi-module MC files); otherwise scan the whole spec file.
module_block=$(awk -v m="$main" '
  $0 ~ "^[[:space:]]*module[[:space:]]+" m "([[:space:]]|\\{|$)" { in_mod = 1 }
  in_mod {
    print
    n_open = gsub(/\{/, "{")
    n_close = gsub(/\}/, "}")
    depth += n_open - n_close
    if (seen_open && depth <= 0) { in_mod = 0 }
    if (n_open > 0) { seen_open = 1 }
  }
' "$spec")

mapfile -t mutants < <(grep -oP '^\s*action\s+\Kmutant\w+' <<<"$module_block")
source_text=$module_block
if [ ${#mutants[@]} -eq 0 ]; then
  mapfile -t mutants < <(grep -oP '^\s*action\s+\Kmutant\w+' "$spec")
  source_text=$(cat "$spec")
fi
if [ ${#mutants[@]} -eq 0 ]; then
  echo "::error::no mutants found in $spec"
  exit 1
fi

failed=0
for m in "${mutants[@]}"; do
  inv=$(grep -B1 -P "^\s*action\s+$m\b" <<<"$source_text" | grep -oP '//\s*breaks:\s*\K\w+' || true)
  inv=${inv:-$default_inv}
  out=$(quint run "$spec" --main="$main" --step="$m" --invariant="$inv" "$@" 2>&1)
  code=$?
  if [ $code -ne 0 ] && grep -q '\[violation\]' <<<"$out" && grep -q 'Invariant violated' <<<"$out"; then
    echo "$m: violates $inv, as expected"
  elif [ $code -eq 0 ]; then
    echo "::error::$m was not caught by $inv"
    failed=1
  else
    echo "::error::$m failed without an invariant violation (exit $code):"
    echo "$out" | tail -n 20
    failed=1
  fi
done
exit $failed
