set shell := ["sh", "-cu"]

# ─── All-up commands ────────────────────────────────────────────────

# List available recipes
default:
    @just --list --unsorted

# Format all code (Rust + C#)
[group('all')]
fmt: rust-fmt-fix csharp-fmt-fix

# Check formatting without modifying files
[group('all')]
fmt-check: rust-fmt csharp-fmt

# Lint all code
[group('all')]
lint: rust-lint

# Run all tests
[group('all')]
test: rust-test-all csharp-test tools-test

# Full CI-equivalent check: format, lint, test everything
[group('all')]
check: fmt-check lint test rust-bench-check

# ─── Rust ───────────────────────────────────────────────────────────

# Check Rust formatting
[group('rust')]
rust-fmt:
    cd rust && cargo fmt --check

# Apply Rust formatting fixes
[group('rust')]
rust-fmt-fix:
    cd rust && cargo fmt

# Run Clippy lints
[group('rust')]
rust-lint:
    cd rust && cargo clippy --workspace -- -D warnings

# Run Rust tests (default features)
[group('rust')]
rust-test:
    cd rust && cargo test --workspace

# Run turmoil deterministic simulation tests
[group('rust')]
rust-test-turmoil:
    cd rust && cargo test -p fast-caspaxos-simulation --features turmoil

# Run chaos seed-sweep tests
[group('rust')]
rust-test-chaos:
    cd rust && cargo test -p fast-caspaxos-simulation --test chaos_tests -- --nocapture

# Run all Rust tests (default + turmoil + chaos)
[group('rust')]
rust-test-all: rust-test rust-test-turmoil

# Compile-check benchmarks without running them
[group('rust')]
rust-bench-check:
    cd rust && cargo bench -p fast-caspaxos-core --no-run

# Run Criterion benchmarks
[group('rust')]
rust-bench:
    cd rust && cargo bench -p fast-caspaxos-core

# Full Rust CI check: format, lint, all tests, bench compile
[group('rust')]
rust-check: rust-fmt rust-lint rust-test-all rust-bench-check

# ─── C# ─────────────────────────────────────────────────────────────

# Check C# formatting without modifying files
[group('csharp')]
csharp-fmt:
    cd csharp && dotnet format FastCASPaxos.slnx --verify-no-changes

# Apply C# formatting fixes
[group('csharp')]
csharp-fmt-fix:
    cd csharp && dotnet format FastCASPaxos.slnx

# Build C# solution
[group('csharp')]
csharp-build:
    cd csharp && dotnet build FastCASPaxos.slnx

# Run C# tests
[group('csharp')]
csharp-test:
    cd csharp && dotnet test FastCASPaxos.slnx

# Full C# check: format, build, test
[group('csharp')]
csharp-check: csharp-fmt csharp-build csharp-test

# ─── TLA+ ───────────────────────────────────────────────────────────

# Run TLA+ model checker (safety + liveness)
[group('tla')]
tla-check:
    cd tla && pwsh -NoProfile -File Run-FastCASPaxos.ps1

# ─── Paper ──────────────────────────────────────────────────────────

# Build paper (ACM format)
[group('paper')]
paper-build:
    cd paper && pwsh -NoProfile -File build.ps1 acm

# Regenerate evaluation figures from benchmark data
[group('paper')]
paper-figures:
    cd paper && pwsh -NoProfile -File build.ps1 figures

# Run comparative benchmarks, generate figures, and rebuild paper
[group('paper')]
paper-eval *args:
    cd csharp && pwsh -NoProfile -File run_comparative_benchmarks.ps1 -PgfOutput ../paper/figures {{args}}
    cd paper && pwsh -NoProfile -File build.ps1 acm

# Clean paper build artifacts
[group('paper')]
paper-clean:
    cd paper && pwsh -NoProfile -File build.ps1 clean

# ─── Go tools ───────────────────────────────────────────────────────

# Run porcupine consistency checker tests
[group('tools')]
tools-test:
    cd tools/porcupine-checker && go test .

# Export Porcupine histories from Rust simulation tests (random seed each run)
[group('tools')]
rust-porcupine-export seed=`python3 -c "import random; print(random.randint(0, 2**64-1))"`:
    PORCUPINE_SEED={{seed}} cd rust && cargo test -p fast-caspaxos-simulation --test porcupine_tests --test chaos_tests -- --nocapture 2>&1 | grep -E '(^test |PORCUPINE_SEED|^running|^test result)'

# Run Porcupine linearizability checks: Rust tests → JSONL → Go checker
[group('tools')]
porcupine-check seed=`python3 -c "import random; print(random.randint(0, 2**64-1))"`:
    @echo "PORCUPINE_SEED={{seed}}"
    rm -f rust/target/tmp/porcupine-histories/*.jsonl
    PORCUPINE_SEED={{seed}} cd rust && cargo test -p fast-caspaxos-simulation --test porcupine_tests
    cd tools/porcupine-checker && go run . -history ../../rust/target/tmp/porcupine-histories
