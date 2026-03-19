use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fast_caspaxos_core::acceptor::{AcceptorEngine, AcceptorHandler};
use fast_caspaxos_core::ballot::Ballot;
use fast_caspaxos_core::messages::*;
use fast_caspaxos_core::quorum;

// ---------------------------------------------------------------------------
// Noop handlers — discard all callbacks so we benchmark engine throughput only
// ---------------------------------------------------------------------------

struct NoopAcceptorHandler;

impl AcceptorHandler<i32, u32> for NoopAcceptorHandler {
    fn on_prepare_promised(&mut self, _: PreparePromise<i32, u32>) {}
    fn on_prepare_rejected(&mut self, _: PrepareRejection<i32, u32>) {}
    fn on_accept_accepted(&mut self, _: AcceptAccepted<u32>) {}
    fn on_accept_rejected(&mut self, _: AcceptRejected<u32>) {}
}

// ---------------------------------------------------------------------------
// Ballot micro-benchmarks
// ---------------------------------------------------------------------------

fn bench_ballot_comparison(c: &mut Criterion) {
    let a = Ballot::new(100, 5);
    let b = Ballot::new(100, 3);
    c.bench_function("ballot_comparison", |iter| {
        iter.iter(|| black_box(a) > black_box(b));
    });
}

fn bench_ballot_max(c: &mut Criterion) {
    let a = Ballot::new(50, 2);
    let b = Ballot::new(75, 1);
    c.bench_function("ballot_max", |iter| {
        iter.iter(|| Ballot::max(black_box(a), black_box(b)));
    });
}

// ---------------------------------------------------------------------------
// Acceptor engine benchmarks
// ---------------------------------------------------------------------------

fn bench_acceptor_prepare(c: &mut Criterion) {
    c.bench_function("acceptor_prepare_100", |b| {
        b.iter(|| {
            let mut engine = AcceptorEngine::<i32, u32>::new(0_u32, None);
            let mut handler = NoopAcceptorHandler;
            for round in 1..=100_u32 {
                engine.prepare(
                    &mut handler,
                    PrepareRequest {
                        ballot: Ballot::new(round, 1),
                    },
                );
            }
        });
    });
}

fn bench_acceptor_accept(c: &mut Criterion) {
    c.bench_function("acceptor_accept_100", |b| {
        b.iter(|| {
            let mut engine = AcceptorEngine::<i32, u32>::new(0_u32, None);
            let mut handler = NoopAcceptorHandler;
            for round in 1..=100_u32 {
                let ballot = Ballot::new(round, 1);
                engine.prepare(
                    &mut handler,
                    PrepareRequest { ballot },
                );
                engine.accept(
                    &mut handler,
                    AcceptRequest {
                        ballot,
                        value: round as i32,
                        next_ballot_to_prepare: None,
                    },
                );
            }
        });
    });
}

// ---------------------------------------------------------------------------
// Quorum check benchmarks
// ---------------------------------------------------------------------------

fn bench_quorum_check(c: &mut Criterion) {
    let fast = Ballot::initial_fast();
    let classic = Ballot::initial_classic(1);
    c.bench_function("quorum_fast_check", |b| {
        b.iter(|| quorum::has_quorum(black_box(fast), black_box(4), black_box(5)));
    });
    c.bench_function("quorum_classic_check", |b| {
        b.iter(|| quorum::has_quorum(black_box(classic), black_box(3), black_box(5)));
    });
}

criterion_group!(
    benches,
    bench_ballot_comparison,
    bench_ballot_max,
    bench_acceptor_prepare,
    bench_acceptor_accept,
    bench_quorum_check,
);
criterion_main!(benches);
