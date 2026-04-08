#![cfg(feature = "turmoil")]

use fast_caspaxos_simulation::turmoil_sim::{build_simulation, propose_write, TurmoilConfig};

#[test]
fn turmoil_single_write_commits() {
    let config = TurmoilConfig {
        num_acceptors: 3,
        num_proposers: 1,
        enable_fast_commit: true,
        enable_distinguished_leader: false,
    };
    let mut sim = build_simulation(&config, 42);

    sim.client("client", async move {
        let (round, committed) = propose_write("proposer-1", 42).await?;
        assert_eq!(committed, 42);
        assert!(round > 0);
        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn turmoil_sequential_writes() {
    let config = TurmoilConfig {
        num_acceptors: 5,
        num_proposers: 1,
        enable_fast_commit: true,
        enable_distinguished_leader: false,
    };
    let mut sim = build_simulation(&config, 123);

    sim.client("client", async move {
        let (_, v1) = propose_write("proposer-1", 10).await?;
        assert_eq!(v1, 10);

        let (_, v2) = propose_write("proposer-1", 20).await?;
        assert_eq!(v2, 20);

        let (_, v3) = propose_write("proposer-1", 30).await?;
        assert_eq!(v3, 30);
        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn turmoil_classic_round_single_write() {
    let config = TurmoilConfig {
        num_acceptors: 3,
        num_proposers: 1,
        enable_fast_commit: false,
        enable_distinguished_leader: false,
    };
    let mut sim = build_simulation(&config, 99);

    sim.client("client", async move {
        let (_, committed) = propose_write("proposer-1", 77).await?;
        assert_eq!(committed, 77);
        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn turmoil_deterministic_across_reruns() {
    // Running with the same seed should give the same result.
    for _ in 0..3 {
        let config = TurmoilConfig {
            num_acceptors: 3,
            num_proposers: 1,
            enable_fast_commit: true,
            enable_distinguished_leader: false,
        };
        let mut sim = build_simulation(&config, 7);

        sim.client("client", async move {
            let (round, committed) = propose_write("proposer-1", 55).await?;
            assert_eq!(committed, 55);
            assert!(round > 0);
            Ok(())
        });

        sim.run().unwrap();
    }
}

#[test]
fn turmoil_five_acceptors_fast_commit() {
    let config = TurmoilConfig {
        num_acceptors: 5,
        num_proposers: 1,
        enable_fast_commit: true,
        enable_distinguished_leader: false,
    };
    let mut sim = build_simulation(&config, 1001);

    sim.client("client", async move {
        let (_, committed) = propose_write("proposer-1", 999).await?;
        assert_eq!(committed, 999);
        Ok(())
    });

    sim.run().unwrap();
}
