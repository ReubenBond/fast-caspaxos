//! Turmoil-based deterministic simulation for Fast `CASPaxos`.
//!
//! Provides async hosts running on turmoil's simulated network,
//! with deterministic execution and fault injection.
//!
//! The core engines (`AcceptorEngine`, `ProposerEngine`) are pure
//! synchronous state machines; this module wraps them in async TCP
//! servers that communicate via length-prefixed bincode messages.

use std::io;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use turmoil::net::{TcpListener, TcpStream};

use fast_caspaxos_core::acceptor::{AcceptorEngine, AcceptorHandler};
use fast_caspaxos_core::ballot::Ballot;
use fast_caspaxos_core::messages::{
    AcceptAccepted, AcceptRejected, AcceptRequest, CommittedValue, PreparePromise,
    PrepareRejection, PrepareRequest, ProposeResponse,
};
use fast_caspaxos_core::operation::{FnOperation, Operation, OperationResult};
use fast_caspaxos_core::proposer::{ProposerConfig, ProposerEngine, ProposerHandler};

const ACCEPTOR_PORT: u16 = 9000;
const PROPOSER_PORT: u16 = 9100;

// ---------------------------------------------------------------------------
// Wire protocol — bincode-serializable messages over TCP
// ---------------------------------------------------------------------------

/// Wire-level message exchanged between turmoil hosts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireMessage {
    // Proposer → Acceptor
    PrepareReq {
        ballot_round: u32,
        ballot_proposer: u32,
    },
    AcceptReq {
        ballot_round: u32,
        ballot_proposer: u32,
        value: i32,
        next_ballot_round: Option<u32>,
        next_ballot_proposer: Option<u32>,
    },

    // Acceptor → Proposer
    PreparePromiseResp {
        round: u32,
        acceptor_id: u32,
        accepted_ballot_round: u32,
        accepted_ballot_proposer: u32,
        accepted_value: i32,
    },
    PrepareRejectionResp {
        round: u32,
        acceptor_id: u32,
        accepted_ballot_round: u32,
        accepted_ballot_proposer: u32,
        accepted_value: i32,
        conflict_ballot_round: u32,
        conflict_ballot_proposer: u32,
    },
    AcceptAcceptedResp {
        round: u32,
        acceptor_id: u32,
        promised_ballot_round: u32,
        promised_ballot_proposer: u32,
    },
    AcceptRejectedResp {
        round: u32,
        acceptor_id: u32,
        conflicting_ballot_round: u32,
        conflicting_ballot_proposer: u32,
    },

    // Client → Proposer
    ClientProposal {
        value: i32,
    },
    // Proposer → Client
    ClientResponse {
        round: u32,
        committed_value: i32,
    },
}

/// Send a length-prefixed bincode message.
async fn send_message(stream: &mut TcpStream, msg: &WireMessage) -> io::Result<()> {
    let bytes = bincode::serialize(msg).map_err(io::Error::other)?;
    let len = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "message too large to frame"))?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&bytes).await?;
    Ok(())
}

/// Receive a length-prefixed bincode message.
async fn recv_message(stream: &mut TcpStream) -> io::Result<WireMessage> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message too large",
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    bincode::deserialize(&buf).map_err(io::Error::other)
}

// ---------------------------------------------------------------------------
// Acceptor host
// ---------------------------------------------------------------------------

/// Handler that captures the single response produced by an acceptor operation.
struct AcceptorResponseCapture {
    response: Option<WireMessage>,
}

impl AcceptorHandler<i32, u32> for AcceptorResponseCapture {
    fn on_prepare_promised(&mut self, result: PreparePromise<i32, u32>) {
        self.response = Some(WireMessage::PreparePromiseResp {
            round: result.round,
            acceptor_id: result.acceptor,
            accepted_ballot_round: result.accepted_ballot.round,
            accepted_ballot_proposer: result.accepted_ballot.proposer,
            accepted_value: result.accepted_value,
        });
    }

    fn on_prepare_rejected(&mut self, result: PrepareRejection<i32, u32>) {
        self.response = Some(WireMessage::PrepareRejectionResp {
            round: result.round,
            acceptor_id: result.acceptor,
            accepted_ballot_round: result.accepted_ballot.round,
            accepted_ballot_proposer: result.accepted_ballot.proposer,
            accepted_value: result.accepted_value,
            conflict_ballot_round: result.conflict_ballot.round,
            conflict_ballot_proposer: result.conflict_ballot.proposer,
        });
    }

    fn on_accept_accepted(&mut self, result: AcceptAccepted<u32>) {
        self.response = Some(WireMessage::AcceptAcceptedResp {
            round: result.round,
            acceptor_id: result.acceptor,
            promised_ballot_round: result.promised_ballot.round,
            promised_ballot_proposer: result.promised_ballot.proposer,
        });
    }

    fn on_accept_rejected(&mut self, result: AcceptRejected<u32>) {
        self.response = Some(WireMessage::AcceptRejectedResp {
            round: result.round,
            acceptor_id: result.acceptor,
            conflicting_ballot_round: result.conflicting_ballot.round,
            conflicting_ballot_proposer: result.conflicting_ballot.proposer,
        });
    }
}

/// Run an acceptor host: listens for TCP connections and processes one
/// message per connection (request-response).
async fn run_acceptor(acceptor_id: u32) -> io::Result<()> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, ACCEPTOR_PORT)).await?;
    let mut engine = AcceptorEngine::new(acceptor_id, None);

    loop {
        let (mut stream, _) = listener.accept().await?;
        let Ok(msg) = recv_message(&mut stream).await else {
            continue;
        };

        let response = {
            let mut handler = AcceptorResponseCapture { response: None };

            match msg {
                WireMessage::PrepareReq {
                    ballot_round,
                    ballot_proposer,
                } => {
                    engine.prepare(
                        &mut handler,
                        &PrepareRequest {
                            ballot: Ballot::new(0, ballot_round, ballot_proposer),
                        },
                    );
                }
                WireMessage::AcceptReq {
                    ballot_round,
                    ballot_proposer,
                    value,
                    next_ballot_round,
                    next_ballot_proposer,
                } => {
                    let next = match (next_ballot_round, next_ballot_proposer) {
                        (Some(r), Some(p)) => Some(Ballot::new(0, r, p)),
                        _ => None,
                    };
                    engine.accept(
                        &mut handler,
                        &AcceptRequest {
                            ballot: Ballot::new(0, ballot_round, ballot_proposer),
                            value,
                            next_ballot_to_prepare: next,
                        },
                    );
                }
                _ => continue,
            }

            handler.response
        };

        if let Some(resp) = response {
            let _ = send_message(&mut stream, &resp).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Proposer host — drives proposals via TCP to acceptors
// ---------------------------------------------------------------------------

/// Outbound messages collected by the proposer handler.
enum ProposerAction {
    SendPrepare(PrepareRequest),
    SendAccept(AcceptRequest<i32>),
    Completed(ProposeResponse<i32>),
}

struct ProposerResponseCapture {
    actions: Vec<ProposerAction>,
}

impl ProposerHandler<i32, u32> for ProposerResponseCapture {
    fn send_prepare(&mut self, request: PrepareRequest) {
        self.actions.push(ProposerAction::SendPrepare(request));
    }
    fn send_accept(&mut self, request: AcceptRequest<i32>) {
        self.actions.push(ProposerAction::SendAccept(request));
    }
    fn proposal_completed(&mut self, response: ProposeResponse<i32>) {
        self.actions.push(ProposerAction::Completed(response));
    }
    fn value_committed(&mut self, _committed: CommittedValue<i32>) {}
}

/// Send a prepare to one acceptor and return the response.
async fn send_prepare_to_acceptor(acceptor_host: &str, ballot: Ballot) -> io::Result<WireMessage> {
    let mut stream = TcpStream::connect((acceptor_host, ACCEPTOR_PORT)).await?;
    send_message(
        &mut stream,
        &WireMessage::PrepareReq {
            ballot_round: ballot.round,
            ballot_proposer: ballot.proposer,
        },
    )
    .await?;
    recv_message(&mut stream).await
}

/// Send an accept to one acceptor and return the response.
async fn send_accept_to_acceptor(
    acceptor_host: &str,
    request: &AcceptRequest<i32>,
) -> io::Result<WireMessage> {
    let mut stream = TcpStream::connect((acceptor_host, ACCEPTOR_PORT)).await?;
    let (next_r, next_p) = match request.next_ballot_to_prepare {
        Some(b) => (Some(b.round), Some(b.proposer)),
        None => (None, None),
    };
    send_message(
        &mut stream,
        &WireMessage::AcceptReq {
            ballot_round: request.ballot.round,
            ballot_proposer: request.ballot.proposer,
            value: request.value,
            next_ballot_round: next_r,
            next_ballot_proposer: next_p,
        },
    )
    .await?;
    recv_message(&mut stream).await
}

/// Run the proposer host: listens for client proposals on `PROPOSER_PORT`,
/// then drives the prepare/accept phases against all acceptors.
///
/// Processes one client at a time (sequential) to avoid `Send` constraints
/// on `dyn Operation`.
async fn run_proposer(
    proposer_id: u32,
    num_acceptors: u32,
    enable_fast_commit: bool,
    enable_distinguished_leader: bool,
) -> io::Result<()> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, PROPOSER_PORT)).await?;
    let acceptor_ids: Vec<u32> = (1..=num_acceptors).collect();
    let mut engine = ProposerEngine::new(
        ProposerConfig {
            proposer_id,
            enable_fast_commit,
            enable_distinguished_leader,
            ballot_seed: Ballot::ZERO,
        },
        acceptor_ids.clone(),
    );

    loop {
        let (mut client_stream, _) = listener.accept().await?;

        let Ok(client_msg) = recv_message(&mut client_stream).await else {
            continue;
        };

        let WireMessage::ClientProposal { value } = client_msg else {
            continue;
        };

        let op: Box<dyn Operation<i32>> = Box::new(FnOperation::new("write", move |_: &i32| {
            OperationResult::Success(value)
        }));

        if let Ok(resp) = drive_proposal(&mut engine, &acceptor_ids, op).await {
            let _ = send_message(
                &mut client_stream,
                &WireMessage::ClientResponse {
                    round: resp.round,
                    committed_value: resp.committed_value,
                },
            )
            .await;
        }
    }
}

/// Drive a single proposal through prepare/accept phases.
///
/// Loops until the engine reports completion (or an error).
async fn drive_proposal(
    engine: &mut ProposerEngine<i32, u32>,
    acceptor_ids: &[u32],
    operation: Box<dyn Operation<i32>>,
) -> io::Result<ProposeResponse<i32>> {
    let initial_actions = {
        let mut handler = ProposerResponseCapture {
            actions: Vec::new(),
        };
        engine.start_proposal(operation, &mut handler);
        handler.actions
    };

    let mut pending_actions = initial_actions;

    for _iteration in 0..20 {
        let mut next_actions = Vec::new();

        for action in pending_actions {
            match action {
                ProposerAction::Completed(resp) => return Ok(resp),
                ProposerAction::SendPrepare(req) => {
                    for &acc_id in acceptor_ids {
                        let host = format!("acceptor-{acc_id}");
                        if let Ok(wire_resp) = send_prepare_to_acceptor(&host, req.ballot).await {
                            let mut handler = ProposerResponseCapture {
                                actions: Vec::new(),
                            };
                            deliver_wire_response(engine, &mut handler, &wire_resp);
                            next_actions.extend(handler.actions);
                        }
                    }
                }
                ProposerAction::SendAccept(req) => {
                    for &acc_id in acceptor_ids {
                        let host = format!("acceptor-{acc_id}");
                        if let Ok(wire_resp) = send_accept_to_acceptor(&host, &req).await {
                            let mut handler = ProposerResponseCapture {
                                actions: Vec::new(),
                            };
                            deliver_wire_response(engine, &mut handler, &wire_resp);
                            next_actions.extend(handler.actions);
                        }
                    }
                }
            }
        }

        if next_actions.is_empty() {
            break;
        }
        pending_actions = next_actions;
    }

    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "proposal did not complete within iteration limit",
    ))
}

/// Translate a wire response into the appropriate engine call.
fn deliver_wire_response(
    engine: &mut ProposerEngine<i32, u32>,
    handler: &mut ProposerResponseCapture,
    msg: &WireMessage,
) {
    match msg {
        WireMessage::PreparePromiseResp {
            round,
            acceptor_id,
            accepted_ballot_round,
            accepted_ballot_proposer,
            accepted_value,
        } => {
            engine.handle_prepare_promised(
                PreparePromise {
                    round: *round,
                    acceptor: *acceptor_id,
                    accepted_ballot: Ballot::new(
                        0,
                        *accepted_ballot_round,
                        *accepted_ballot_proposer,
                    ),
                    accepted_value: *accepted_value,
                },
                handler,
            );
        }
        WireMessage::PrepareRejectionResp {
            round,
            acceptor_id,
            accepted_ballot_round,
            accepted_ballot_proposer,
            accepted_value,
            conflict_ballot_round,
            conflict_ballot_proposer,
        } => {
            engine.handle_prepare_rejected(
                PrepareRejection {
                    round: *round,
                    acceptor: *acceptor_id,
                    accepted_ballot: Ballot::new(
                        0,
                        *accepted_ballot_round,
                        *accepted_ballot_proposer,
                    ),
                    accepted_value: *accepted_value,
                    conflict_ballot: Ballot::new(
                        0,
                        *conflict_ballot_round,
                        *conflict_ballot_proposer,
                    ),
                },
                handler,
            );
        }
        WireMessage::AcceptAcceptedResp {
            round,
            acceptor_id,
            promised_ballot_round,
            promised_ballot_proposer,
        } => {
            engine.handle_accept_accepted(
                AcceptAccepted {
                    round: *round,
                    acceptor: *acceptor_id,
                    promised_ballot: Ballot::new(
                        0,
                        *promised_ballot_round,
                        *promised_ballot_proposer,
                    ),
                },
                handler,
            );
        }
        WireMessage::AcceptRejectedResp {
            round,
            acceptor_id,
            conflicting_ballot_round,
            conflicting_ballot_proposer,
        } => {
            engine.handle_accept_rejected(
                AcceptRejected {
                    round: *round,
                    acceptor: *acceptor_id,
                    conflicting_ballot: Ballot::new(
                        *conflicting_ballot_round,
                        *conflicting_ballot_proposer,
                    ),
                },
                handler,
            );
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Configuration for a turmoil simulation.
#[derive(Debug, Clone)]
pub struct TurmoilConfig {
    pub num_acceptors: u32,
    pub num_proposers: u32,
    pub enable_fast_commit: bool,
    pub enable_distinguished_leader: bool,
}

impl Default for TurmoilConfig {
    fn default() -> Self {
        Self {
            num_acceptors: 5,
            num_proposers: 3,
            enable_fast_commit: true,
            enable_distinguished_leader: false,
        }
    }
}

/// Build a turmoil [`Sim`](turmoil::Sim) with acceptor and proposer hosts
/// registered, then hand it to the caller to add clients and run.
///
/// # Errors
///
/// The returned `Sim` itself does not error. Individual `sim.run()` calls
/// propagate host errors.
pub fn build_simulation(config: &TurmoilConfig, seed: u64) -> turmoil::Sim<'static> {
    let mut builder = turmoil::Builder::new();
    builder
        .simulation_duration(Duration::from_secs(60))
        .rng_seed(seed);
    let mut sim = builder.build();

    // Register acceptor hosts.
    for i in 1..=config.num_acceptors {
        let host_name = format!("acceptor-{i}");
        sim.host(host_name, move || async move {
            run_acceptor(i).await?;
            Ok(())
        });
    }

    // Register proposer hosts.
    for i in 1..=config.num_proposers {
        let num_acc = config.num_acceptors;
        let fast = config.enable_fast_commit;
        let leader = config.enable_distinguished_leader;
        let host_name = format!("proposer-{i}");
        sim.host(host_name, move || async move {
            run_proposer(i, num_acc, fast, leader).await?;
            Ok(())
        });
    }

    sim
}

/// Helper to submit a write proposal to a proposer and receive the response.
///
/// # Errors
///
/// Returns an error if the TCP connection fails or the proposer sends an
/// unexpected response.
pub async fn propose_write(proposer_host: &str, value: i32) -> io::Result<(u32, i32)> {
    let mut stream = TcpStream::connect((proposer_host, PROPOSER_PORT)).await?;
    send_message(&mut stream, &WireMessage::ClientProposal { value }).await?;
    let resp = recv_message(&mut stream).await?;
    match resp {
        WireMessage::ClientResponse {
            round,
            committed_value,
        } => Ok((round, committed_value)),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected response: {other:?}"),
        )),
    }
}
