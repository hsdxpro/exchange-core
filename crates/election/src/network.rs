//! Carrying Raft's messages between nodes.
//!
//! Length-prefixed JSON over TCP, and deliberately unremarkable. Everything on
//! the venue's own path is a fixed 64-byte record read with no parsing step,
//! because that path carries every order; this one carries a heartbeat twice a
//! second and an election once per failure. Spending the same effort here would
//! buy microseconds on a path measured in elections per year, and cost a second
//! hand-rolled wire format to keep in step with openraft's types.
//!
//! Connections are kept and re-dialled when they break, rather than opened per
//! message. A heartbeat that pays for a TCP handshake makes a healthy follower
//! look slow, and a follower that looks slow is one an election is called over.

use crate::types::{Leadership, NodeId};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Raft};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// One Raft message, as it goes on the wire.
#[derive(Debug, Deserialize, Serialize)]
enum Rpc {
    Append(AppendEntriesRequest<Leadership>),
    Vote(VoteRequest<NodeId>),
    Snapshot(InstallSnapshotRequest<Leadership>),
}

/// What comes back. `Err` carries the message rather than the error type,
/// because a node that cannot answer should say why in a way the caller can log.
#[derive(Debug, Deserialize, Serialize)]
enum Reply {
    Append(AppendEntriesResponse<NodeId>),
    Vote(VoteResponse<NodeId>),
    Snapshot(InstallSnapshotResponse<NodeId>),
    Refused(String),
}

/// Largest message accepted, so a malformed or hostile length cannot make a node
/// allocate without bound. A leadership message is a few hundred bytes; a
/// snapshot of a one-integer state machine is smaller still.
const MAX_MESSAGE: u32 = 1 << 20;

async fn write_framed<T: Serialize>(stream: &mut TcpStream, value: &T) -> std::io::Result<()> {
    let body = serde_json::to_vec(value)?;
    let length = u32::try_from(body.len())
        .map_err(|_| std::io::Error::other("leadership message too large to frame"))?;
    stream.write_all(&length.to_le_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
}

async fn read_framed<T: for<'a> Deserialize<'a>>(stream: &mut TcpStream) -> std::io::Result<T> {
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header).await?;
    let length = u32::from_le_bytes(header);
    if length > MAX_MESSAGE {
        return Err(std::io::Error::other(format!(
            "leadership message claims {length} bytes, over the {MAX_MESSAGE} limit"
        )));
    }
    let mut body = vec![0_u8; length as usize];
    stream.read_exact(&mut body).await?;
    serde_json::from_slice(&body).map_err(std::io::Error::other)
}

/// Hands out one client per peer.
#[derive(Clone, Debug)]
pub struct Network {
    peers: Vec<(NodeId, String)>,
}

impl Network {
    #[must_use]
    pub fn new(peers: Vec<(NodeId, String)>) -> Self {
        Self { peers }
    }

    fn address_of(&self, target: NodeId) -> Option<String> {
        self.peers
            .iter()
            .find(|(id, _)| *id == target)
            .map(|(_, address)| address.clone())
    }
}

impl RaftNetworkFactory<Leadership> for Network {
    type Network = Peer;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        Peer {
            target,
            address: self.address_of(target).unwrap_or_default(),
            stream: None,
        }
    }
}

/// One peer, and the connection to it.
#[derive(Debug)]
pub struct Peer {
    target: NodeId,
    address: String,
    stream: Option<TcpStream>,
}

impl Peer {
    /// Sends one message and reads its answer, re-dialling once if the held
    /// connection has gone.
    ///
    /// One retry rather than a loop: if a fresh connection also fails the peer
    /// is genuinely unreachable, and openraft's own backoff is the right place
    /// for that decision, not a retry loop buried in the transport.
    async fn exchange(&mut self, request: &Rpc) -> Result<Reply, std::io::Error> {
        for attempt in 0..2 {
            if self.stream.is_none() {
                self.stream = Some(TcpStream::connect(&self.address).await?);
                if let Some(stream) = &self.stream {
                    let _ = stream.set_nodelay(true);
                }
            }
            let Some(stream) = self.stream.as_mut() else {
                continue;
            };
            match write_framed(stream, request).await {
                Ok(()) => match read_framed(stream).await {
                    Ok(reply) => return Ok(reply),
                    Err(e) if attempt == 1 => return Err(e),
                    Err(_) => self.stream = None,
                },
                Err(e) if attempt == 1 => return Err(e),
                Err(_) => self.stream = None,
            }
        }
        Err(std::io::Error::other("peer did not answer"))
    }
}

/// Turns a transport failure into the shape openraft expects.
fn unreachable<E: std::error::Error + 'static, T>(
    target: NodeId,
    error: &E,
) -> RPCError<NodeId, BasicNode, T>
where
    T: std::error::Error,
{
    let _ = target;
    RPCError::Unreachable(Unreachable::new(error))
}

impl RaftNetwork<Leadership> for Peer {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<Leadership>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.exchange(&Rpc::Append(request)).await {
            Ok(Reply::Append(reply)) => Ok(reply),
            Ok(other) => Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other(format!("peer answered an append with {other:?}")),
            ))),
            Err(e) => Err(unreachable(self.target, &e)),
        }
    }

    async fn vote(
        &mut self,
        request: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.exchange(&Rpc::Vote(request)).await {
            Ok(Reply::Vote(reply)) => Ok(reply),
            Ok(other) => Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other(format!("peer answered a vote with {other:?}")),
            ))),
            Err(e) => Err(unreachable(self.target, &e)),
        }
    }

    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<Leadership>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        match self.exchange(&Rpc::Snapshot(request)).await {
            Ok(Reply::Snapshot(reply)) => Ok(reply),
            Ok(other) => Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other(format!("peer answered a snapshot with {other:?}")),
            ))),
            Err(e) => Err(unreachable(self.target, &e)),
        }
    }
}

/// Answers the other nodes' Raft messages, for as long as the process lives.
///
/// Every connection is served on its own task, because a node that is slow to
/// read must not stop this one answering a vote — an unanswered vote is an
/// election that does not finish.
pub async fn serve(listener: TcpListener, raft: Raft<Leadership>) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let raft = raft.clone();
        tokio::spawn(async move {
            let _ = stream.set_nodelay(true);
            while let Ok(request) = read_framed::<Rpc>(&mut stream).await {
                let reply = match request {
                    Rpc::Append(request) => raft
                        .append_entries(request)
                        .await
                        .map_or_else(|e| Reply::Refused(e.to_string()), Reply::Append),
                    Rpc::Vote(request) => raft
                        .vote(request)
                        .await
                        .map_or_else(|e| Reply::Refused(e.to_string()), Reply::Vote),
                    Rpc::Snapshot(request) => raft
                        .install_snapshot(request)
                        .await
                        .map_or_else(|e| Reply::Refused(e.to_string()), Reply::Snapshot),
                };
                if write_framed(&mut stream, &reply).await.is_err() {
                    break;
                }
            }
        });
    }
}
