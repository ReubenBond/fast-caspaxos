use std::fmt;

/// Role of a participant in the Fast `CASPaxos` protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ParticipantRole {
    Client,
    Proposer,
    Acceptor,
}

impl ParticipantRole {
    fn as_str(self) -> &'static str {
        match self {
            ParticipantRole::Client => "client",
            ParticipantRole::Proposer => "proposer",
            ParticipantRole::Acceptor => "acceptor",
        }
    }
}

impl fmt::Display for ParticipantRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Address of a network endpoint in the simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Address {
    pub role: ParticipantRole,
    pub ordinal: u32,
}

impl Address {
    pub fn client(ordinal: u32) -> Self {
        Self {
            role: ParticipantRole::Client,
            ordinal,
        }
    }

    pub fn proposer(ordinal: u32) -> Self {
        Self {
            role: ParticipantRole::Proposer,
            ordinal,
        }
    }

    pub fn acceptor(ordinal: u32) -> Self {
        Self {
            role: ParticipantRole::Acceptor,
            ordinal,
        }
    }

    /// Network address string like `"proposer-1"`, `"acceptor-3"`, `"client-0"`.
    pub fn network_address(&self) -> String {
        format!("{}-{}", self.role.as_str(), self.ordinal)
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.role, self.ordinal)
    }
}

/// Identifies a proposer or acceptor node (not a client).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId {
    address: Address,
}

impl NodeId {
    /// Create a new `NodeId`. Panics if role is `Client`.
    pub fn new(address: Address) -> Self {
        assert_ne!(
            address.role,
            ParticipantRole::Client,
            "NodeId cannot be created for a Client address"
        );
        Self { address }
    }

    pub fn address(&self) -> Address {
        self.address
    }

    pub fn role(&self) -> ParticipantRole {
        self.address.role
    }

    pub fn ordinal(&self) -> u32 {
        self.address.ordinal
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_address_construction() {
        let addr = Address::client(0);
        assert_eq!(addr.role, ParticipantRole::Client);
        assert_eq!(addr.ordinal, 0);
    }

    #[test]
    fn proposer_address_construction() {
        let addr = Address::proposer(5);
        assert_eq!(addr.role, ParticipantRole::Proposer);
        assert_eq!(addr.ordinal, 5);
    }

    #[test]
    fn acceptor_address_construction() {
        let addr = Address::acceptor(3);
        assert_eq!(addr.role, ParticipantRole::Acceptor);
        assert_eq!(addr.ordinal, 3);
    }

    #[test]
    fn network_address_format() {
        assert_eq!(Address::client(0).network_address(), "client-0");
        assert_eq!(Address::proposer(1).network_address(), "proposer-1");
        assert_eq!(Address::acceptor(3).network_address(), "acceptor-3");
    }

    #[test]
    fn display_matches_network_address() {
        let addr = Address::proposer(2);
        assert_eq!(addr.to_string(), addr.network_address());
    }

    #[test]
    fn address_equality() {
        assert_eq!(Address::proposer(1), Address::proposer(1));
        assert_ne!(Address::proposer(1), Address::proposer(2));
        assert_ne!(Address::proposer(1), Address::acceptor(1));
    }

    #[test]
    fn address_ordering() {
        // Client < Proposer < Acceptor (enum declaration order)
        assert!(Address::client(0) < Address::proposer(0));
        assert!(Address::proposer(0) < Address::acceptor(0));
        // Same role, different ordinal
        assert!(Address::acceptor(1) < Address::acceptor(2));
    }

    #[test]
    fn node_id_from_proposer() {
        let addr = Address::proposer(1);
        let node = NodeId::new(addr);
        assert_eq!(node.address(), addr);
        assert_eq!(node.role(), ParticipantRole::Proposer);
        assert_eq!(node.ordinal(), 1);
    }

    #[test]
    fn node_id_from_acceptor() {
        let addr = Address::acceptor(2);
        let node = NodeId::new(addr);
        assert_eq!(node.address(), addr);
        assert_eq!(node.role(), ParticipantRole::Acceptor);
        assert_eq!(node.ordinal(), 2);
    }

    #[test]
    #[should_panic(expected = "NodeId cannot be created for a Client address")]
    fn node_id_panics_on_client() {
        let _ = NodeId::new(Address::client(0));
    }

    #[test]
    fn node_id_display() {
        let node = NodeId::new(Address::acceptor(7));
        assert_eq!(node.to_string(), "acceptor-7");
    }

    #[test]
    fn participant_role_display() {
        assert_eq!(ParticipantRole::Client.to_string(), "client");
        assert_eq!(ParticipantRole::Proposer.to_string(), "proposer");
        assert_eq!(ParticipantRole::Acceptor.to_string(), "acceptor");
    }
}
