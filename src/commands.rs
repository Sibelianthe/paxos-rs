use crate::{Ballot, NodeId, NodeMetadata, Slot};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Sends commands to other replicas in addition to applying
/// resolved commands at the current replica
pub trait Transport {
    /// Send a message to a single node
    fn send(&mut self, node: NodeId, node_metadata: &NodeMetadata, command: Command, command_metadata: CommandMetas);
}

/// Receiver of Paxos commands.
pub trait Receiver {
    /// Receives a command and reacts accordingly
    fn receive(&mut self, command: Command, cmd_metas: CommandMetas);
}

/// Receiver of Paxos commands.
///
/// This is a convenience trait that breaks out reactors for each command.
pub trait Commander {
    /// Receive a proposal
    fn proposal(&mut self, val: Bytes, cmd_metas: CommandMetas);

    /// Receive a Phase 1a PREPARE message containing the proposed ballot
    fn prepare(&mut self, bal: Ballot, cmd_metas: CommandMetas);

    /// Receive a Phase 1b PROMISE message containing the node
    /// that generated the promise, the ballot promised and all accepted
    /// values within the open window.
    fn promise(&mut self, node: NodeId, bal: Ballot, accepted: Vec<(Slot, Ballot, Bytes)>, cmd_metas: CommandMetas);

    /// Receive a Phase 2a ACCEPT message that contains the the slot, proposed
    /// ballot and value of the proposal. The ballot contains the node of
    /// the leader of the slot.
    fn accept(&mut self, bal: Ballot, slot_values: Vec<(Slot, Bytes)>, cmd_metas: CommandMetas);

    /// Receives a REJECT message from a peer containing a higher ballot that
    /// preempts either a Phase 1a (PREPARE) for Phase 2a (ACCEPT) message.
    fn reject(&mut self, node: NodeId, proposed: Ballot, preempted: Ballot, cmd_metas: CommandMetas);

    /// Receives a Phase 2b ACCEPTED message containing the acceptor that has
    /// accepted the slot's proposal along with the ballot that generated
    /// the slot.
    fn accepted(&mut self, node: NodeId, bal: Ballot, slots: Vec<Slot>, cmd_metas: CommandMetas);

    /// Receives a final resolution of a slot that has been accepted by a
    /// majority of acceptors.
    ///
    /// NOTE: Resolutions may arrive out-of-order. No guarantees are made on
    /// slot order.
    fn resolution(&mut self, bal: Ballot, values: Vec<(Slot, Bytes)>, cmd_metas: CommandMetas);

    /// Request sent to a distinguished learner to catch up to latest slot
    /// values.
    fn catchup(&mut self, node: NodeId, slots: Vec<Slot>, cmd_metas: CommandMetas);
}

impl<T: Commander> Receiver for T {
    fn receive(&mut self, command: Command, cmd_metas: CommandMetas) {
        match command {
            Command::Proposal { payload: val } => {
                self.proposal(val, cmd_metas);
            }
            Command::Prepare { payload: bal } => {
                self.prepare(bal, cmd_metas);
            }
            Command::Promise { payload: (node, bal, accepted)} => {
                self.promise(node, bal, accepted, cmd_metas);
            }
            Command::Accept { payload: (bal, slot_vals)} => {
                self.accept(bal, slot_vals, cmd_metas);
            }
            Command::Reject { payload: (node, proposed, preempted)} => {
                self.reject(node, proposed, preempted, cmd_metas);
            }
            Command::Accepted { payload: (node, bal, slots)} => {
                self.accepted(node, bal, slots, cmd_metas);
            }
            Command::Resolution { payload: (bal, slot_vals)} => {
                self.resolution(bal, slot_vals, cmd_metas);
            }
            Command::Catchup { payload: (node, slots)} => {
                self.catchup(node, slots, cmd_metas);
            }
        }
    }
}

// #[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
/// RPC commands sent between replicas
// pub enum OrigCommand {
//     /// Propose a value
//     Proposal(Bytes),

//     /// Phase 1a PREPARE message containing the proposed ballot
//     Prepare(Ballot),

//     /// Phase 1b PROMISE message containing the node
//     /// that generated the promise, the ballot promised and all accepted
//     /// values within the open window.
//     Promise(NodeId, Ballot, Vec<(Slot, Ballot, Bytes)>),

//     /// Phase 2a ACCEPT message that contains the the slot, proposed
//     /// ballot and value of the proposal. The ballot contains the node of
//     /// the leader of the slot.
//     Accept(Ballot, Vec<(Slot, Bytes)>),

//     /// REJECT a peer's previous message containing a higher ballot that
//     /// preempts either a Phase 1a (PREPARE) for Phase 2a (ACCEPT) message.
//     Reject(NodeId, Ballot, Ballot),

//     /// Phase 2b ACCEPTED message containing the acceptor that has
//     /// accepted the slot's proposal along with the ballot that generated
//     /// the slot.
//     Accepted(NodeId, Ballot, Vec<Slot>),

//     /// Resolution of a slot that has been accepted by a
//     /// majority of acceptors.
//     ///
//     /// NOTE: Resolutions may arrive out-of-order. No guarantees are made on
//     /// slot order.
//     Resolution(Ballot, Vec<(Slot, Bytes)>),

//     /// Request sent to a distinguished learner to catch up to latest slot
//     /// values.
//     Catchup(NodeId, Vec<Slot>),
// }

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
/// RPC commands sent between replicas
#[serde(tag = "messageName")]
pub enum Command {
    /// Propose a value
    Proposal { payload: Bytes },

    /// Phase 1a PREPARE message containing the proposed ballot
    Prepare { payload: Ballot },

    /// Phase 1b PROMISE message containing the node
    /// that generated the promise, the ballot promised and all accepted
    /// values within the open window.
    Promise { payload: (NodeId, Ballot, Vec<(Slot, Ballot, Bytes)>) },

    /// Phase 2a ACCEPT message that contains the the slot, proposed
    /// ballot and value of the proposal. The ballot contains the node of
    /// the leader of the slot.
    Accept { payload: (Ballot, Vec<(Slot, Bytes)>) },

    /// REJECT a peer's previous message containing a higher ballot that
    /// preempts either a Phase 1a (PREPARE) for Phase 2a (ACCEPT) message.
    Reject { payload: (NodeId, Ballot, Ballot) },

    /// Phase 2b ACCEPTED message containing the acceptor that has
    /// accepted the slot's proposal along with the ballot that generated
    /// the slot.
    Accepted { payload: (NodeId, Ballot, Vec<Slot>) },

    /// Resolution of a slot that has been accepted by a
    /// majority of acceptors.
    ///
    /// NOTE: Resolutions may arrive out-of-order. No guarantees are made on
    /// slot order.
    Resolution { payload: (Ballot, Vec<(Slot, Bytes)>) },

    /// Request sent to a distinguished learner to catch up to latest slot
    /// values.
    Catchup { payload: (NodeId, Vec<Slot>) },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandMetas(pub Bytes);

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_serializes_command_proposal() {
        let json = r#"{"messageName":"Proposal","payload":[]}"#;

        let command = Command::Proposal { payload: "".into() };
        let serialized_command = serde_json::to_string(&command).unwrap();
        assert_eq!(&serialized_command, json);
    }

    #[test]
    fn it_serializes_command_prepare() {
        let json = r#"{"messageName":"Prepare","payload":[123,345]}"#;
        let ballot = Ballot(123_u32, 345_u32);

        let command = Command::Prepare { payload: ballot };
        let serialized_command = serde_json::to_string(&command).unwrap();
        assert_eq!(&serialized_command, json);
    }

    #[test]
    fn it_serializes_command_promise() {
        let json = r#"{"messageName":"Promise","payload":[42,[123,345],[[0,[123,345],[104,101,108,108,111]]]]}"#;
        let v = vec![(0u64, Ballot(123_u32, 345_u32), "hello".into())];
        let payload = (42, Ballot(123_u32, 345_u32), v);

        let command = Command::Promise { payload };
        let serialized_command = serde_json::to_string(&command).unwrap();
        assert_eq!(&serialized_command, json);
    }

    #[test]
    fn it_serializes_command_accept() {
        let json = r#"{"messageName":"Accept","payload":[[123,345],[[0,[104,101,108,108,111]]]]}"#;
        let v = vec![(0u64, "hello".into())];
        let payload = (Ballot(123_u32, 345_u32), v);

        let command = Command::Accept { payload };
        let serialized_command = serde_json::to_string(&command).unwrap();
        assert_eq!(&serialized_command, json);
    }

    #[test]
    fn it_serializes_command_reject() {
        let json = r#"{"messageName":"Reject","payload":[13,[123,345],[123,345]]}"#;
        let ballot = Ballot(123_u32, 345_u32);

        let command = Command::Reject { payload: (13, ballot, ballot) };
        let serialized_command = serde_json::to_string(&command).unwrap();
        assert_eq!(&serialized_command, json);
    }

    #[test]
    fn it_serializes_command_accepted() {
        let json = r#"{"messageName":"Accepted","payload":[13,[123,345],[15]]}"#;
        let v = vec![15_u64];
        let ballot = Ballot(123_u32, 345_u32);

        let command = Command::Accepted { payload: (13, ballot, v) };
        let serialized_command = serde_json::to_string(&command).unwrap();
        assert_eq!(&serialized_command, json);
    }

    #[test]
    fn it_serializes_command_resolution() {
        let json = r#"{"messageName":"Resolution","payload":[[123,345],[[15,[]]]]}"#;
        let v = vec![(15_u64, "".into())];
        let ballot = Ballot(123_u32, 345_u32);

        let command = Command::Resolution { payload: (ballot, v) };
        let serialized_command = serde_json::to_string(&command).unwrap();
        assert_eq!(&serialized_command, json);
    }

    #[test]
    fn it_serializes_command_catchup() {
        let json = r#"{"messageName":"Catchup","payload":[16,[444]]}"#;
        let v = vec![444_u64];

        let command = Command::Catchup { payload: (16, v) };
        let serialized_command = serde_json::to_string(&command).unwrap();
        assert_eq!(&serialized_command, json);
    }
}
