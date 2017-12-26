use std::io;
use futures::{Async, AsyncSink, Poll, Sink, StartSend, Stream};
use futures::unsync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use either::Either;
use messages::*;
use algo::*;
use super::Instance;
use state::*;
use statemachine::*;
use config::*;
use timer::*;

/// `MultiPaxos` receives messages and attempts to receive consensus on a replicated
/// value. Multiple instances of the paxos algorithm are chained together.
///
/// `ReplicatedState` is applied at each instance transition.
///
/// `ClientMessage` proposals start the process of replicating the value with
/// consensus from a majority. The `MultiPaxosMessage` values are sent out according
/// to the Paxos protocol to the other peers in the cluster.
///
/// `MultiPaxos` is both a `futures::Stream` and `futures::Sink`. It takes in messages
/// and produces messages for other actors within the system. The algorithm itself
/// is separated from any networking concerns.
pub struct MultiPaxos<R: ReplicatedState, S: Scheduler> {
    state_machine: R,
    state_handler: StateHandler,

    instance: Instance,
    paxos: PaxosInstance,
    config: Configuration,

    // downstream is sent out from this node
    downstream_sink: UnboundedSender<ClusterMessage>,
    downstream_stream: UnboundedReceiver<ClusterMessage>,

    // proposals received async
    proposal_sink: UnboundedSender<Value>,
    proposal_stream: UnboundedReceiver<Value>,

    // timers for driving resolution
    retransmit_timer: RetransmitTimer<S>,
    prepare_timer: InstanceResolutionTimer<S>,
    sync_timer: RandomPeerSyncTimer<S>,
}

impl<R: ReplicatedState, S: Scheduler> MultiPaxos<R, S> {
    /// Creates a new multi-paxos machine
    pub fn new(mut state_machine: R, scheduler: S, config: Configuration) -> MultiPaxos<R, S> {
        let mut state_handler = StateHandler::new();

        let state = state_handler.load().unwrap_or_default();
        let paxos = PaxosInstance::new(
            config.current(),
            config.quorum_size(),
            state.promised,
            state.accepted,
        );

        if let Some(v) = state.current_value.clone() {
            state_machine.apply_value(state.instance, v);
        }

        let (downstream_sink, downstream_stream) = unbounded::<ClusterMessage>();
        let (proposal_sink, proposal_stream) = unbounded::<Value>();

        let retransmit_timer = RetransmitTimer::new(scheduler.clone());
        let sync_timer = RandomPeerSyncTimer::new(scheduler.clone());
        let prepare_timer = InstanceResolutionTimer::new(scheduler);

        MultiPaxos {
            state_machine,
            state_handler,
            instance: state.instance,
            paxos,
            config,
            downstream_sink,
            downstream_stream,
            proposal_sink,
            proposal_stream,
            retransmit_timer,
            prepare_timer,
            sync_timer,
        }
    }

    /// Creates stream and sink for network messages.
    pub fn into_networked(self) -> NetworkedMultiPaxos<R, S> {
        NetworkedMultiPaxos { multipaxos: self }
    }

    /// Creates a sink for proposals
    pub fn proposal_sender(&self) -> ProposalSender {
        ProposalSender {
            sink: self.proposal_sink.clone(),
        }
    }

    /// Moves to the next instance with an accepted value
    fn advance_instance(&mut self, instance: Instance, value: Value) {
        self.state_machine.apply_value(instance, value.clone());

        let new_inst = instance + 1;
        self.instance = new_inst;

        info!("Starting instance {}", new_inst);
        self.state_handler.persist(State {
            instance: new_inst,
            current_value: Some(value),
            promised: None,
            accepted: None,
        });
        self.paxos =
            PaxosInstance::new(self.config.current(), self.config.quorum_size(), None, None);

        self.retransmit_timer.reset();
        self.prepare_timer.reset();
    }

    #[inline]
    fn send_multipaxos(&self, peer: NodeId, message: MultiPaxosMessage) {
        self.downstream_sink
            .unbounded_send(ClusterMessage { peer, message })
            .unwrap();
    }

    /// Broadcasts PREPARE messages to all peers
    fn send_prepare(&mut self, prepare: &Prepare) {
        let peers = self.config.peers();
        for peer in &peers {
            self.send_multipaxos(
                peer,
                MultiPaxosMessage::Prepare(self.instance, prepare.clone()),
            );
        }
    }

    /// Broadcasts ACCEPT messages to all peers
    fn send_accept(&mut self, accept: &Accept) {
        let peers = self.config.peers();
        for peer in &peers {
            self.send_multipaxos(
                peer,
                MultiPaxosMessage::Accept(self.instance, accept.clone()),
            );
        }
    }

    /// Broadcasts ACCEPTED messages to all peers
    fn send_accepted(&mut self, accepted: &Accepted) {
        let peers = self.config.peers();
        for peer in &peers {
            self.send_multipaxos(
                peer,
                MultiPaxosMessage::Accepted(self.instance, accepted.clone()),
            );
        }
    }

    fn propose_update(&mut self, value: Value) {
        let inst = self.instance;
        match self.paxos.propose_value(value) {
            Some(Either::Left(prepare)) => {
                info!("Starting Phase 1a with proposed value");
                self.send_prepare(&prepare);
                self.retransmit_timer.schedule(inst, Either::Left(prepare));
            }
            Some(Either::Right(accept)) => {
                info!("Starting Phase 2a with proposed value");
                self.send_accept(&accept);
                self.retransmit_timer.schedule(inst, Either::Right(accept));
            }
            None => {
                warn!("Alrady have a value during proposal phases");
            }
        }
    }

    fn poll_retransmit(&mut self, inst: Instance, msg: ProposerMsg) {
        if inst != self.instance {
            // TODO: assert?
            warn!("Retransmit for previous instance dropped");
            self.retransmit_timer.reset();
            return;
        }

        // resend prepare messages to peers
        match msg {
            Either::Left(ref v) => {
                debug!("Retransmitting {:?} to followers", v);
                self.send_prepare(v);
            }
            Either::Right(ref v) => {
                debug!("Retransmitting {:?} to followers", v);
                self.send_accept(v);
            }
        }
    }

    fn poll_restart_prepare(&mut self, instance: Instance) {
        if instance != self.instance {
            warn!("Restart prepare for previous instance dropped");
            self.prepare_timer.reset();
            return;
        }

        let prepare = self.paxos.prepare();
        info!("Restarting Phase 1 with {:?}", prepare.0);
        self.send_prepare(&prepare);
        self.retransmit_timer
            .schedule(instance, Either::Left(prepare));
    }

    fn poll_syncronization(&mut self) {
        if let Some(node) = self.config.random_peer() {
            debug!("Sending SYNC request");
            self.send_multipaxos(node, MultiPaxosMessage::Sync(self.instance));
        }
    }

    fn on_prepare(&mut self, peer: NodeId, inst: Instance, prepare: Prepare) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        match self.paxos.receive_prepare(peer, prepare) {
            Either::Left(promise) => {
                self.state_handler.persist(State {
                    instance: self.instance,
                    current_value: self.state_machine.snapshot(inst).clone(),
                    promised: Some(promise.message.0),
                    accepted: promise.message.1.clone(),
                });

                self.send_multipaxos(
                    promise.reply_to,
                    MultiPaxosMessage::Promise(self.instance, promise.message),
                );
            }
            Either::Right(reject) => {
                self.send_multipaxos(
                    reject.reply_to,
                    MultiPaxosMessage::Reject(self.instance, reject.message),
                );
            }
        }
    }

    fn on_promise(&mut self, peer: NodeId, inst: Instance, promise: Promise) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        if let Some(accept) = self.paxos.receive_promise(peer, promise) {
            self.send_accept(&accept);
            self.retransmit_timer.schedule(inst, Either::Right(accept));
        }
    }

    fn on_reject(&mut self, peer: NodeId, inst: Instance, reject: Reject) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        // go back to phase 1 when a quorum of REJECT has been received
        let prepare = self.paxos.receive_reject(peer, reject);
        if let Some(prepare) = prepare {
            self.send_prepare(&prepare);
            self.retransmit_timer.schedule(inst, Either::Left(prepare));
        } else {
            self.retransmit_timer.reset();
        }

        // schedule a retry of PREPARE with a higher ballot
        self.prepare_timer.schedule_retry(inst);
    }

    fn on_accept(&mut self, peer: NodeId, inst: Instance, accept: Accept) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        match self.paxos.receive_accept(peer, accept) {
            Either::Left(accepted) => {
                self.state_handler.persist(State {
                    instance: self.instance,
                    current_value: self.state_machine.snapshot(inst).clone(),
                    promised: Some(accepted.0),
                    accepted: Some((accepted.0, accepted.1.clone())),
                });

                self.send_accepted(&accepted);
            }
            Either::Right(reject) => {
                self.send_multipaxos(
                    reject.reply_to,
                    MultiPaxosMessage::Reject(self.instance, reject.message),
                );
            }
        }

        // if the proposer dies before receiving consensus, this node can
        // "pick up the ball" and receive consensus from a quorum of
        // the remaining selectors
        self.prepare_timer.schedule_timeout(inst);
    }

    fn on_accepted(&mut self, peer: NodeId, inst: Instance, accepted: Accepted) {
        // ignore previous or future instances
        if self.instance != inst {
            return;
        }

        let resol = self.paxos.receive_accepted(peer, accepted);

        // if there is quorum, we can advance to the next instance
        if let Some(Resolution(_, value)) = resol {
            self.advance_instance(inst, value);
        }
    }

    fn on_sync(&mut self, peer: NodeId, inst: Instance) {
        if self.instance <= inst {
            return;
        }

        // receives SYNC request from a peer to get the present value
        // if the instance known to the peer preceeds the current
        // known instance's value
        //
        // Why is this `self.instance - 1`?
        //
        // The catchup will send the current instance (which may be in-flight)
        // and the value from the last instance.
        if let Some(v) = self.state_machine.snapshot(self.instance - 1) {
            self.send_multipaxos(peer, MultiPaxosMessage::Catchup(self.instance, v));
        }
    }

    fn on_catchup(&mut self, inst: Instance, current: Value) {
        // only accept a catchup value if it is greater than
        // the current instance known to this node
        if inst > self.instance {
            // TODO: this call shouldn't have a random -1 without reason...
            // probably should fix advance_instance logic
            self.advance_instance(inst - 1, current);
        }
    }
}

impl<R: ReplicatedState, S: Scheduler> Sink for MultiPaxos<R, S> {
    type SinkItem = ClusterMessage;
    type SinkError = io::Error;

    fn start_send(&mut self, msg: ClusterMessage) -> StartSend<ClusterMessage, io::Error> {
        let ClusterMessage { peer, message } = msg;
        match message {
            MultiPaxosMessage::Prepare(inst, prepare) => {
                self.on_prepare(peer, inst, prepare);
            }
            MultiPaxosMessage::Promise(inst, promise) => {
                self.on_promise(peer, inst, promise);
            }
            MultiPaxosMessage::Accept(inst, accept) => {
                self.on_accept(peer, inst, accept);
            }
            MultiPaxosMessage::Accepted(inst, accepted) => {
                self.on_accepted(peer, inst, accepted);
            }
            MultiPaxosMessage::Reject(inst, reject) => {
                self.on_reject(peer, inst, reject);
            }
            MultiPaxosMessage::Sync(inst) => {
                self.on_sync(peer, inst);
            }
            MultiPaxosMessage::Catchup(inst, value) => {
                self.on_catchup(inst, value);
            }
        }

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        Ok(Async::Ready(()))
    }
}

#[inline]
fn from_poll<V>(s: Poll<Option<V>, io::Error>) -> io::Result<Option<V>> {
    match s {
        Ok(Async::Ready(Some(v))) => Ok(Some(v)),
        Ok(Async::Ready(None)) | Ok(Async::NotReady) => Ok(None),
        Err(e) => Err(e),
    }
}

impl<R: ReplicatedState, S: Scheduler> Stream for MultiPaxos<R, S> {
    type Item = ClusterMessage;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Option<ClusterMessage>, io::Error> {
        // poll for retransmission
        if let Some((inst, msg)) = from_poll(self.retransmit_timer.poll())? {
            self.poll_retransmit(inst, msg);
        }

        // poll for retry prepare
        if let Some(inst) = from_poll(self.prepare_timer.poll())? {
            self.poll_restart_prepare(inst);
        }

        // poll for sync
        if from_poll(self.sync_timer.poll())?.is_some() {
            self.poll_syncronization();
        }

        // poll for proposals
        match self.proposal_stream.poll() {
            Ok(Async::Ready(Some(value))) => self.propose_update(value),
            Err(_) => warn!("Error polling for proposals"),
            _ => {}
        }

        self.downstream_stream
            .poll()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "unexpected timer error"))
    }
}

pub struct NetworkedMultiPaxos<R: ReplicatedState, S: Scheduler> {
    multipaxos: MultiPaxos<R, S>,
}

impl<R: ReplicatedState, S: Scheduler> Sink for NetworkedMultiPaxos<R, S> {
    type SinkItem = NetworkMessage;
    type SinkError = io::Error;

    fn start_send(&mut self, msg: NetworkMessage) -> StartSend<NetworkMessage, io::Error> {
        let NetworkMessage { address, message } = msg;
        let peer = match self.multipaxos.config.peer_id(&address) {
            Some(v) => v,
            None => {
                warn!(
                    "Received message from address, but is not in configuration: {}",
                    msg.address
                );
                return Ok(AsyncSink::Ready);
            }
        };

        let send_res = self.multipaxos
            .start_send(ClusterMessage { peer, message })?;
        match send_res {
            AsyncSink::Ready => Ok(AsyncSink::Ready),
            AsyncSink::NotReady(ClusterMessage { message, .. }) => {
                Ok(AsyncSink::NotReady(NetworkMessage { address, message }))
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        self.multipaxos.poll_complete()
    }
}

impl<R: ReplicatedState, S: Scheduler> Stream for NetworkedMultiPaxos<R, S> {
    type Item = NetworkMessage;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<NetworkMessage>, io::Error> {
        loop {
            match try_ready!(self.multipaxos.poll()) {
                Some(ClusterMessage { peer, message }) => {
                    if let Some(address) = self.multipaxos.config.address(peer) {
                        return Ok(Async::Ready(Some(NetworkMessage { address, message })));
                    } else {
                        warn!("Unknown peer {:?}", peer);
                    }
                }
                None => {
                    return Ok(Async::Ready(None));
                }
            }
        }
    }
}

/// Sink allowing proposals to be sent to `MultiPaxos`.
#[derive(Clone)]
pub struct ProposalSender {
    sink: UnboundedSender<Value>,
}

impl ProposalSender {
    pub fn propose(&self, value: Value) -> io::Result<()> {
        self.sink.unbounded_send(value).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with unbounded sender for proposal",
            )
        })
    }
}

impl Sink for ProposalSender {
    type SinkItem = Value;
    type SinkError = io::Error;

    fn start_send(&mut self, value: Value) -> StartSend<Value, io::Error> {
        self.sink.start_send(value).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with start_send on unbounded sender for proposal",
            )
        })
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        self.sink.poll_complete().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Unexpected error with poll_complete on unbounded sender for proposal",
            )
        })
    }
}
