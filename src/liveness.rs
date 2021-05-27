use crate::{
    commands::{Command, CommandMetas, Receiver},
    window::DecisionSet,
    Replica,
};
use std::time::{Duration, Instant};

/// Adds liveness to a commander by taking leadership
/// when a timeout occurs
pub struct Liveness<R: Replica> {
    inner: R,
    leader_election: Timeout,
}

impl<R: Replica> Liveness<R> {
    pub(crate) fn new(inner: R) -> Liveness<R> {
        Liveness {
            inner,
            // TODO: configurable leadership election timeout
            leader_election: Timeout::new(Duration::from_secs(2)),
        }
    }
}

impl<R: Replica> Receiver for Liveness<R> {
    fn receive(&mut self, cmd: Command, cmd_metas: CommandMetas) {
        // Bump leadership timeout if the command is not a catchup or proposal
        match &cmd {
            &Command::Proposal { payload: _} | &Command::Catchup { payload: (..)} => {}
            _ => self.leader_election.bump(),
        }

        self.inner.receive(cmd, cmd_metas);
    }
}

impl<R: Replica> Replica for Liveness<R> {
    fn tick(&mut self, cmd_metas: CommandMetas) {
        let lapsed = if self.inner.is_leader() {
            self.leader_election.near()
        } else {
            self.leader_election.lapsed()
        };

        if lapsed {
            info!("Leadership timeout lapsed, proposing leadership");
            self.inner.propose_leadership(cmd_metas.clone());
            self.leader_election.clear();
        }

        self.inner.tick(cmd_metas);
    }

    fn propose_leadership(&mut self, cmd_metas: CommandMetas) {
        self.inner.propose_leadership(cmd_metas);
    }

    fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }

    fn decisions(&self) -> DecisionSet {
        self.inner.decisions()
    }
}

struct Timeout {
    latest_message: Option<Instant>,
    timeout: Duration,
}

impl Timeout {
    fn new(timeout: Duration) -> Timeout {
        Timeout { latest_message: None, timeout }
    }

    fn clear(&mut self) {
        self.latest_message = None;
    }

    fn bump(&mut self) {
        trace!("Leadership timeout bumped");
        self.latest_message = Some(Instant::now());
    }

    fn lapsed(&self) -> bool {
        if let Some(latest) = self.latest_message {
            Instant::now() > latest + self.timeout
        } else {
            false
        }
    }

    fn near(&self) -> bool {
        if let Some(latest) = self.latest_message {
            Instant::now() > latest + (self.timeout / 2)
        } else {
            false
        }
    }

    #[cfg(test)]
    fn fast_forward(&mut self, d: Duration) {
        // "jump" in time for tests by setting the clock back
        self.latest_message = self.latest_message.take().map(|i| i - d - Duration::from_nanos(10));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{commands::Command, Ballot};

    #[test]
    fn propose_does_not_bump_timeout() {
        let mut live = Liveness::new(Inner::default());
        let cmd_metas = CommandMetas("".into());

        live.receive(Command::Proposal { payload: ("123".into())}, cmd_metas);

        // does not bump leadership
        assert!(live.leader_election.latest_message.is_none());
        assert_eq!(Command::Proposal { payload: ("123".into())}, live.inner.commands[0]);
    }

    #[test]
    fn commands_bump_timeout() {
        let mut live = Liveness::new(Inner::default());
        let cmd_metas = CommandMetas("".into());

        live.receive(Command::Prepare { payload: (Ballot(2, 3))}, cmd_metas.clone());
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Prepare { payload: (Ballot(2, 3))});

        let mut live = Liveness::new(Inner::default());
        live.receive(Command::Promise { payload: (0, Ballot(2, 3), vec![])}, cmd_metas.clone());
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Promise { payload: (0, Ballot(2, 3), vec![])});

        let mut live = Liveness::new(Inner::default());
        live.receive(Command::Reject { payload: (4, Ballot(0, 1), Ballot(4, 5))}, cmd_metas.clone());
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Reject { payload: (4, Ballot(0, 1), Ballot(4, 5))});

        let mut live = Liveness::new(Inner::default());
        live.receive(Command::Accept { payload: (Ballot(4, 5), vec![])}, cmd_metas.clone());
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Accept { payload: (Ballot(4, 5), vec![])});

        let mut live = Liveness::new(Inner::default());
        live.receive(Command::Accepted { payload: (5, Ballot(1, 2), vec![2, 3, 4])}, cmd_metas.clone());
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Accepted { payload: (5, Ballot(1, 2), vec![2, 3, 4])});

        let mut live = Liveness::new(Inner::default());
        live.receive(Command::Resolution { payload: (Ballot(1, 2), vec![])}, cmd_metas.clone());
        assert!(live.leader_election.latest_message.is_some());
        assert_eq!(live.inner.commands[0], Command::Resolution { payload: (Ballot(1, 2), vec![])});
    }

    #[test]
    fn tick_leader() {
        let mut live = Liveness::new(Inner::default());
        let cmd_metas = CommandMetas("".into());

        live.inner.leader = true;
        assert!(live.is_leader());
        live.tick(cmd_metas.clone());
        assert!(!live.inner.proposed_leadership);

        // receive a message
        live.receive(Command::Accepted { payload: (5, Ballot(1, 2), vec![2, 3, 4])}, cmd_metas.clone());
        live.tick(cmd_metas.clone());
        assert!(!live.inner.proposed_leadership);

        // jump forward the timeout duration
        live.leader_election.fast_forward(Duration::from_secs(1));

        live.tick(cmd_metas);
        assert!(live.inner.proposed_leadership);
    }

    #[test]
    fn tick_follower() {
        let mut live = Liveness::new(Inner::default());
        let cmd_metas = CommandMetas("".into());

        live.inner.leader = false;
        assert!(!live.is_leader());
        live.tick(cmd_metas.clone());
        assert!(!live.inner.proposed_leadership);

        // receive a message
        live.receive(Command::Resolution { payload: (Ballot(0, 1), vec![])}, cmd_metas.clone());
        live.tick(cmd_metas.clone());
        assert!(!live.inner.proposed_leadership);

        // jump forward the timeout duration
        live.leader_election.fast_forward(Duration::from_secs(2));

        live.tick(cmd_metas);
        assert!(live.inner.proposed_leadership);
    }

    #[derive(Default)]
    struct Inner {
        commands: Vec<Command>,
        leader: bool,
        proposed_leadership: bool,
    }

    impl Receiver for Inner {
        fn receive(&mut self, cmd: Command, _cmd_metas: CommandMetas) {
            self.commands.push(cmd);
        }
    }

    impl Replica for Inner {
        fn propose_leadership(&mut self, _cmd_metas: CommandMetas) {
            self.proposed_leadership = true;
        }

        fn is_leader(&self) -> bool {
            self.leader
        }

        fn tick(&mut self, _cmd_metas: CommandMetas) {}
        fn decisions(&self) -> DecisionSet {
            unimplemented!();
        }
    }
}
