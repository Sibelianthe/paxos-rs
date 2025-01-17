use crate::{
    commands,
    commands::HttpTransport,
    kvstore::{KeyValueStore, KvCommand},
};
use bytes::Bytes;
use hyper::{Body, Method, Request, Response, StatusCode};
use paxos::{
    statemachine::StateMachineReplica, Command, CommandMetas, Configuration, Node, Receiver,
    Replica,
};
use rand::random;
use std::{sync::Arc, time::Duration};
use tokio::{self, sync::Mutex, task::JoinHandle, time::interval};

type PaxosReplica = StateMachineReplica<Node<HttpTransport>, KeyValueStore>;

#[derive(Clone)]
pub struct Handler {
    replica: Arc<Mutex<PaxosReplica>>,
    store: KeyValueStore,
}

impl Handler {
    pub fn new(config: Configuration) -> Handler {
        let store = KeyValueStore::default();
        let replica = Node::new(HttpTransport::default(), config).state_machine(store.clone());
        Handler { replica: Arc::new(Mutex::new(replica)), store }
    }

    pub fn spawn_timers(&self) -> JoinHandle<()> {
        let store = self.store.clone();
        let listener_cleanup = tokio::spawn(async move {
            let mut ticks = interval(Duration::new(30, 0));
            loop {
                ticks.tick().await;
                store.prune_listeners();
            }
        });

        listener_cleanup
    }

    pub async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let path = Bytes::from(req.uri().path()[1..].to_string());
        match (req.method(), path) {
            (&Method::POST, key) if key == "paxos" => {
                {
                    let cmd = hyper::body::to_bytes(req.into_body()).await?;
                    let mut replica = self.replica.lock().await;
                    commands::invoke::<PaxosReplica>(&mut replica, cmd);
                }

                respond(StatusCode::ACCEPTED)
            }
            (&Method::POST, key) => {
                let value = hyper::body::to_bytes(req.into_body()).await?;
                let request_id = random();
                let receiver = self.store.register_set(request_id);
                {
                    self.replica.lock().await.receive(
                        Command::Proposal {
                            payload: KvCommand::Set { request_id, key, value }.into(),
                        },
                        CommandMetas("".into()),
                    );
                }

                match receiver.await {
                    Ok(slot) => Ok(Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .header("X-Paxos-Slot", slot)
                        .body(Body::empty())
                        .unwrap()),
                    Err(_) => respond(StatusCode::INTERNAL_SERVER_ERROR),
                }
            }
            (&Method::GET, key) => {
                let request_id = random::<u64>();
                let receiver = self.store.register_get(request_id);
                {
                    self.replica.lock().await.receive(
                        Command::Proposal { payload: (KvCommand::Get { request_id, key }.into()) },
                        CommandMetas("".into()),
                    );
                }

                match receiver.await {
                    Ok(Some((slot, value))) => Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("X-Paxos-Slot", slot)
                        .body(value.into())
                        .unwrap()),
                    Ok(None) => respond(StatusCode::NOT_FOUND),
                    Err(_) => respond(StatusCode::INTERNAL_SERVER_ERROR),
                }
            }
            (_, key) if key == "paxos" => respond(StatusCode::METHOD_NOT_ALLOWED),
            _ => respond(StatusCode::NOT_FOUND),
        }
    }
}

fn respond(code: StatusCode) -> Result<Response<Body>, hyper::Error> {
    let mut resp = Response::default();
    *resp.status_mut() = code;
    Ok(resp)
}
