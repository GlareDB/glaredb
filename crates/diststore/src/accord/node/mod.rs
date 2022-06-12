pub mod coordinator;
pub mod replica;

use super::keys::Key;
use super::protocol::{Message, ProtocolMessage};
use super::topology::Address;
use super::{AccordError, Executor, NodeId, Result};
use coordinator::CoordinatorState;
use log::{debug, info};
use replica::ReplicaState;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Node<K> {
    node_id: NodeId,
    replica: ReplicaState<K>,
    coordinator: CoordinatorState<K>,
}

impl<K: Key> Node<K> {
    // pub async fn start(self)
}

struct StateDriver<K> {
    inbound: mpsc::Receiver<Message<K>>,
    outbound: mpsc::Sender<Message<K>>,
    replica: ReplicaState<K>,
    coordinator: CoordinatorState<K>,
}

impl<K: Key> StateDriver<K> {
    fn new(coordinator: CoordinatorState<K>, replica: ReplicaState<K>) {}

    async fn start<E>(mut self, executor: E) -> Result<()>
    where
        E: Executor<K>,
    {
        info!("starting state driver");
        while let Some(msg) = self.inbound.recv().await {
            debug!("received message: {:?}", msg);
            self.handle_msg(&executor, msg).await?;
        }
        Ok(())
    }

    async fn handle_msg<E>(&mut self, executor: &E, msg: Message<K>) -> Result<()>
    where
        E: Executor<K>,
    {
        use ProtocolMessage::*;

        let from = msg.from;
        match msg.proto_msg {
            BeginRead { keys, command } => {
                let msg = self.coordinator.new_read_tx(keys, command);
                self.send_outbound(Address::Peers, ProtocolMessage::PreAccept(msg))
                    .await?;
            }
            BeginWrite { keys, command } => {
                let msg = self.coordinator.new_write_tx(keys, command);
                self.send_outbound(Address::Peers, ProtocolMessage::PreAccept(msg))
                    .await?;
            }
            StartExecute(msg) => {
                let msg = self.coordinator.start_execute(msg)?;
                self.send_outbound(Address::Peers, ProtocolMessage::Read(msg))
                    .await?;
            }
            PreAccept(msg) => {
                let msg = self.replica.receive_preaccept(msg);
                self.send_outbound(Address::Peer(from), ProtocolMessage::PreAcceptOk(msg))
                    .await?;
            }
            PreAcceptOk(msg) => {
                let msg = self.coordinator.store_proposal(from, msg)?;
                match msg {
                    // Some(AcceptOrCommit::Accept(msg)) => {
                    //     self.send_outbound(Address::Peers, ProtocolMessage::Accept(msg))
                    //         .await?
                    // }
                    // Some(AcceptOrCommit::Commit(msg)) => {
                    //     let id = msg.tx.get_id().clone();
                    //     self.send_outbound(Address::Peers, ProtocolMessage::Commit(msg))
                    //         .await?;
                    //     self.send_outbound(
                    //         Address::Local,
                    //         ProtocolMessage::StartExecute(StartExecuteInternal { tx: id }),
                    //     )
                    //     .await?;
                    // }
                    _ => (), // Nothing to send yet.
                }
            }
            Accept(msg) => {
                let msg = self.replica.receive_accept(msg);
                self.send_outbound(Address::Peer(from), ProtocolMessage::AcceptOk(msg))
                    .await?;
            }
            AcceptOk(msg) => {
                let msg = self.coordinator.store_accept_ok(from, msg)?;
                if let Some(msg) = msg {
                    let id = msg.tx.get_id().clone();
                    self.send_outbound(Address::Peers, ProtocolMessage::Commit(msg))
                        .await?;
                    // self.send_outbound(
                    //     Address::Local,
                    //     ProtocolMessage::StartExecute(StartExecuteInternal { tx: id }),
                    // )
                    // .await?;
                }
            }
            Commit(msg) => {
                self.replica.receive_commit(msg);
            }
            Read(msg) => {
                let msg = self.replica.receive_read(msg)?;
                self.send_outbound(Address::Peer(from), ProtocolMessage::ReadOk(msg))
                    .await?;
            }
            ReadOk(msg) => {
                let msg = self.coordinator.store_read_ok(msg)?;
                if let Some(msg) = msg {
                    self.send_outbound(Address::Peers, ProtocolMessage::Apply(msg))
                        .await?;
                }
            }
            Apply(msg) => (), // self.replica.receive_apply(msg)?,
        };

        Ok(())
    }

    async fn send_outbound(&self, to: Address, msg: ProtocolMessage<K>) -> Result<()> {
        let msg = Message {
            from: self.replica.get_node_id(),
            to,
            proto_msg: msg,
        };
        match self.outbound.send(msg).await {
            Ok(_) => Ok(()),
            Err(msg) => Err(AccordError::OutboundSend(format!("msg: {:?}", msg))),
        }
    }
}
