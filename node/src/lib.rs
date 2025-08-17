mod gossip;
use anyhow::Result;
use libp2p::request_response::{
    self, Event as ReqResEvent, Message as ReqResMessage, ProtocolSupport,
};
use libp2p::StreamProtocol;
use libp2p::{
    futures::StreamExt,
    identify, identity, noise, ping,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tls, yamux, Multiaddr, PeerId, SwarmBuilder,
};
use std::collections::HashSet;
use tracing::info;

#[derive(NetworkBehaviour)]
pub struct NodeBehaviour {
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    gossip: libp2p::request_response::json::Behaviour<core::WireMsg, core::Ack>,
}

impl NodeBehaviour {
    fn new(local_public: &identity::PublicKey) -> Self {
        let identify = identify::Behaviour::new(identify::Config::new(
            "/gossimini/1.0.0".into(),
            local_public.clone(),
        ));
        let ping = ping::Behaviour::default();
        let protocols = [(
            StreamProtocol::new("/gossimini/req/1.0.0"),
            ProtocolSupport::Full,
        )];
        // Construct the gossip behaviour (replace with actual config as needed)
        let gossip = libp2p::request_response::json::Behaviour::new(
            protocols,
            request_response::Config::default(),
        );

        Self {
            identify,
            ping,
            gossip,
        }
    }
}

pub struct Node {
    pub peer_id: PeerId,
    pub swarm: Swarm<NodeBehaviour>,
}

impl Node {
    pub async fn new() -> Result<Self> {
        let swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                Default::default(),
                (tls::Config::new, noise::Config::new),
                yamux::Config::default,
            )?
            .with_quic()
            .with_behaviour(|keypair| NodeBehaviour::new(&keypair.public()))?
            .build();
        let peer_id = *swarm.local_peer_id();
        Ok(Self { peer_id, swarm })
    }

    pub fn listen_on(&mut self, addr: Multiaddr) -> Result<()> {
        self.swarm.listen_on(addr)?;
        Ok(())
    }

    pub async fn run(mut self) -> Result<()> {
        let mut peers: HashSet<PeerId> = HashSet::new();
        loop {
            match self.swarm.select_next_some().await {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Address: {}", address);
                }
                SwarmEvent::Behaviour(ev) => {
                    println!("ev: {:?}", ev);
                    match ev {
                        NodeBehaviourEvent::Gossip(gossip_ev) => {
                            match gossip_ev {
                                ReqResEvent::Message {
                                    peer,
                                    connection_id: _,
                                    message,
                                } => {
                                    match message {
                                        ReqResMessage::Request {
                                            request_id: _,
                                            request,
                                            channel,
                                        } => {
                                            match &request {
                                                core::WireMsg::Publish {
                                                    topic,
                                                    msg_id,
                                                    data,
                                                } => {
                                                    info!("Received Publish: topic={:?}, data_len={:?}", topic, data);
                                                    // Ack
                                                    let ack = core::Ack { ok: true };
                                                    self.swarm
                                                        .behaviour_mut()
                                                        .gossip
                                                        .send_response(channel, ack)
                                                        .expect("send response");
                                                    // Broadcast to all other connected peers
                                                    let peers_to_notify: Vec<_> = self
                                                        .swarm
                                                        .connected_peers()
                                                        .cloned()
                                                        .collect();
                                                    for p in peers_to_notify {
                                                        if p != peer {
                                                            self.swarm
                                                                .behaviour_mut()
                                                                .gossip
                                                                .send_request(
                                                                    &p,
                                                                    core::WireMsg::Publish {
                                                                        topic: topic.clone(),
                                                                        msg_id: msg_id.clone(),
                                                                        data: data.clone(),
                                                                    },
                                                                );
                                                        }
                                                    }
                                                }
                                                core::WireMsg::Subscribe { topic } => {
                                                    let _request = core::WireMsg::Subscribe {
                                                        topic: topic.clone(),
                                                    };
                                                    let ack = core::Ack { ok: true };
                                                    self.swarm
                                                        .behaviour_mut()
                                                        .gossip
                                                        .send_response(channel, ack)
                                                        .expect("send response");
                                                }
                                                _ => {
                                                    // Default: Ack for other messages
                                                    let ack = core::Ack { ok: true };
                                                    self.swarm
                                                        .behaviour_mut()
                                                        .gossip
                                                        .send_response(channel, ack)
                                                        .expect("send response");
                                                }
                                            }
                                        }
                                        ReqResMessage::Response {
                                            request_id,
                                            response,
                                        } => {
                                            info!(
                                                "Received response: {:?} for request_id: {:?}",
                                                response, request_id
                                            );
                                            // Handle the response as needed
                                        }
                                    }
                                }
                                ReqResEvent::OutboundFailure {
                                    peer,
                                    request_id,
                                    connection_id,
                                    error,
                                } => {
                                    info!("OutboundFailure: peer={:?}, request_id={:?}, connection_id={:?}, error={:?}", peer, request_id, connection_id, error);
                                }
                                ReqResEvent::InboundFailure {
                                    peer,
                                    request_id,
                                    connection_id,
                                    error,
                                } => {
                                    info!("InboundFailure: peer={:?}, request_id={:?}, connection_id={:?}, error={:?}", peer, request_id, connection_id, error);
                                }
                                ReqResEvent::ResponseSent {
                                    peer,
                                    request_id,
                                    connection_id: _,
                                } => {
                                    info!(
                                        "ResponseSent: peer={:?}, request_id={:?}",
                                        peer, request_id
                                    );
                                }
                            }
                        }
                        NodeBehaviourEvent::Identify(ev) => {
                            info!("Identify event: {:?}", ev);
                        }
                        NodeBehaviourEvent::Ping(ev) => {
                            info!("Ping event: {:?}", ev);
                        }
                    }
                }
                SwarmEvent::ConnectionEstablished {
                    peer_id,
                    connection_id: _,
                    endpoint,
                    num_established: _,
                    concurrent_dial_errors: _,
                    established_in: _,
                } => {
                    info!("peer-id: {:?}", peer_id);
                    if endpoint.is_dialer() && peers.insert(peer_id) {
                        let subscribe_msg = core::WireMsg::Subscribe {
                            topic: core::TopicId::new("news"),
                        };
                        self.swarm
                            .behaviour_mut()
                            .gossip
                            .send_request(&peer_id, subscribe_msg);

                        let publish = core::WireMsg::Publish {
                            topic: core::TopicId::new("news"),
                            msg_id: core::MsgId::from_bytes([1; 32]),                            // If Payload is a type alias (type Payload = Vec<u8>), use just .to_vec():
                            data: core::Payload(b"hello world".to_vec()),
                        };
                        self.swarm
                            .behaviour_mut()
                            .gossip
                            .send_request(&peer_id, publish);
                    }
                }
                other => {
                    info!(?other, "swarm");
                }
            }
        }
    }

    pub fn dial(&mut self, addr: Multiaddr) -> Result<()> {
        self.swarm.dial(addr)?;
        Ok(())
    }
}
