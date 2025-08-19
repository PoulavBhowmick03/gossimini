mod gossip;
use anyhow::Result;
use core::{TopicId, WireMsg};
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
use std::collections::{HashMap, HashSet};
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
    pub seen: core::SeenCache,
    pub local_subs: HashSet<TopicId>,
    pub peer_subs: HashMap<TopicId, HashSet<PeerId>>,
    pub autosub: Option<core::TopicId>,
    pub autopub: Option<(core::TopicId, Vec<u8>)>,
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
        let seen = core::SeenCache::new(4096);
        let local_subs = [core::TopicId::new("news")].into_iter().collect();
        let peer_subs = std::collections::HashMap::new();

        Ok(Self {
            peer_id,
            swarm,
            seen,
            local_subs,
            peer_subs,
            autosub: None,
            autopub: None,
        })
    }
    pub fn set_autosub(&mut self, topic: core::TopicId) {
        self.autosub = Some(topic);
    }

    pub fn set_autopub(&mut self, topic: core::TopicId, data: Vec<u8>) {
        self.autopub = Some((topic, data));
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
                                                    let computed =
                                                        core::MsgId::from_data(data.0.as_slice()); // or data.as_slice()
                                                    let is_valid = computed == *msg_id;
                                                    let is_new =
                                                        is_valid && self.seen.insert(&computed);

                                                    self.swarm
                                                        .behaviour_mut()
                                                        .gossip
                                                        .send_response(
                                                            channel,
                                                            core::Ack { ok: is_valid },
                                                        )
                                                        .expect("send response");

                                                    if is_new {
                                                        let targets = self.subscribed_peers(&topic);
                                                        for p in targets {
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
                                                }
                                                core::WireMsg::Subscribe { topic } => {
                                                    self.peer_subscribe(peer, topic.clone());
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
                                                core::WireMsg::Unsubscribe { topic } => {
                                                    self.peer_unsubscribe(peer, &topic.clone());
                                                    let _request = WireMsg::Unsubscribe {
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
                        // Existing logging
                        if let Some(topic) = self.autosub.clone() {
                            let subscribe_msg = core::WireMsg::Subscribe {
                                topic: topic.clone(),
                            };
                            self.swarm
                                .behaviour_mut()
                                .gossip
                                .send_request(&peer_id, subscribe_msg);
                            self.local_subs.insert(topic);
                        }
                        if let Some((topic, data)) = self.autopub.clone() {
                            let id = core::MsgId::from_data(&data);
                            let publish = core::WireMsg::Publish {
                                topic: topic.clone(),
                                msg_id: id,
                                data: core::Payload(data.clone()),
                            };
                            self.swarm
                                .behaviour_mut()
                                .gossip
                                .send_request(&peer_id, publish);
                        }
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

    pub fn peer_subscribe(&mut self, peer: PeerId, topic: TopicId) {
        self.peer_subs.entry(topic).or_default().insert(peer);
    }
    pub fn peer_unsubscribe(&mut self, _peer: PeerId, topic: &TopicId) {
        self.peer_subs.remove(topic);
    }
    pub fn subscribed_peers(&self, topic: &core::TopicId) -> Vec<PeerId> {
        self.peer_subs
            .get(topic)
            .map(|peers| peers.iter().cloned().collect())
            .unwrap_or_default()
    }
}
