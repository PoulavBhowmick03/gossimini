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
use libp2p_request_response::{Behaviour, Codec};
use std::fmt;
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
        loop {
            match self.swarm.select_next_some().await {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Address: {}", address);
                }
                SwarmEvent::Behaviour(ev) => {
                    println!("ev: {:?}", ev);
                }
                SwarmEvent::ConnectionEstablished { peer_id, connection_id, endpoint, num_established, concurrent_dial_errors, established_in } => {
                    info!("peer-id: {:?}", peer_id)
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
