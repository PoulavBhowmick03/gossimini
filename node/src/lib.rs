use anyhow::Result;
use libp2p::{
    Multiaddr, PeerId, SwarmBuilder,
    futures::StreamExt,
    identify, identity, ping,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
};
use tracing::info;

#[derive(NetworkBehaviour)]
pub struct NodeBehaviour {
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

impl NodeBehaviour {
    fn new(local_public: &identity::PublicKey) -> Self {
        let identify = identify::Behaviour::new(identify::Config::new(
            "/gossimini/1.0.0".into(),
            local_public.clone(),
        ));
        let ping = ping::Behaviour::default();
        Self { identify, ping }
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
                (libp2p_tls::Config::new, libp2p_noise::Config::new),
                libp2p_yamux::Config::default,
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
                other => {
                    info!(?other, "swarm");
                }
            }
        }
    }
}
