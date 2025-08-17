pub struct GossipProtocol;

// Define the ProtocolName trait if not already defined elsewhere
pub trait ProtocolName {
    fn protocol_name() -> &'static [u8];
}

impl ProtocolName for GossipProtocol {
    fn protocol_name() -> &'static [u8] {
        b"/gossimini/req/1.0.0"
    }
}
pub struct GossipCodec;

pub type GossipRequest = Vec<u8>;
pub type GossipResponse = Vec<u8>;

pub fn to_req(msg: &core::WireMsg) -> GossipRequest {
    core::codec::encode(msg)
}
pub fn from_req(bytes: &[u8]) -> Result<core::WireMsg, core::codec::DecodeError> {
    core::codec::decode(bytes)
}
