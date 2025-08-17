use std::collections::HashSet;
pub use std::fmt;

use serde::{Deserialize, Serialize};

#[serde(transparent)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq)]
struct TopicId(String);

impl TopicId {
    pub fn new<S: Into<String>>(s: S) -> Self {
        TopicId(s.into())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MsgId([u8; 32]);

impl MsgId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        MsgId(bytes)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Eq)]
struct Payload(Vec<u8>);

pub struct SeenCache {
    pub seen: HashSet<MsgId>,
    pub capacity: usize,
}

impl SeenCache {
    pub fn new(capacity: usize) -> Self {
        SeenCache {
            seen: HashSet::with_capacity(capacity),
            capacity,
        }
    }
    pub fn insert(&mut self, id: &MsgId) -> bool {
        if self.seen.contains(id) {
            false
        } else {
            if self.seen.len() >= self.capacity {
                self.seen.clear();
            }
            self.seen.insert(*id)
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WireMsg {
    Subscribe {
        topic: TopicId,
    },
    Unsubscribe {
        topic: TopicId,
    },
    Publish {
        topic: TopicId,
        msg_id: MsgId,
        data: Payload,
    },
}

pub mod codec {
    use super::WireMsg;
    pub type DecodeError = serde_json::Error;

    pub fn encode(msg: &WireMsg) -> Vec<u8> {
        serde_json::to_vec(msg).expect("Failed to serialize WireMsg")
    }
    pub fn decode(bytes: &[u8]) -> Result<WireMsg, DecodeError> {
        serde_json::from_slice(bytes)
    }
}

#[derive(Serialize,Deserialize,Debug,Clone,PartialEq,Eq)]
pub struct Ack {
    pub ok : bool,
}

#[cfg(test)]
mod tests {
    use core::error;

    use crate::codec::{decode, encode};

    use super::*;

    #[test]
    fn basic_pub_seen_cache() {
        let topic = TopicId::new("room1");
        let msg_id = MsgId::from_bytes([0; 32]);
        let data = vec![1, 2, 3];
        let m1 = WireMsg::Publish {
            topic: topic.clone(),
            msg_id,
            data: Payload(data.clone()),
        };
        let m2 = WireMsg::Publish {
            topic: topic.clone(),
            msg_id,
            data: Payload(data.clone()),
        };
        if let WireMsg::Publish {
            topic: t1,
            data: d1,
            ..
        } = m1.clone()
        {
            if let WireMsg::Publish {
                topic: t2,
                data: d2,
                ..
            } = m2.clone()
            {
                assert_eq!(t1, t2);
                assert_eq!(d1, d2);
            } else {
                panic!("m2 not Publish");
            }
        } else {
            panic!("m1 not Publish");
        }

        // SeenCache duplicate detection
        let mut cache = SeenCache::new(10);
        assert_eq!(cache.insert(&msg_id), true); // first time
        assert_eq!(cache.insert(&msg_id), false); // duplicate
    }

    #[test]
    fn test_coded() -> Result<(), Box<dyn error::Error>> {
        let topic = TopicId::new("news");
        let msg_id = MsgId::from_bytes([0; 32]);
        let data = vec![10];
        let msg = WireMsg::Publish {
            topic: topic.clone(),
            msg_id,
            data: Payload(data.clone()),
        };
        let bytes = encode(&msg);
        let back = decode(&bytes)?;
        assert_eq!(msg, back);
        Ok(())
    }
}
