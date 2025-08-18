use lru;

use crate::MsgId;
pub struct SeenCache {
    cache: lru::LruCache<MsgId, ()>,
}

impl SeenCache {
    pub fn new(capacity: usize) -> Self {
        if capacity < 1 {
            println!("incorrect capacity");
        }
        SeenCache {
            cache: lru::LruCache::new(
                std::num::NonZeroUsize::new(capacity).expect("capacity must be non-zero"),
            ),
        }
    }
    pub fn insert(&mut self, id: &MsgId) -> bool {
        if self.cache.get(id).is_some() {
            return false;
        }
        self.cache.put(id.clone(), ());
        true
    }
}
