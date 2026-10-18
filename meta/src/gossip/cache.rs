use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

const DEFAULT_SEEN_CACHE_TTL_SECS: u64 = 60;
const DEFAULT_SEEN_CACHE_CAPACITY: usize = 10_000;

pub struct SeenCache {
    seen: HashSet<String>,
    order: VecDeque<(String, Instant)>,
    ttl: Duration,
    max_capacity: usize,
}

impl SeenCache {
    pub fn new(ttl: Duration, max_capacity: usize) -> Self {
        Self {
            seen: HashSet::new(),
            order: VecDeque::new(),
            ttl,
            max_capacity,
        }
    }

    pub fn with_default() -> Self {
        Self::new(
            Duration::from_secs(DEFAULT_SEEN_CACHE_TTL_SECS),
            DEFAULT_SEEN_CACHE_CAPACITY,
        )
    }

    /// メッセージが既に処理済みかどうかを判定し、未処理なら記録して true を返す（初回受信なら true、既読なら false）
    pub fn check_and_insert(&mut self, id: &str) -> bool {
        self.cleanup();

        if self.seen.contains(id) {
            return false;
        }

        if self.seen.len() >= self.max_capacity
            && let Some((old_id, _)) = self.order.pop_front()
        {
            self.seen.remove(&old_id);
        }

        let now = Instant::now();
        self.seen.insert(id.to_string());
        self.order.push_back((id.to_string(), now));
        true
    }

    pub fn contains(&self, id: &str) -> bool {
        self.seen.contains(id)
    }

    fn cleanup(&mut self) {
        let now = Instant::now();
        while let Some((_, timestamp)) = self.order.front() {
            if now.duration_since(*timestamp) > self.ttl {
                if let Some((old_id, _)) = self.order.pop_front() {
                    self.seen.remove(&old_id);
                }
            } else {
                break;
            }
        }
    }
}
