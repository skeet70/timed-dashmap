use std::borrow::Borrow;
use std::cmp::Eq;
use std::collections::hash_map::RandomState;
use std::hash::Hash;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

/// # Safety
///
/// Requires that you ensure the reference does not become invalid.
/// The object has to outlive the reference.
pub unsafe fn change_lifetime_const<'a, 'b, T>(x: &'a T) -> &'b T {
    &*(x as *const T)
}

/// # Safety
///
/// Requires that you ensure the reference does not become invalid.
/// The object has to outlive the reference.
pub unsafe fn change_lifetime_mut<'a, 'b, T>(x: &'a mut T) -> &'b mut T {
    &mut *(x as *mut T)
}

pub struct ExpiringValue<V> {
    value: V,
    expiration: Instant,
}

/// A concurrent cache that expires entries after a given duration. The `start` function must be called
/// to kick off the background task responsible for reaping expired entries.
/// WARNING: likely unsafe with the current implementations of `iter`, `iter_mut`, and `get`.
pub struct ExpiringDashMap<K, V> {
    cache: Arc<DashMap<K, ExpiringValue<V>, RandomState>>,
    expiry: Duration,
    reaper_thread_handle: Option<Arc<JoinHandle<()>>>,
}

pub struct Iter<'a, K, V> {
    iter: dashmap::iter::Iter<'a, K, ExpiringValue<V>>,
}

impl<'a, K: 'static + Hash + Eq + Send + Sync, V: 'static + Send + Sync> Iterator
    for Iter<'a, K, V>
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .filter(|multiref| multiref.value().expiration > Instant::now())
            .map(|multiref| {
                // TODO: DRAGONS
                unsafe {
                    let k = change_lifetime_const(multiref.key());
                    let v = change_lifetime_const(&multiref.value().value);
                    (k, v)
                }
            })
    }
}

pub struct IterMut<'a, K, V> {
    iter: dashmap::iter::IterMut<'a, K, ExpiringValue<V>>,
}

impl<'a, K: 'static + Hash + Eq + Send + Sync, V: 'static + Send + Sync> Iterator
    for IterMut<'a, K, V>
{
    type Item = (&'a K, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .filter(|multiref| multiref.value().expiration > Instant::now())
            .map(|mut multiref| {
                // TODO: DRAGONS
                unsafe {
                    let k = change_lifetime_const(multiref.key());
                    let v = change_lifetime_mut(&mut multiref.value_mut().value);
                    (k, v)
                }
            })
    }
}

impl<K: Hash + Eq + Send + Sync, V: Send + Sync> Clone for ExpiringDashMap<K, V> {
    fn clone(&self) -> Self {
        ExpiringDashMap {
            cache: self.cache.clone(),
            expiry: self.expiry,
            reaper_thread_handle: self.reaper_thread_handle.clone(),
        }
    }
}

impl<'a, K: 'static + Hash + Eq + Send + Sync, V: 'static + Send + Sync> ExpiringDashMap<K, V> {
    pub fn new(expiry: Duration) -> Self {
        ExpiringDashMap {
            cache: Arc::new(DashMap::new()),
            expiry,
            reaper_thread_handle: None,
        }
    }

    /// Start the reaper thread, which will periodically check for expired entries and remove them.
    pub fn start(&mut self, reaper_interval: Duration) {
        self.reaper_thread_handle = Some(Arc::new(run(self.clone(), reaper_interval)));
    }

    /// Remove all expired entries from the cache.
    pub fn remove_expired(&self, now: Instant) {
        self.cache.retain(|_, v| v.expiration > now);
    }

    /// Inserts a key and a value into the map. Returns the old value associated with the key if there was one and it was unexpired.
    ///
    /// **Locking behaviour:** May deadlock if called when holding any sort of reference into the map.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.cache
            .insert(
                key,
                ExpiringValue {
                    value,
                    expiration: Instant::now() + self.expiry,
                },
            )
            .filter(|ev| ev.expiration < Instant::now())
            .map(|ev| ev.value)
    }

    /// Removes an entry from the map, returning the key and value if they existed in the map and the value was unexpired.
    ///
    /// **Locking behaviour:** May deadlock if called when holding any sort of reference into the map.
    pub fn remove<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache
            .remove(key)
            .filter(|(_, ev)| ev.expiration <= Instant::now())
            .map(|(k, ev)| (k, ev.value))
    }

    /// Removes an entry from the map, returning the key and value
    /// if the entry existed and the provided conditional function returned true.
    ///
    /// **Locking behaviour:** May deadlock if called when holding any sort of reference into the map.
    pub fn remove_if<Q>(&self, key: &Q, f: impl FnOnce(&K, &V) -> bool) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache
            .get(key)
            .filter(|mr| f(mr.key(), &mr.value().value))
            .filter(|mr| mr.value().expiration > Instant::now())
            .map(|_| self.cache.remove(key))
            .flatten()
            .map(|(k, ev)| (k, ev.value))
    }

    /// Creates an iterator over a DashMap yielding immutable references.
    pub fn iter(&self) -> Iter<K, V> {
        Iter {
            iter: self.cache.iter(),
        }
    }

    /// Iterator over a DashMap yielding mutable references.
    ///
    /// **Locking behaviour:** May deadlock if called when holding any sort of reference into the map.
    pub fn iter_mut(&self) -> IterMut<K, V> {
        IterMut {
            iter: self.cache.iter_mut(),
        }
    }

    /// Get a immutable reference to an unexpired entry in the map
    ///
    /// **Locking behaviour:** May deadlock if called when holding a mutable reference into the map.
    pub fn get<Q>(&'a self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache
            .get(key)
            .filter(|r| r.value().expiration > Instant::now())
            .map(|r| unsafe {
                // TODO: DRAGONS
                change_lifetime_const(&r.value().value)
            })
    }

    /// Retain elements that whose predicates return true
    /// and discard elements whose predicates return false.
    ///
    /// **Locking behaviour:** May deadlock if called when holding any sort of reference into the map.
    pub fn retain(&self, mut f: impl FnMut(&K, &mut V) -> bool) {
        self.cache
            .retain(|k, ev| ev.expiration > Instant::now() && f(k, &mut ev.value))
    }

    /// Fetches the total number of key-value pairs stored in the map.
    ///
    /// **Locking behaviour:** May deadlock if called when holding a mutable reference into the map.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Checks if the map is empty or not.
    ///
    /// **Locking behaviour:** May deadlock if called when holding a mutable reference into the map.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Removes all key-value pairs in the map.
    ///
    /// **Locking behaviour:** May deadlock if called when holding any sort of reference into the map.
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Returns how many key-value pairs the map can store without reallocating.
    pub fn capacity(&self) -> usize {
        self.cache.capacity()
    }
}

fn run<K, V>(cache: ExpiringDashMap<K, V>, reaper_interval: Duration) -> JoinHandle<()>
where
    K: 'static + Hash + Eq + Send + Sync,
    V: 'static + Send + Sync,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(reaper_interval);
        loop {
            let now = interval.tick().await;
            cache.remove_expired(now);
        }
    })
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{pause, advance, resume, sleep};

    #[tokio::test]
    async fn it_works() {
        let mut cache = ExpiringDashMap::new(Duration::from_secs(1));
        cache.start(Duration::from_millis(500));
        cache.insert("key", "value");
        
        pause();
        
        assert_eq!(cache.get("key"), Some(&"value"));
        
        advance(Duration::from_secs(1)).await;
        resume();

        // give the reaper a moment to tick and run
        sleep(Duration::from_millis(1)).await;

        // even before we've made a call that may prune, the entry has already been evicted
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.get("key"), None);
    }
}
