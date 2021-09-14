# timed-dashmap
A timed expiration map wrapper around the performant and concurrent DashMap. Automatically creates a reaping thread so
memory gets cleared quickly after an entry expires, instead of waiting for an access that passively clears it.

```
// create the map
let mut minute_cache = ExpiringDashMap::new(Duration::from_secs(60));

// run `start` to kick off the reaper thread
minute_cache.start(Duration::from_millis(500));

// add a value
minute_cache.insert("key", "value");

// see that it's there
minute_cache.len();
> 1
minute_cache.get("key");
> Some(&"key")

// wait a minute and see that the cache is now empty
minute_cache.len()
> 0
minute_cache.get("key");
> None
```

Currently probably not safe because of manual `unsafe` lifetime changing in `get()`, `iter()`, and `iter_mut()`.
