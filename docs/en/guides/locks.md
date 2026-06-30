# Coordination Locks

## The abstraction

`CoordinationLock` is a pluggable trait for mutual exclusion across processes.
It is the shared primitive for both fault-tolerance strategies:

- **Replica** (Subsystem A) — coordinate concurrent writes to shared state.
- **Leader/Follower** (Subsystem B) — use the lock as the leader lease.

## The trait

```rust
#[async_trait]
pub trait CoordinationLock: Send + Sync {
    async fn acquire(&self, key: &str, lease: Duration)
        -> Result<Box<dyn LockGuard>, LockError>;
}

#[async_trait]
pub trait LockGuard: Send + Sync {
    async fn release(&mut self);
}
```

## Backends

### `file-lock` (POSIX `flock`)

Enable the `file-lock` feature:

```toml
malkuth = { features = ["file-lock"] }
```

```rust
use malkuth::lock::FileLock;
use std::time::Duration;

let lock = FileLock::new("/var/lib/myapp/locks");

let mut guard = lock.acquire("write-queue", Duration::from_secs(30)).await?;
// ... exclusive work ...
guard.release().await; // or just drop guard
```

One lock file per `key`, created under the root directory. Uses `flock(LOCK_EX | LOCK_NB)`
for non-blocking exclusive locks. If another process holds the lock, returns
`LockError::Contended`.

### `lease` (file lock with TTL)

Enable the `lease` feature (implies `file-lock`):

```toml
malkuth = { features = ["lease"] }
```

Same API as `file-lock`, but if the lock holder crashes, the lease expires
after the TTL and another process can acquire it. Useful for single-host
deployments where the lock holder might die without releasing.

### `pg-lock` (PostgreSQL advisory lock)

Staged — not yet implemented. Will use `pg_advisory_lock` for distributed
coordination across multiple hosts sharing a Postgres instance.

## When to use which

| Scenario | Backend |
| --- | --- |
| Single host, lock holder won't crash | `file-lock` |
| Single host, lock holder might crash | `lease` |
| Multiple hosts, shared Postgres | `pg-lock` (staged) |
| Multiple hosts, no shared DB | External (etcd, Consul) |
