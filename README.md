# Oxanus

<p align="center">
  <picture>
    <img alt="Oxanus logo" src="logo.jpg" width="320">
  </picture>
</p>

Oxanus is job processing library written in Rust that won't suck (or at least it will suck in a completely different way than other options). It is still very much work in progress.

## Features

- [x] Isolated Queues
- [x] Retrying
- [x] Scheduled Jobs
- [x] Dynamic Queues
- [x] Throttling (queue-based)
- [x] Unique Jobs
- [x] Resilient Jobs
- [x] Graceful Shutdown
- [ ] Tests
- [ ] Dev API
- [ ] Observability (prometheus)
- [ ] Periodic Jobs

## Dev

```bash
psql -c "CREATE DATABASE oxanus;"
```

```bash
RUST_LOG=debug cargo run --example foo
```

