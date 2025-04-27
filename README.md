![DHT Chord Logo](assets/dht_chord_logo.png)

A high-performance, secure, and reliable Rust implementation of a peer-to-peer Distributed Hash Table (DHT) based on the Chord protocol.

---

## Features

- 🔑 **Key–Value Storage** – Standard DHT operations with built-in replication.
- ⚙️ **Asynchronous & Multithreaded** – Powered by Tokio for high concurrency.
- 🔄 **Automatic Stabilization** – Node discovery and network stabilization on churn.
- 🛡️ **Sybil Resistance** – Proof‑of‑work challenge to deter malicious actors.
- 📦 **Replication & Caching** – Configurable replication factor and local caching.
- 🌐 **IPv4 & IPv6** – Dual-stack support.
- 🚀 **Performance Optimized** – Benchmarks: 2,000 nodes on one machine in <10 s (no PoW).

## Documentation

Explore the full API and design docs:

[📖 View the DHT Chord Docs](https://egroh.github.io/dht_chord/dht_chord/index.html)

---

## Architecture

DHT Chord separates core Chord logic from the external API:

- **`chord` crate**: Core DHT implementation, finger tables, routing, replication.
- **`api_communication` module**: Exposes DHT operations over a simple network API.

### Threading Model

- **Tokio tasks** for async workloads.
- **Dedicated threads** per P2P/API connection.
- **Background housekeeping** for cleanup, replication, stabilization, and finger fixing.

### Security

- **Proof‑of‑Work** (SHA3‑512) for get/insert operations with adjustable difficulty.
- **ID assignment** by hashing node IP & port.
- **Housekeeping refresh** to guard against dropouts.

## Prerequisites

- Rust (nightly)
- Cargo

> Tested on:
> - `rustc 1.74.0-nightly`
> - `cargo 1.74.0-nightly`

Install Rust via [rust-lang.org](https://www.rust-lang.org/tools/install).

## Installation

```bash
# Clone the repository
git clone https://github.com/egroh/dht_chord.git
cd dht_chord

# Build in release mode
cargo build --release
```

The binary will be at `target/release/p2p_dht`.

To build the docs locally:
```bash
cargo doc --no-deps --document-private-items
# open in browser: target/doc/dht_chord/index.html
```

## Configuration

The DHT node is configured via an INI file. Example:

```ini
[dht]
default_store_duration = 60     ; seconds
max_store_duration     = 600    ; seconds
p2p_address            = 127.0.0.1:8001
api_address            = 127.0.0.1:7401
bootstrap_node         = 127.0.0.1:8000  ; optional
```

## Usage

Run a node:

```bash
RUST_LOG=info cargo run --release -- -c example_config_node_0.ini
```

- `-c <config>`: path to INI config file
- `RUST_LOG`: set log level (`error`, `warn`, `info`, `debug`, `trace`)

## Testing

Run the test suite:
```bash
cargo test --release
```

## CI / Releases

- **GitHub Actions** runs tests and builds docs on each push.
- **Documentation** is auto‑deployed: https://egroh.github.io/dht_chord/dht_chord/index.html