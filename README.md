# Kalshi order-book collector

Rust implementation of the [Voluntas core engineering challenge](https://voluntas.io/engineering-challenge),
built on the [provided SBE template](https://github.com/voluntas-io/engineering-challenge-template).
Collects all live regular markets, excludes MVEs, and writes order-book updates and
minute snapshots to stdout. Extensions are outside the scope of this submission.

[Approach and local walkthrough](https://ryouol.github.io/voluntas-engineering-challenge/)
explains the design, status output, tradeoffs, and a possible connection to my WLA project.

## Setup and run

Requires Rust 1.92+ and a read-only Kalshi API key with an Ed25519 PKCS#8 PEM private key.
See [Kalshi's key setup guide](https://docs.kalshi.com/getting_started/api_keys).

```sh
cp .env.example .env
mkdir -p .secrets data
# Save your private key as .secrets/kalshi.pem and set its key ID in .env.
chmod 700 .secrets
chmod 600 .env .secrets/kalshi.pem
cargo build --release --locked
./target/release/engineering-challenge > data/capture.sbe 2> data/collector.log
# Stop with Ctrl-C, then verify the capture:
./target/release/verify data/capture.sbe
```

Credentials, captures, and logs are ignored by Git. Tests require no Kalshi credentials.
The default binary destination is stdout; diagnostics go to stderr. Alternatively,
`--output data/capture.sbe` writes directly to a new file and refuses to overwrite one.
Run from the repository root so `.env` and the private-key path resolve correctly.

Useful options:

- `--markets-per-socket 1000`: set the target shard size. The default is 100, growing
  as necessary to respect `--max-data-sockets` (default 199, plus one lifecycle socket).
- `--run-seconds 120`: stop gracefully after a bounded run.
- `--symbols TICKER`: restrict a smoke test to selected live markets; omit for full coverage.

## Design

- Paginated discovery and lifecycle notifications maintain market membership. Failed
  scans retain the previous universe; lifecycle lookups are batched and rate-limited.
- Each socket owns its books. Ownership changes wait for the old source to finish
  enqueueing records. Sequence gaps, disconnects, and missing snapshots trigger recovery.
- Prices and quantities use exact integer millionths. NO bids become YES asks at
  `1 - price`; signed exchange changes become absolute quantities, with zero deleting a level.
- Receive timestamps are recorded before JSON parsing. Exchange timestamps are kept
  separately; Unix epoch zero denotes unavailable exchange time.
- The supplied SBE schema and helpers encode initial/recovery snapshots, updates,
  and snapshots on a 60-second schedule. Bounded queues fail explicitly on output stalls.
- SIGINT stops producers, closes sockets, drains accepted records, and flushes output.

Start with `src/book.rs` and `src/feed.rs` for book and subscription logic.
`discovery.rs`, `socket.rs`, and `collector.rs` handle membership, connections, and
coordination; `output.rs` uses the supplied writer. Tests live beside modules in
`*_tests.rs` files and use the local exchange fixture in `tests/common`.
For a dependency-ordered review, follow the [reading map](https://ryouol.github.io/voluntas-engineering-challenge/#review).

## Tests and validation

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked -- --include-ignored
```

All 39 tests pass, including the 65-second checkpoint test, independent replay,
subscription churn, recovery, stdout decoding, broken pipes, and SIGINT.
The final four-minute live run captured 3.08 million updates across 178,095 markets.
See [validation results](docs/VALIDATION.md).

## Limitations

The template's framing uses 64 MiB chunks. Output is buffered until a chunk fills,
a periodic flush, or shutdown; partial flushes pad to a full chunk. Snapshot timing
has scheduler jitter, and market discovery is paginated rather than atomic.
Recovery replaces book state but cannot recover updates missed while disconnected.
