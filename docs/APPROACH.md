# Approach and local walkthrough

I started from the [official Rust template](https://github.com/voluntas-io/engineering-challenge-template)
and focused on the core challenge: live regular-market discovery, correct order
books, replayable output, minute snapshots, and graceful shutdown. The supplied
SBE schemas and reader are unchanged; the writer accepts stdout or a file while
preserving its chunk format.

## Ownership and data flow

```text
REST discovery + lifecycle notifications
                  ↓
        subscription coordinator
          ↙       ↓       ↘
       socket A  socket B  socket …
       + books   + books   + books
          ↘       ↓       ↙
          bounded output queue
                  ↓
          supplied SBE writer
```

Each socket task owns its books, so updates are applied in order without shared
book locks. One writer prevents binary records from being interleaved.

1. **Discover membership safely.** Follow every REST page with `mve_filter=exclude`.
   Reconcile on a 60-second schedule and process lifecycle notifications between
   scans. Failed scans retain membership; newer lifecycle observations override
   stale scan results.
2. **Keep assignments stable.** New markets go to the socket with the fewest
   markets. Existing assignments stay put to reduce resubscriptions. The target
   starts at 100 markets per socket and grows to fit the configured connection
   budget. Multiple markets use at least two data sockets. During a handoff, the
   old owner finishes queuing records before the new owner starts.
3. **Apply exact book changes.** Prices and quantities use integer millionths.
   NO bids become YES asks at `1 - price`; signed deltas become absolute output
   quantities, and zero deletes a level. A snapshot must precede deltas. Sequence
   checks are per subscription, even when several markets share its sequence.
   Gaps and disconnects discard affected books and rebuild from fresh snapshots.
4. **Make output verifiable.** Emit initial/recovery snapshots, updates, and
   minute checkpoints. Capture local receive time before parsing and preserve
   exchange time separately; epoch zero means no exchange timestamp was supplied.
   A bounded queue reports sustained output stalls. SIGINT stops producers,
   closes sockets, drains accepted records, and flushes output.

For example, a NO bid at `0.37` for `12.50` contracts becomes a YES ask at `0.63`.
A subsequent quantity change of `-2.50` emits price `630000`, quantity `10000000`.

## Tradeoffs

| Choice | Benefit | Limit |
| --- | --- | --- |
| Stable market-count balancing | Simple ownership, fewer resubscriptions | Equal counts do not mean equal traffic. |
| One active owner per market | Ordered state and output | Recovery restores current state, not missed history. |
| Supplied 64 MiB framing | Compatible with the provided reader | Partial flushes pad a full chunk. |
| Scans plus lifecycle events | Prompt changes with reconciliation | Paginated discovery is not atomic. |

Extensions are outside this submission. Timing jitter and recovery gaps are
documented in the [validation record](VALIDATION.md).

## Local walkthrough

Complete the [README setup](../README.md#setup-and-run), then run from the repo root:

```sh
cargo build --release --locked
mkdir -p data
./target/release/engineering-challenge \
  --markets-per-socket 1000 --run-seconds 180 \
  --output data/walkthrough.sbe
./target/release/verify data/walkthrough.sbe
```

The collector stops after three minutes; Ctrl-C also stops gracefully. Use a new
filename each time because file output refuses to overwrite a capture. To use
stdout, omit `--output` and redirect with `> data/walkthrough.sbe`. Diagnostics
remain on stderr. Add `--symbols TICKER` for a smaller, currently active market.

Watch `desired` (selected markets), `acknowledged` (confirmed subscriptions), and
`valid` (usable books) converge after startup. Updates grow continuously and
snapshots advance each minute. `written` counts encoded records, not necessarily
flushed bytes; `queue` shows pending records. Coverage equality is an observation,
not proof that every exchange event arrived.

After shutdown, the verifier independently replays the capture, compares later
snapshots, and reports counts and snapshot timing. Inspect state replacements
alongside reconnect logs: new exchange snapshots can legitimately replace state.

## Reading order for code review

Review each slice with its adjacent tests before moving to its consumers:

1. `src/util/aligned_sbe_writer.rs`: independent writer adapter and framing tests.
2. `src/book.rs` and `src/output.rs`: arithmetic, canonical sides, and queued records.
3. `src/config.rs`, `src/auth.rs`, `src/discovery.rs`: configuration and membership.
4. `src/feed.rs`: subscription commands, sequence checks, and book validity.
5. `src/socket.rs`: recovery, checkpoint timing, and ownership release.
6. `src/collector.rs` and `src/main.rs`: coordination and shutdown.
7. `src/bin/verify.rs`: independent replay and timing statistics.
8. `tests/common` and integration tests: exchange fixture, recovery, discovery
   races, scale, stdout, minute checkpoints, and SIGINT.

## Possible extension: connection to WLA

My [Weighted Log Analyzer Distributor](https://github.com/ryouol/wla-distibutor)
uses configured analyzer weights, health checks, and retries. The challenge's
subscription-monitoring extension raises a related question: how do we detect a
connection falling behind or a subscription going unexpectedly quiet?

A future version could measure per-market activity and connection delay,
distinguish quiet markets from suspicious stalls, and use those observations to
guide resubscription and workload distribution. Balancing would move whole market
subscriptions with an ordered handoff; per-packet weighted routing would not
preserve this ownership model. Traffic imbalance should be measured first.

This is a proposed direction, not implemented functionality. The collector was
designed from the challenge requirements and does not reuse WLA code.
