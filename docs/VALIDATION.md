# Validation

Tested on macOS with Rust 1.92. All **39 tests**, formatting, and Clippy with warnings
denied pass, rechecked November 1, 2026. Tests use disposable keys and a local
HTTP/WebSocket exchange. The latest pass also verifies exchange-time precedence
and receive timestamps through the decoded SBE output.

Coverage includes exact book arithmetic, empty/replacement snapshots, sequence gaps,
rapid reactivation, socket ownership handoffs under backpressure, subscription batches,
lifecycle bursts, failed discovery, output framing, verifier rejection, and shutdown.

## Current implementation: 20-minute live run

Run November 1, 2026, using the code in `c206aec`, with 1,000 markets per socket
and automatic shutdown after 1,200 seconds. Later commits change documentation only.

| Measurement | Result |
| --- | ---: |
| Distinct markets | 171,875 |
| Updates decoded | 23,680,020 |
| Snapshots decoded | 3,255,660 |
| Data sockets | 172 |
| Exact desired/acknowledged/valid equality after bootstrap | 85 / 110 samples |
| Valid-book count equals desired count after bootstrap | 109 / 110 samples |
| Reconnects | 2 |
| Snapshot gap median / p99 / maximum | 60.000 / 60.004 / 60.058 seconds |
| Collector / verifier exit codes | 0 / 0 |

Both reconnects followed heartbeat timeouts. The final status had 170,138 desired,
acknowledged, and valid markets; shutdown closed sockets and flushed output.
Independent replay matched 3,081,931 repeated snapshots. Another 1,854 snapshots
replaced state with a new exchange timestamp; none changed state while retaining
the same exchange timestamp. Coverage counts are sampled observations, not a
claim of uninterrupted coverage or recovery of every missed event.

## Initial submission: four-minute live run

Measured on the implementation published in `74c3bde`. The November 1 code change
clarifies service-task error propagation; it preserves the collection behavior.

| Measurement | Result |
| --- | ---: |
| Distinct markets | 178,095 |
| Updates decoded | 3,079,480 |
| Snapshots decoded | 711,544 |
| Data sockets | 179 |
| Full book-coverage samples after bootstrap | 16 / 16 |
| Reconnects | 1 |
| Snapshot gap median / p99 / maximum | 59.999 / 60.076 / 60.135 seconds |
| Exit code | 0 |

The reconnect followed a subscription acknowledgment timeout during startup. The
collector restored full coverage and shut down cleanly. Independent replay matched
533,386 repeated snapshots; 63 snapshots replaced state with a new exchange timestamp.
None changed state while retaining the previous exchange timestamp.

## Earlier long run

Before the final review fixes, a 15+ minute full-universe observation decoded
12,561,172 updates across 178,118 markets, with complete coverage in 87/87 sampled
statuses and zero reconnects. Snapshot gaps were 60.000 seconds median and 63.039
seconds maximum. The current implementation's 20-minute run is recorded above.
The final verifier also reproduced this earlier capture's report exactly.

Coverage figures describe sampled statuses, not proof that no exchange event was
missed. Captures and credentials are excluded from this repository. Reproduce the
offline checks with the commands in the README; live validation requires your own key.
