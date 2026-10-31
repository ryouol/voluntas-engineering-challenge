# Validation

Tested on macOS with Rust 1.92. All **39 tests**, formatting, and Clippy with warnings
denied pass. Tests use disposable keys and a local HTTP/WebSocket exchange.

Coverage includes exact book arithmetic, empty/replacement snapshots, sequence gaps,
rapid reactivation, socket ownership handoffs under backpressure, subscription batches,
lifecycle bursts, failed discovery, output framing, verifier rejection, and shutdown.

## Final implementation: four-minute live run

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
seconds maximum. This was not repeated as a 15-minute soak of the final revision.
The final verifier reproduced the earlier capture's report exactly.

Coverage figures describe sampled statuses, not proof that no exchange event was
missed. Captures and credentials are excluded from this repository. Reproduce the
offline checks with the commands in the README; live validation requires your own key.
