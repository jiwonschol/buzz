# Usage hold review follow-up

This branch supplements jiwonschol/buzz#1 at
`ef89d7c9677b939b036dcd4e85be9c9c00dd915b`. Ownership of the original active
implementation session was not confirmed. The original branch is untouched;
all changes and checks use a separate `buzz-1-review-20260923` worktree.

## Review disposition

Review links below are discussions on https://github.com/jiwonschol/buzz/pull/1.

| Review ID | Disposition and verification |
| --- | --- |
| 3969670421 | Existing inactivity guard retained; `usage_limit_held_batch_blocks_inactivity_shutdown` exercises it. |
| 3969670432 | Explicit timezone continues to use a conservative fallback rather than host-local interpretation; retention is extended below. |
| 3969670446 | Existing just-expired explicit date/year-boundary behavior retained and tested. |
| 3969670467 | Existing failure detail formatter includes current and cancelled events; existing linked-event/excerpt test retained. |
| 3969954486 | Held event IDs are protected from per-scope and channel-wide FIFO eviction; busy-scope and cross-thread overflow test recovers the original request. |
| 3969954496 | Cancelled-only fallback checks scope throttle in both readiness and dispatch; direct cancelled-only regression test. |
| 3969954501 | Space, hyphen and underscore rate-limit forms excluded from account usage classification; production predicate tests. |
| 3969954507 | Hold count derived from the eight-day supported window and minimum 90-second production delay (7,681 holds). Simulated week of half-hour fallback retries preserves the request. This is a minimum supported retention window, not an eight-day expiry: repeated 30-minute unknown-zone failures can retain longer before the finite cap. |
| 3969954516 | DST ambiguity selects the still-future occurrence before tomorrow rollover. Production parser runs in an isolated `TZ=America/New_York` child at the second 01:15; reset is 15 minutes away. |
| 3970303240 | Harness-level deadline monotonically extends across limit responses and gates all subsequent scope dispatch/readiness, including fresh channels and cancelled carryover. It does not cancel already running turns or coordinate separate harness processes sharing credentials. |
| 3970303248 | Usage notices retry with bounded backoff; generic failure notices stay one-shot. Local HTTP fixture returns 503, then accepted:false, then accepted:true for the same signed event ID. |
| 3970303254 | Separate inbound 65,535-byte cap and outbound codec ceiling 65,408. Authenticated peer frames at 65,409 and 65,535 are decrypted and deserialized by the production receive path. |

No review thread was marked resolved or commented on remotely. Independent
review must judge the actual final patch, not treat this table as approval.

## Verification and limitations

Commands use the repository Hermit environment. The changed-package suite is
`env -u BUZZ_ACP_LAZY_POOL -u BUZZ_ACP_IDLE_POOL_SLEEP -u BUZZ_ACP_IDLE_POOL_SLEEP_SECS CARGO_BUILD_JOBS=2 cargo test -p buzz-core -p buzz-acp --no-fail-fast`.
With the current uncommitted patch on the base above: buzz-acp 956, integration
9, buzz-core 258, doc tests 2 passed. Package clippy with `--all-targets -- -D
warnings` passed; formatting and diff whitespace checks passed. Repository-wide
The first `just ci` stopped at missing pkg-config/OpenSSL development files.
A follow-up extracted Ubuntu packages into the account's `.scratch/buzz-ci-deps`
without installing packages or running maintainer scripts. Process-local
PKG_CONFIG, PKG_CONFIG_SYSROOT_DIR, PKG_CONFIG_LIBDIR, CPATH and LD_LIBRARY_PATH
connect the extracted headers/libraries. Multiarch OpenSSL headers, GTK/WebKit,
shared-mime-info and ALSA development files were needed. With those files,
workspace clippy, desktop checks, Tauri clippy (default and mesh-llm), and web
checks passed. Desktop check reported four warnings and five informational
findings without changing files. Hermit bootstrapped Flutter 3.41.7 in its
personal cache. Mobile analysis passed; security-review tests (13), file-size
policy tests (10) and all three file-size entrypoints passed. Missing origin/main
was fetched read-only at c045321a7fb3ca8939f28519ce7a555a6f597728.

All CI recipe stages subsequently passed across the original run and resumed
targets. This is not a claim that one uninterrupted `just ci` exited zero:
the original attempt exited 101 at Tauri test linking. Broken shared-library
links in the extracted development packages selected static GLib archives;
the corresponding runtime packages were also extracted locally. After that,
`just desktop-tauri-test web-build mobile-test` exited zero.

Observed results: the repository Rust unit-test runner passed its configured
package/filter sequence (not all infrastructure-backed Rust integration tests),
desktop JavaScript 6,456 passed with zero skipped, desktop production build and
OSS artifact matrix passed, Tauri check passed, Tauri workspace tests 3,273
passed with 20 ignored, web production build passed, and mobile 2,076 passed.
The source remains the uncommitted patch over the base above; no commit or
deployment provenance is implied. No production services were launched.

Reproduction artifacts in `/home/buzz-hyuncheol/.buzz/.scratch/`:
`buzz-ci-userdeps.sh` sets only process-local dependency paths and activates
Hermit; `buzz-ci-apt-plan.log` and `buzz-ci-runtime-packages.txt` identify extracted
packages. `buzz-ci-complete-attempt.log` contains the full attempt through the
Tauri linker failure; `buzz-ci-remaining-runtime.log` contains the successful
resumed targets. Earlier environment failures remain in `buzz-ci-*.log`.

Mutation checks removed request protection, global gating, extended retention,
rate exclusion, notice retry and the inbound ceiling independently in the patch;
the combined suite failed six corresponding regression tests. Disabling the DST
selection guard separately failed the deterministic production-helper test.
The host-local parser test alone did not catch that mutation, so it is not used
as proof of the guard. All mutations were restored before the final full suite.

No installed runtime was replaced and no production-provider turn was invoked.
In-memory queue/notice retention does not survive a deliberate process restart;
durable journaling remains separate work (upstream block/buzz#6045). Named-zone
timestamps remain fallback polling, not timezone-database conversion. Browser
or desktop UI behavior is outside these package checks.

## Runtime provenance: read-only observation

On 2026-09-23 the running process for this account was PID 3417080,
`/proc/3417080/exe` -> `/usr/local/bin/buzz-acp`, cwd
`/home/buzz-hyuncheol/.buzz`. Executable SHA-256:
`4a3b519d7712a27637939b3ef1ef0cb410588b3e41075c8b7c77a7513479e18a`.
This differs from the previous day's recorded binary hash. `/opt/buzz` returned
`Permission denied`; no source commit mapping is established. The deployment
owner must provide the build manifest/source SHA for that binary hash. A changed
hash alone does not prove that #1 or this follow-up is deployed.
