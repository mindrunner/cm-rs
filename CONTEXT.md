# Domain glossary

Terms used in code, logs, and docs. Keep code identifiers aligned with these.

## Snapshot

A Solana snapshot archive (`.tar.zst`). **Full** snapshots stand alone
(`snapshot-SLOT-HASH.tar.zst`); **incremental** snapshots apply on top of a
full at their **base slot** (`incremental-snapshot-BASE-SLOT-HASH.tar.zst`).
The filename grammar — slots, hash component, sidecar and partial-marker
conventions — is owned by `src/snapshot.rs`; nothing else parses snapshot
filenames.

## Bootstrap slot

The slot a validator would boot to from the local set: highest full, then the
highest incremental based on that full (`agave_bootstrap_slot`).

## Sidecar

The `.sha256` file next to a snapshot holding its hex digest, so hashes
survive restarts without re-hashing multi-GB files.

## Partial marker

The `.aria2` control file a partial download leaves next to a snapshot. A
snapshot with a partial marker is not **complete** and is invisible to the
index.

## Snapshot store

`SnapshotIndex` (`src/index.rs`): the sole owner of the snapshot directory.
Watches and scans it, **admits** downloaded files from the **staging
directory** (`.cm-tmp`), applies **retention** (`retain`: keep the newest
N fulls, the newest M incrementals whose base full survives), and sweeps
staging at daemon startup (`clean_staging`: drop entries at or below the
local bootstrap plus junk, keep fresher partials so downloads can resume).
Other modules never write to the snapshot directory directly. A verified
file already in staging is reused by checksum instead of re-downloaded.

## Fetch policy

`src/download/policy.rs`: pure decision logic — local snapshots + **source**
snapshots + config in, ordered **download plans** out, with a **fetch reason**.
The same policy ranks peers and external sources.
Validators generate their own snapshots; peer downloads exist for bootstrap
and offline-too-long recovery, plus cheap incremental top-ups.

- **Fetch reason** — why a cycle downloads at all: `bootstrap` (no usable
  local full; gate and floor bypassed), `behind` (bootstrap at least
  `min_slots_behind` behind the cluster tip), or `topup` (within tolerance,
  but a peer incremental still improves the local bootstrap). Exposed as the
  `cm_downloads{reason}` metric.
- **Full-fetch gate** — full-snapshot plans exist only on bootstrap or when
  the local bootstrap is `min_slots_behind` behind the best peer.
- **Improvement floor** — every full plan must individually improve the local
  bootstrap by at least `min_slots_behind`; a failed best plan never falls
  through to a marginal-gain full download. Bypassed on bootstrap.
- **Sufficient incremental** — an incremental-on-local-full plan whose
  resulting bootstrap reaches within `min_slots_behind` of the tip. Sufficient
  incrementals are tried before any full plan, as a class: if the best peer
  fails, the next sufficient incremental is tried, not a full.
- Incremental top-ups stay ungated (they're cheap); only full downloads are
  guarded. A standby whose validator is down is therefore kept roughly
  `min_slots_behind` fresh, not incremental-interval fresh.
- `cm fetch --force` bypasses the gate and floor (treats `min_slots_behind`
  as 0) for manual recovery.
- **Fetch outcome** — what a cycle achieved: `Downloaded`, `NotNeeded`,
  `NoPeers`, or `Unresolved(reason)`. `cm fetch` fails on `NoPeers` and on an
  unresolved `bootstrap` or `behind`; an unresolved `topup` is not a failure.

## Download plan

One attemptable unit: a **source**, a full snapshot, an optional incremental,
and whether the full is already local (**incremental-only**).

## Source

Where a plan downloads from (`SnapshotSource`): a **peer** (cm API) or an
**external source** (agave convention: `{base}/snapshot.tar.bz2` redirects to
the concrete file).

## Fallback tier

`src/fallback/`: opt-in (`[fallback] enabled`) external sources used when a
**fallback trigger** fires: `no_peers`, `only_slow_peers` (every peer's recent
bandwidth below `min_peer_bandwidth`; marks expire after 15 minutes), or
`peer_plans_failed`. Candidates are configured **mirrors** plus RPC nodes from
`getClusterNodes`; each round **probes** them (alias redirect, latency), drops
those more than `max_slot_lag` behind the tip, speed-tests the fastest few,
and caches the ranking for `cooldown`. External files carry no source
checksum; the locally computed hash becomes the sidecar.

## Speed floor

`min_speed` steers a cycle away from slow peers: a transfer below it for 60s
is aborted and that peer's other plans are skipped for the cycle. The floor
applies only while a plan from another usable source remains (including the
fallback tier when it still has sources this cycle), so the last candidate is
never abandoned while it makes progress. A transfer that moves
no bytes for 60s is always aborted.

## Download backend

An adapter that fetches one URL to one destination and returns bytes +
SHA-256: `builtin` (parallel HTTP ranges) or `aria2c` (subprocess). Backend
quirks (chunk files, control files, hashing) stay inside the adapter.

## Peer

Another cm node, discovered statically or via Tailscale, polled for its
snapshot inventory, and ranked by latency, errors, bandwidth, and
**DC affinity** (preferred datacenters, configured under `[peers]`).
