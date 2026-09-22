# WarpFile

**WarpFile** is an experimental peer-to-peer file transfer application written in Rust.

It transfers files directly between devices using **WFP (WarpFile Protocol)**, its own binary application-layer protocol, without requiring WarpFile cloud storage or permanent user accounts.

The current development version supports integrity-verified and resumable TCP transfers, persistent transfer identity, recovery across interrupted connections, completion reconciliation after lost final confirmation, persistent receivers, automatic device discovery across local networks and Tailscale, and sending files by device name.

> WarpFile is under active development and is not yet intended for untrusted networks.

---

## Current capabilities

WarpFile currently supports:

- WFP/0.2 binary framing over TCP and UDP;
- streaming transfers without loading the entire file into memory;
- 64 KiB DATA frames;
- incremental BLAKE3 integrity verification;
- 128-bit Transfer IDs for logical send jobs;
- preservation of the same Transfer ID across automatic retries;
- file offer acceptance and structured rejection;
- explicit transfer cancellation with `Ctrl+C`;
- `.part` files for incomplete incoming transfers;
- persistent `.part.warpmeta` metadata for partial transfer identity;
- preservation of partial state after recoverable connection loss;
- verified resumable transfers using byte offsets and BLAKE3 prefix hashes;
- cryptographically verified adoption of useful partial data when transfer metadata is missing, stale or belongs to another Transfer ID;
- automatic restart from byte zero when retained partial data does not match the source;
- avoidance of retransmitting already validated file prefixes;
- resume from arbitrary byte offsets rather than DATA-frame boundaries;
- zero DATA retransmission when the receiver already has the complete validated partial contents;
- immutable receiver-side completion receipts;
- automatic reconciliation when the final `VERIFIED` confirmation is lost;
- direct `OFFER -> VERIFIED` completion reconciliation;
- persistent receivers that can accept multiple sequential transfer sessions;
- bounded automatic sender reconnect after selected recoverable network failures;
- UDP peer discovery with WFP `DISCOVER` / `ANNOUNCE`;
- discovery across IPv4 local network interfaces;
- optional Tailscale-assisted peer discovery;
- device-name resolution;
- direct `IP:port` transfers as a fallback;
- unit and end-to-end tests covering protocol, discovery, cancellation, corruption, resume, persistence, retry and completion reconciliation.

---

## Example

Start a receiver:

```powershell
warpfile receive
```

Discover WarpFile devices:

```powershell
warpfile discover
```

Example output:

```text
WarpFile devices found:

1. EDBOOK
   100.68.8.15:42069
```

Send a file using only the device name:

```powershell
warpfile send .\README.md EDBOOK
```

WarpFile resolves the device automatically:

```text
Resolved EDBOOK -> 100.68.8.15:42069
```

A direct address can still be used:

```powershell
warpfile send .\README.md 100.68.8.15:42069
```

---

## How discovery works

WarpFile discovery is provider-based.

```text
                    warpfile discover
                           |
              +------------+------------+
              |                         |
         Local networks              Tailscale
              |                         |
       directed UDP              candidate peer IPs
        broadcasts                     |
              |                         |
              +------------+------------+
                           |
                     WFP DISCOVER
                           |
                     WFP ANNOUNCE
                           |
                  discovered device
```

Tailscale is optional.

When available, WarpFile uses Tailscale only to obtain candidate peer addresses.

A Tailscale peer is **not automatically considered a WarpFile peer**.

WarpFile sends its own WFP `DISCOVER` message to each candidate, and only devices actually running WarpFile respond with `ANNOUNCE`.

---

## Transfer flow

A fresh WFP/0.2 file transfer looks like this:

```text
Sender                                      Receiver
  |                                            |
  | ---------------- HELLO -----------------> |
  | <------------- HELLO_ACK ---------------- |
  |                                            |
  | ---------------- OFFER -----------------> |
  |        Transfer ID + file metadata         |
  |                                            |
  | <--------------- ACCEPT ----------------- |
  |                                            |
  | ---------------- DATA ------------------> |
  | ---------------- DATA ------------------> |
  |                   ...                      |
  |                                            |
  | -------------- COMPLETE ----------------> |
  |          complete BLAKE3 digest            |
  |                                            |
  | <-------------- VERIFIED ---------------- |
  |                                            |
```

The sender calculates a BLAKE3 digest while streaming the file.

The receiver calculates the same digest while writing incoming data.

Before reporting success, the receiver validates the byte count and BLAKE3 digest and persists enough completion state to recover safely if final confirmation is lost.

---

## Transfer identity

WFP/0.2 adds a 128-bit **Transfer ID** to `OFFER`.

Conceptually:

```text
logical send job
Transfer ID = X
      |
      +--> TCP session 1
      |
      +--> TCP session 2
      |
      `--> TCP session 3
```

A Transfer ID identifies one logical sender operation across automatic reconnects.

It is **not**:

- a file-content hash;
- peer identity;
- authentication;
- a secret token.

The current sender generates one Transfer ID when a send operation starts and reuses that ID across its automatic retries.

A sender process restart currently generates a new Transfer ID.

---

## Resumable transfers

Unexpected connection loss does not automatically destroy useful received data.

If a transfer is interrupted by a recoverable network failure, the receiver may preserve:

```text
<filename>.part
<filename>.part.warpmeta
```

For example:

```text
video.mkv.part
video.mkv.part.warpmeta
```

When a later `OFFER` arrives, the receiver can propose the retained prefix:

```text
Sender                                      Receiver
  |                                            |
  | ---------------- OFFER -----------------> |
  |                                            |
  | <--------------- RESUME ----------------- |
  |          offset + prefix hash              |
  |                                            |
  | validate local source prefix               |
  |                                            |
  | ---------------- ACCEPT ----------------> |
  |                                            |
  | -------- DATA from resume offset --------> |
  |                   ...                      |
  |                                            |
  | -------------- COMPLETE ----------------> |
  | <-------------- VERIFIED ---------------- |
```

`RESUME` does not mean that the sender blindly trusts an offset.

The receiver sends:

```text
resume byte offset
+
BLAKE3 hash of the retained prefix
```

The sender reads and hashes exactly the same prefix from its source file.

Continuation is accepted only when both prefix hashes match.

---

## Restart after a mismatched prefix

If the retained receiver bytes do not match the sender's source:

```text
Sender                                      Receiver
  |                                            |
  | <--------------- RESUME ----------------- |
  |                                            |
  | --------------- RESTART ----------------> |
  |                                            |
  |                 discard stale partial     |
  |                 prepare fresh state       |
  |                                            |
  | <--------------- ACCEPT ----------------- |
  |                                            |
  | ------------ DATA from byte 0 ----------> |
```

This protects against accidentally combining data from different files that happen to share the same filename or size.

---

## Resume uses byte offsets

Resume positions are not tied to 64 KiB DATA-frame boundaries.

For example, a transfer may safely resume from:

```text
123457 bytes
```

as long as the BLAKE3 digest of:

```text
[0, 123457)
```

matches on both peers.

This keeps resume independent from current and future chunk-sizing strategies.

---

## Partial transfer metadata

The receiver persists auxiliary metadata next to incomplete files.

For:

```text
video.mkv.part
```

the metadata file is:

```text
video.mkv.part.warpmeta
```

It records information such as:

```text
Transfer ID
filename
file size
partial state
```

This metadata helps correlate interrupted transfer state.

It is not blindly trusted.

The actual `.part` bytes are still verified through BLAKE3 before resume is accepted.

If metadata is missing, corrupt or belongs to a different Transfer ID, WarpFile may still recover the useful partial bytes when the sender proves that the retained prefix matches its source.

---

## Already-complete partial files

A connection may disappear after the receiver has obtained every file byte but before final completion finishes.

If the receiver later proposes:

```text
resume offset == complete file size
```

and the full retained prefix matches the source, the sender does not retransmit DATA.

It can proceed directly to `COMPLETE`.

This is different from completion-receipt reconciliation, which handles a transfer whose completed state was already persisted by the receiver.

---

## Completion receipts

After complete size and BLAKE3 verification, the receiver persists an immutable completion receipt.

Receipts are stored under:

```text
<destination>/
`-- .warpfile/
    `-- receipts/
        `-- <transfer-id>.json
```

Conceptually, a receipt records:

```text
Transfer ID
filename
file size
complete BLAKE3
```

A completion receipt is receiver-local state.

It is not transmitted over WFP.

Its purpose is to answer a later question:

```text
Was this exact logical transfer already completed?
```

---

## Lost VERIFIED reconciliation

A particularly important failure can happen at the end of a transfer.

Consider:

```text
Sender                                      Receiver
  |                                            |
  | -------------- COMPLETE ----------------> |
  |                                            |
  |                       verify complete file |
  |                       persist receipt      |
  |                       commit final file    |
  |                                            |
  X <------------- VERIFIED ----------------- |
        connection disappears
```

The receiver may already have completed the transfer even though the sender did not receive `VERIFIED`.

WFP/0.2 allows the sender to retry safely with the **same Transfer ID**.

```text
new TCP session

Sender                                      Receiver
  |                                            |
  | ---------------- HELLO -----------------> |
  | <------------- HELLO_ACK ---------------- |
  |                                            |
  | ----- OFFER with same Transfer ID ------> |
  |                                            |
  |                    find completion receipt |
  |                    verify physical file    |
  |                                            |
  | <-------------- VERIFIED ---------------- |
```

In this state:

```text
OFFER -> VERIFIED
```

is a valid successful WFP/0.2 exchange.

No DATA or COMPLETE needs to be transmitted again.

---

## Why receipts are not blindly trusted

A receipt alone is not enough to return `VERIFIED`.

During reconciliation, the receiver checks:

```text
receipt Transfer ID
receipt filename
receipt file size
receipt BLAKE3
        |
        v
actual physical file
        |
        v
size + BLAKE3 verification
        |
        v
VERIFIED
```

If the physical file is missing, altered or inconsistent with the receipt, WarpFile refuses to report successful completion.

The uncommon reconciliation path therefore re-reads and hashes the completed file.

Normal successful transfers still use one-pass streaming BLAKE3.

---

## Partial-file behavior

Incoming incomplete files are written to:

```text
<filename>.part
```

The final filename is created only after complete size and BLAKE3 verification succeeds.

Different failure types intentionally have different behavior.

Unexpected recoverable connection loss:

```text
retain .part
retain .part.warpmeta
        |
        v
offer verified resume later
```

Explicit `CANCEL`:

```text
remove partial state
```

Invalid final BLAKE3, impossible transfer state or protocol failure:

```text
remove partial state
```

A zero-byte `.part` contains no useful resumable data and is discarded before starting a fresh transfer.

A `.part` larger than the file size announced by the sender cannot be a valid prefix and is also discarded.

---

## Automatic reconnect

The sender currently applies a bounded retry policy for selected recoverable network failures.

Current reference policy:

```text
maximum attempts: 3
delay:            1 second
```

Each retry creates a new WFP TCP session:

```text
connect
  |
  v
HELLO / HELLO_ACK
  |
  v
OFFER
```

The same logical send operation reuses the same Transfer ID.

If partial data exists, normal RESUME negotiation occurs.

If completion had already been persisted, the receiver may answer the new OFFER directly with VERIFIED.

Permanent failures are not automatically retried.

Examples include:

- receiver `REJECT`;
- malformed protocol state;
- non-transient local file errors;
- explicit user cancellation.

Retry timing and retry limits are sender implementation policy, not WFP protocol requirements.

---

## Architecture

The implementation is intentionally separated into independent responsibilities:

```text
CLI
 |
 +-- destination resolution
 |
 +-- discovery
 |    +-- LAN
 |    `-- Tailscale
 |
 +-- sender / receiver
 |        |
 |        +-- partial transfer state
 |        |
 |        +-- completion receipts
 |        |
 |        `-- WFP protocol
 |              |
 |              +-- framing
 |              +-- encoding / decoding
 |              +-- Transfer ID
 |              +-- transfer payloads
 |              +-- resume negotiation
 |              `-- completion reconciliation
 |
 `-- TCP / UDP I/O
```

The transfer layer does not need to know whether an address came from LAN discovery, Tailscale discovery or explicit user input.

This separation allows additional connectivity mechanisms to be introduced later without rewriting file-transfer semantics.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the implementation architecture.

---

## WarpFile Protocol

WarpFile uses its own experimental application-layer protocol:

```text
WFP/0.2
```

The current package version and protocol version are independent.

The latest published WarpFile alpha remains:

```text
v0.1.0-alpha.2
```

while current post-release development uses:

```text
WFP/0.2
```

An opt-in WFP/0.3 is also available via `--wfp-version=0.3` or `--wfp-version 0.3`, adding chunk-aware resume (deterministic chunk layout, per-chunk BLAKE3 manifest, sparse chunk state). WFP/0.2 remains the default. Details live in `docs/ARCHITECTURE.md`.

Every frame starts with a fixed 12-byte header:

```text
+----------------------+ 0
| Magic                | 4 bytes
+----------------------+
| Version              | 1 byte
+----------------------+
| Message Type         | 1 byte
+----------------------+
| Flags                | 2 bytes
+----------------------+
| Payload Length       | 4 bytes
+----------------------+ 12
| Payload              | N bytes
+----------------------+
```

Current message families include:

```text
HELLO / HELLO_ACK

OFFER / ACCEPT / REJECT
RESUME / RESTART

DATA

COMPLETE / VERIFIED

CANCEL

DISCOVER / ANNOUNCE

ERROR
```

WFP/0.2 OFFER contains:

```text
16-byte Transfer ID
u16 filename length
UTF-8 filename
u64 file size
```

Multi-byte integers use big-endian byte order.

The general frame payload limit is 1 MiB.

DATA payloads are limited to 64 KiB.

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the evolving protocol specification.

---

## Reliability

WarpFile currently handles or detects:

- unexpected disconnection;
- explicit cancellation;
- incorrect file size;
- corrupted data;
- invalid BLAKE3 digest;
- unsafe filenames;
- existing destination files;
- malformed WFP frames;
- stale partial-file contents;
- impossible resume offsets;
- empty or oversized partial-file state;
- partial metadata loss or mismatch;
- retry-session transfer identity;
- loss of final VERIFIED confirmation;
- conflicting completion receipts;
- physical files that disagree with persisted completion state.

Verified resume adds a second integrity checkpoint before continuation.

For a resume offset `X`:

```text
Receiver:

BLAKE3(.part bytes 0..X)


Sender:

BLAKE3(source bytes 0..X)
```

The transfer continues from `X` only when those values match.

Final `COMPLETE` / `VERIFIED` verifies the resulting complete file.

Completion reconciliation adds another check for the rare lost-final-confirmation path:

```text
completion receipt
        |
        v
actual completed file
        |
        v
rehash + compare
        |
        v
VERIFIED
```

---

## Current reliability limitations

The reliability layer is functional but intentionally remains simpler than the long-term WarpFile design.

Current limitations include:

- no persistent sender-side transfer database;
- Transfer IDs survive automatic retries but not sender process restart;
- no persisted BLAKE3 checkpoints;
- retained prefixes must be reread locally to reconstruct hash state;
- reconciliation rehashes completed physical files;
- one file per TCP transfer session;
- no directory-transfer manifest;
- WFP/0.2 resume is prefix-only (no arbitrary missing-chunk map); WFP/0.3
  provides a sparse chunk inventory via `--wfp-version=0.3`.
- no partial-content deduplication;
- no receipt garbage-collection policy;
- sequential rather than simultaneous multi-client receiving;
- no protocol STATUS query;
- no authenticated session.

For example:

```text
100 GiB source
8 GiB already retained
```

may require both peers to read and hash those 8 GiB locally again, but only the remaining 92 GiB needs to cross the network.

Future hash checkpoints may reduce this local reread cost.

---

## Security

**WFP/0.2 currently provides no encryption, authenticated peer identity or authenticated device identity.**

The current implementation should therefore only be used in trusted development environments or over an independently trusted network layer.

BLAKE3 prefix hashes used during resume prove content equality for the proposed prefix.

Completion BLAKE3 verification proves equality with the expected completed content.

They do **not** prove peer identity and must not be treated as authentication.

Transfer IDs are correlation identifiers.

They are **not** credentials or authentication tokens.

Receiver-side `.warpmeta` files and completion receipts are also not cryptographic proof against an attacker who can modify the receiver filesystem.

WarpFile will not design custom cryptographic algorithms.

Future authenticated and encrypted sessions will use established cryptographic primitives and libraries.

---

## Roadmap

### M0 — First Byte

Complete.

- TCP sender and receiver;
- WFP framing;
- encoder and decoder;
- `HELLO`;
- `HELLO_ACK`.

### M1 — First File

Complete.

- `OFFER`;
- `ACCEPT`;
- structured `REJECT`;
- streaming file transfer;
- progress, throughput and ETA;
- BLAKE3 verification;
- `COMPLETE`;
- `VERIFIED`;
- explicit cancellation;
- partial-file handling;
- end-to-end transfer tests.

### M2 — Zero Config

Complete for the current prototype.

- UDP `DISCOVER` / `ANNOUNCE`;
- local network discovery;
- multiple-interface support;
- self-discovery filtering;
- optional Tailscale provider;
- device-name resolution;
- persistent receiver;
- transfer by device name.

### M3 — Reliable Transfer

In progress.

Implemented:

- retained partial files after recoverable connection loss;
- verified resume negotiation;
- BLAKE3 prefix validation;
- byte-offset resume;
- `RESUME`;
- `RESTART`;
- suffix-only retransmission;
- recovery after real TCP connection loss;
- stale partial detection;
- zero-DATA completion when all file bytes are already present;
- end-to-end resume coverage;
- bounded automatic reconnect after recoverable network failures;
- three-attempt retry policy with one-second delays;
- automatic verified resume after reconnect;
- permanent-error retry suppression;
- WFP/0.2 Transfer IDs;
- Transfer ID preservation across automatic retries;
- persistent partial-transfer metadata;
- verified adoption of useful partial data across Transfer IDs;
- immutable completion receipts;
- receiver-side completion reconciliation;
- safe retry after lost `VERIFIED`;
- direct `OFFER -> VERIFIED` reconciliation;
- refusal to trust completion receipts without verifying physical bytes;
- chunk management (deterministic chunk layout, per-chunk BLAKE3 manifest,
  sparse chunk state) — available in WFP/0.3 via `--wfp-version=0.3`.

Still planned within the broader reliability and transfer-efficiency milestone:

- BLAKE3 checkpoints;
- directory transfer design;
- receipt lifecycle / garbage collection;
- persistent sender job identity where useful;
- improved path selection.

### M4 — Distribution

Planned.

- verified Linux support;
- broader release automation;
- installation workflow;
- reproducible performance benchmarks;
- broader documentation and deployment validation.

---

## Testing

WarpFile uses unit tests and end-to-end tests with real local TCP and UDP sockets.

Current validated suite:

```text
304 unit tests
5 CLI tests in main.rs
32 end-to-end tests
-------------------
341 tests total

0 failures
```

Coverage includes:

```text
protocol framing and validation
OFFER / REJECT payloads
WFP/0.2 Transfer ID encoding and generation
RESUME encoding and decoding
BLAKE3 integrity

normal transfers
empty files
cancellation

connection loss
partial retention
persistent partial metadata
verified partial adoption
metadata replacement after RESTART

prefix mismatch and RESTART
suffix-only resume
100% partial resume with zero DATA retransmission
recovery across two TCP connections

automatic sender reconnect
Transfer ID reuse across retries
bounded retry policy
permanent REJECT without retry

completion receipt persistence
receipt idempotency and conflict detection
final-file completion reconciliation
completed-.part reconciliation
physical-file verification against receipts
retry after lost VERIFIED
direct VERIFIED after OFFER

persistent receiver behavior

LAN discovery
Tailscale parsing
device-name resolution
```

---

## Performance philosophy

WarpFile is intended to become performance-oriented, but it will not claim to be faster than other tools without reproducible benchmarks.

A central principle is:

> Never transfer a byte that does not need to be transferred.

Current resume and reconciliation behavior already apply that principle by avoiding unnecessary retransmission of validated file bytes.

Chunk-oriented recovery (deterministic chunk layout, per-chunk BLAKE3 manifest, sparse chunk state) is delivered in WFP/0.3 via `--wfp-version=0.3`.

Future optimization work includes:

- BLAKE3 checkpoints;
- adaptive chunks;
- pipelining;
- minimizing unnecessary copies;
- platform-specific zero-copy transfer paths where practical;
- selective compression;
- content deduplication;
- batching many small files;
- automatic path selection;
- multi-source transfers.

Potential connectivity evolution includes:

```text
LAN direct
    |
    v
IPv6 direct
    |
    v
NAT traversal
    |
    v
encrypted relay
```

Performance work will be measured using:

- throughput;
- CPU usage;
- memory usage;
- protocol overhead;
- retransmitted bytes;
- recovery behavior.

No performance claim should be made without reproducible measurements.

---

## Building from source

WarpFile currently uses the Rust stable toolchain.

Clone the repository:

```powershell
git clone https://github.com/edvdourado/warpfile.git
cd warpfile
```

Build:

```powershell
cargo build --release
```

Run the test suite:

```powershell
cargo test
```

Run Clippy with warnings denied:

```powershell
cargo clippy --all-targets -- -D warnings
```

The optimized Windows executable is generated at:

```text
target\release\warpfile.exe
```

---

## Platform status

Development and real multi-machine testing have currently focused on Windows.

The core architecture is intended to remain portable.

Linux remains a target platform.

Platform-specific performance optimizations should remain behind portable abstractions rather than leaking into WFP semantics.

Tailscale is optional and is not required for LAN transfers.

---

## Project status

WarpFile is experimental.

Protocol details, CLI behavior and internal architecture may change without backward compatibility before the first stable release.

The current published alpha release is:

```text
v0.1.0-alpha.2
```

It introduced the verified resumable-transfer foundation developed after `v0.1.0-alpha.1`.

Development after `v0.1.0-alpha.2` now additionally includes:

- bounded automatic reconnect;
- WFP/0.2;
- persistent logical Transfer IDs;
- persistent partial-transfer metadata;
- immutable completion receipts;
- completion reconciliation;
- safe retry after lost final verification.

These post-release changes are development state and do not imply that a new published alpha version already exists.
