# WarpFile

**WarpFile** is an experimental peer-to-peer file transfer application written in Rust.

It transfers files directly between devices using **WFP (WarpFile Protocol)**, its own binary application-layer protocol, without requiring WarpFile cloud storage or permanent user accounts.

The current prototype supports integrity-verified and resumable TCP transfers, persistent receiving, automatic device discovery across local networks and Tailscale, and sending files by device name.

> WarpFile is under active development and is not yet intended for untrusted networks.

## Current capabilities

WarpFile currently supports:

- WFP/0.1 binary framing over TCP;
- streaming transfers without loading the entire file into memory;
- 64 KiB DATA frames;
- incremental BLAKE3 integrity verification;
- file offer acceptance and structured rejection;
- explicit transfer cancellation with `Ctrl+C`;
- `.part` files for incomplete incoming transfers;
- preservation of partial files after recoverable connection loss;
- verified resumable transfers using byte offsets and BLAKE3 prefix hashes;
- automatic restart from byte zero when retained partial data does not match the source;
- avoidance of retransmitting already validated file prefixes;
- resume from arbitrary byte offsets rather than chunk boundaries;
- zero DATA retransmission when the receiver already has the complete validated file contents;
- persistent receivers that can accept multiple sequential transfers;
- UDP peer discovery with WFP `DISCOVER` / `ANNOUNCE`;
- discovery across IPv4 local network interfaces;
- optional Tailscale-assisted peer discovery;
- device-name resolution;
- direct `IP:port` transfers as a fallback;
- unit and end-to-end tests covering protocol, discovery, cancellation, corruption, recovery and real resume flows.

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

When available, WarpFile uses Tailscale only to obtain candidate peer addresses. A Tailscale peer is **not automatically considered a WarpFile peer**.

WarpFile sends its own WFP `DISCOVER` message to each candidate, and only devices actually running WarpFile respond with `ANNOUNCE`.

## Transfer flow

A fresh WFP/0.1 file transfer looks like this:

```text
Sender                                      Receiver
  |                                            |
  | ---------------- HELLO -----------------> |
  | <------------- HELLO_ACK ---------------- |
  |                                            |
  | ---------------- OFFER -----------------> |
  | <--------------- ACCEPT ----------------- |
  |                                            |
  | ---------------- DATA ------------------> |
  | ---------------- DATA ------------------> |
  |                   ...                      |
  |                                            |
  | -------------- COMPLETE ----------------> |
  | <-------------- VERIFIED ---------------- |
  |                                            |
```

The sender calculates a BLAKE3 digest while streaming the file.

The receiver calculates the same digest while writing incoming data and only sends `VERIFIED` when the received file size and BLAKE3 digest are both correct.

## Resumable transfers

Unexpected connection loss does not automatically destroy useful received data.

If a transfer is interrupted by a recoverable network failure, the receiver retains:

```text
<filename>.part
```

When the same filename is offered again, the receiver may propose a resume state:

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

If they do not match:

```text
Sender                                      Receiver
  |                                            |
  | <--------------- RESUME ----------------- |
  |                                            |
  | --------------- RESTART ----------------> |
  |                                            |
  |                      discard stale .part   |
  |                                            |
  | <--------------- ACCEPT ----------------- |
  |                                            |
  | ------------ DATA from byte 0 ----------> |
```

This protects against accidentally combining data from different files that happen to share the same filename or size.

### Resume uses byte offsets

Resume positions are not tied to 64 KiB DATA-frame boundaries.

For example, a transfer may safely resume from:

```text
123457 bytes
```

as long as the BLAKE3 digest of bytes:

```text
[0, 123457)
```

matches on both peers.

This keeps resume independent from current and future chunk-sizing strategies.

### Already-complete partial files

A connection may disappear after the receiver has obtained every file byte but before the final `COMPLETE` / `VERIFIED` exchange finishes.

If the receiver later proposes:

```text
resume offset == complete file size
```

and the full retained prefix matches the source, the sender does not need to retransmit any DATA payload.

It can proceed directly to `COMPLETE`.

## Partial-file behavior

Incoming files are written to:

```text
<filename>.part
```

The final filename is created only after complete size and BLAKE3 verification succeeds.

Different failure types intentionally have different behavior.

Unexpected recoverable connection loss:

```text
retain .part
â†’ offer verified resume later
```

Explicit `CANCEL`:

```text
remove .part
```

Invalid final BLAKE3, impossible transfer state or protocol failure:

```text
remove .part
```

A zero-byte `.part` contains no useful resumable data and is discarded before starting a fresh transfer.

A `.part` larger than the file size announced by the sender cannot be a valid prefix and is also discarded.

## Architecture

The implementation is intentionally separated into independent responsibilities:

```text
CLI
 |
 +-- destination resolution
 |
 +-- discovery
 |    +-- LAN
 |    +-- Tailscale
 |
 +-- sender / receiver
          |
          +-- WFP protocol
                 |
                 +-- framing
                 +-- encoding / decoding
                 +-- transfer payloads
                 +-- resume negotiation
                 |
                 +-- TCP / UDP I/O
```

The transfer layer does not need to know whether an address came from LAN discovery, Tailscale discovery or explicit user input.

This separation is intentional so additional connectivity mechanisms can be introduced later without rewriting file-transfer semantics.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the implementation architecture.

## WarpFile Protocol

WarpFile uses its own experimental application-layer protocol:

```text
WFP/0.1
```

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

Multi-byte integers use big-endian byte order.

The general frame payload limit is 1 MiB. DATA payloads are limited to 64 KiB.

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the evolving protocol specification.

## Reliability

WarpFile already protects transfers against several failure cases.

Transfers detect or handle:

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
- empty or oversized partial-file state.

Verified resume adds a second integrity checkpoint before continuation.

For a resume offset `X`:

```text
Receiver:
BLAKE3(.part bytes 0..X)

Sender:
BLAKE3(source bytes 0..X)
```

The transfer continues from `X` only when those values match.

Final `COMPLETE` / `VERIFIED` still verifies the resulting complete file.

## Current resume limitations

Resume is functional, but the current implementation remains intentionally simple.

It currently does not provide:

- automatic sender reconnect after a failure;
- automatic retry scheduling;
- persistent sender-side transfer tracking;
- persisted BLAKE3 checkpoints;
- directory-transfer manifests;
- simultaneous multi-client receiving.

After an unexpected failure, the user currently starts the send operation again.

The receiver then discovers the retained `.part` state and negotiates resume through WFP.

Retained prefixes must currently be reread locally on both peers to reconstruct BLAKE3 state.

For example:

```text
100 GiB source
8 GiB already retained
```

may require both peers to read and hash those 8 GiB locally again, but only the remaining 92 GiB needs to cross the network.

Future hash checkpoints may reduce this local reread cost.

## Security

**WFP/0.1 currently provides no encryption, authentication or peer identity verification.**

The current implementation should therefore only be used in trusted development environments or over a trusted network layer.

BLAKE3 prefix hashes used during resume prove content equality for the proposed prefix.

They do **not** prove peer identity and must not be treated as authentication.

WarpFile will not design custom cryptographic algorithms. Future authenticated and encrypted sessions will use established cryptographic primitives and libraries.

## Roadmap

### M0 â€” First Byte

Complete.

- TCP sender and receiver;
- WFP framing;
- encoder and decoder;
- `HELLO`;
- `HELLO_ACK`.

### M1 â€” First File

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

### M2 â€” Zero Config

Complete for the current prototype.

- UDP `DISCOVER` / `ANNOUNCE`;
- local network discovery;
- multiple-interface support;
- self-discovery filtering;
- optional Tailscale provider;
- device-name resolution;
- persistent receiver;
- transfer by device name.

### M3 â€” Reliable Transfer

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
- end-to-end resume coverage.

Still planned within the broader reliability milestone:

- automatic reconnect and retry policy;
- improved chunk management;
- persistent transfer metadata;
- hash checkpoints;
- directory transfer design;
- improved path selection.

### M4 â€” Distribution

Planned.

- verified Linux support;
- automated CI;
- release binaries;
- installation workflow;
- broader documentation;
- performance benchmarks.

## Testing

WarpFile uses unit tests and end-to-end tests with real local TCP and UDP sockets.

Current coverage includes:

```text
protocol framing and validation
OFFER / REJECT payloads
RESUME encoding and decoding
BLAKE3 integrity
normal transfers
empty files
cancellation
connection loss
partial retention
prefix mismatch and RESTART
suffix-only resume
100% partial resume with zero DATA retransmission
recovery across two TCP connections
empty and oversized partial states
persistent receiver behavior
LAN discovery
Tailscale parsing
device-name resolution
```

At the time this development state was documented, the suite contains:

```text
72 passing tests
0 failures
```

## Performance philosophy

WarpFile is intended to become performance-oriented, but it will not claim to be faster than other tools without reproducible benchmarks.

A central principle is:

> Never transfer a byte that does not need to be transferred.

Current resume behavior already applies that principle by avoiding retransmission of validated prefixes.

Future optimization work includes:

- minimizing unnecessary copies;
- zero-copy transfer paths where practical;
- adaptive chunks;
- pipelining;
- selective compression;
- deduplication;
- batching;
- automatic path selection;
- multi-source transfers.

Performance work will be measured using throughput, CPU usage, memory usage, protocol overhead and recovery behavior.

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

The optimized executable is generated at:

```text
target\release\warpfile.exe
```

## Platform status

Development and real multi-machine testing have currently focused on Windows.

The core architecture is intended to remain portable, with Linux support as a target.

Tailscale is optional and is not required for LAN transfers.

## Project status

WarpFile is experimental.

Protocol details, CLI behavior and internal architecture may change without backward compatibility before the first stable release.

The current alpha release is `0.1.0-alpha.2`, adding verified resumable-transfer support introduced after `0.1.0-alpha.1`.
