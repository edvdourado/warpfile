# WarpFile Architecture

**Status:** Experimental
**Reference state:** development after `v0.1.0-alpha.2`

This document describes the current architecture of the WarpFile reference implementation.

## 1. Architecture goals

WarpFile should remain:

- cross-platform;
- protocol-driven;
- modular;
- streaming-oriented;
- memory-efficient;
- testable;
- transport-extensible;
- recoverable after connection loss.

A central design principle is separation of responsibilities.

File-transfer code should not need to know how a peer address was discovered.

Discovery code should not implement file-transfer semantics.

Protocol encoding should remain separate from CLI behavior.

Resume behavior should be an explicit WFP protocol feature rather than an implicit filesystem shortcut.

## 2. Current high-level architecture

```text
                         CLI
                          |
          +---------------+---------------+
          |                               |
   destination resolver               receive
          |                               |
          v                               v
      discovery                     receiver loop
          |                               |
    +-----+------+                        |
    |            |                        |
   LAN       Tailscale                    |
    |            |                        |
    +-----+------+                        |
          |                               |
          v                               |
   discovered address                    |
          |                               |
          +---------------+---------------+
                          |
                  sender / receiver
                          |
                          v
                     WFP protocol
                          |
                  +-------+-------+
                  |               |
                 TCP             UDP
             file transfer     discovery
```

The transfer layer operates on a concrete socket address.

It does not need to know whether that address came from:

- explicit user input;
- LAN discovery;
- Tailscale-assisted discovery;
- a future connectivity provider.

## 3. Current source layout

```text
src/
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ main.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ lib.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ destination.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ discovery.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ progress.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ receiver.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ sender.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ tailscale.rs
Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ protocol/
    Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ mod.rs
    Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ frame.rs
    Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ message.rs
    Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ encoder.rs
    Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ decoder.rs
    Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ io.rs
    Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ offer.rs
    Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ reject.rs
    Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ resume.rs
    Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ discovery.rs

tests/
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ discovery_e2e.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ receiver_persistent_e2e.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ resume_receiver_edge_e2e.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ resume_recovery_e2e.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ resume_sender_e2e.rs
Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ transfer_e2e.rs
```

## 4. `main.rs`

`main.rs` is the command-line entry point.

Current commands:

```text
warpfile receive
warpfile discover
warpfile send <file> <address-or-device>
```

Responsibilities:

- read CLI arguments;
- dispatch commands;
- resolve send destinations;
- display discovery results;
- call sender or receiver orchestration.

It should contain minimal protocol logic.

## 5. `destination.rs`

`destination.rs` converts user input into a concrete transfer address.

Input may already be a socket address:

```text
100.68.8.15:42069
```

In that case it is used directly.

Otherwise, the value is treated as a device name:

```text
EDBOOK
```

The destination resolver calls discovery and matches device names case-insensitively.

Example:

```text
EDBOOK
   |
   v
discover_devices()
   |
   v
100.68.8.15:42069
```

If no device matches, resolution fails.

If more than one discovered endpoint has the same device name, WarpFile currently refuses to choose automatically.

This prevents silent route selection before a real path-selection system exists.

## 6. `discovery.rs`

`discovery.rs` implements peer discovery orchestration.

Responsibilities include:

- UDP discovery responder;
- UDP DISCOVER transmission;
- ANNOUNCE collection;
- local interface enumeration;
- subnet broadcast calculation;
- self-discovery filtering;
- aggregation of discovery targets;
- device deduplication.

Default discovery port:

```text
42070/UDP
```

Discovery is intentionally separate from the transfer sender.

The sender ultimately receives only a concrete socket address.

## 7. `tailscale.rs`

`tailscale.rs` is an optional discovery provider.

It executes:

```text
tailscale status --json
```

when the Tailscale CLI is available.

It extracts online IPv4 peer addresses.

Those addresses are treated only as candidate discovery targets.

```text
Tailscale peer
      |
      v
candidate IP
      |
      v
WFP DISCOVER
      |
      +---- valid ANNOUNCE ----> WarpFile peer
      |
      +---- no response -------> ignored
```

WarpFile therefore does not equate Tailscale membership with WarpFile availability.

If Tailscale is missing, returns an error or produces invalid JSON, the provider yields no candidates and LAN discovery remains usable.

## 8. `sender.rs`

`sender.rs` controls outgoing file-transfer orchestration.

`run_sender` validates the source file and applies sender-side retry policy.

Each `send_once` invocation represents one independent WFP TCP transfer session.

Responsibilities include:

- validate the source file;
- connect to the destination;
- perform HELLO / HELLO_ACK negotiation;
- send OFFER;
- handle ACCEPT, REJECT or RESUME;
- validate resumable prefixes;
- send ACCEPT when a proposed resume state is valid;
- send RESTART when a proposed resume state is unusable;
- stream only missing file data after an accepted resume;
- update BLAKE3 incrementally;
- update progress reporting;
- send CANCEL after user interruption;
- send COMPLETE;
- wait for VERIFIED;
- classify recoverable network failures separately from permanent failures;
- retry selected recoverable failures with a bounded attempt policy;
- reconnect through a new WFP session and allow normal verified resume negotiation;
- avoid automatic retry after ambiguous COMPLETE / VERIFIED finalization.

### 8.1 Fresh transfer

For a fresh transfer:

```text
OFFER
  |
  v
ACCEPT
  |
  v
read source from byte 0
  |
  +----> BLAKE3
  |
  +----> DATA
```

### 8.2 Resume negotiation

When the receiver sends RESUME:

```text
RESUME
  |
  +-- offset
  |
  +-- BLAKE3 prefix digest
```

the sender reads exactly the requested source prefix.

Example:

```text
source file

0 ---------------- X ---------------- total
        local read
```

The sender updates a BLAKE3 hasher while reading that prefix.

It then compares:

```text
BLAKE3(local source bytes 0..X)

vs

BLAKE3(receiver .part bytes 0..X)
```

If they match:

```text
send ACCEPT
     |
     v
continue reading at X
     |
     +----> DATA
     |
     +----> same BLAKE3 state
```

No explicit seek is required in the current implementation because reading the prefix naturally leaves the file cursor at the requested offset.

If the hashes differ:

```text
send RESTART
     |
     v
wait for receiver ACCEPT
     |
     v
reopen source file
     |
     v
start from byte 0
```

The sender also sends RESTART if the receiver proposes an offset beyond the source file size.

### 8.3 Resume and network traffic

A validated prefix is read locally but is not retransmitted.

For example:

```text
file size:      400,000 bytes
resume offset:  123,457 bytes

network DATA after resume:

400,000 - 123,457
= 276,543 bytes
```

If the receiver already has the complete correct file contents in `.part`:

```text
resume offset == file size
```

then the sender may send zero DATA bytes and proceed directly to COMPLETE after validating the full prefix.

### 8.4 Automatic reconnect and retry policy

The sender currently allows up to three total transfer attempts, with a one-second delay between attempts.

A selected recoverable network failure before finalization causes the current TCP session to end. The sender waits, opens a new TCP connection, performs HELLO and OFFER again, and lets the receiver propose retained partial state through normal RESUME negotiation.

Retry policy belongs to sender orchestration rather than to one WFP session.

Permanent receiver decisions such as REJECT, protocol validation failures, local source-file failures and explicit cancellation do not trigger automatic retry.

### 8.5 Ambiguous completion

After COMPLETE enters finalization, connection loss before VERIFIED is not automatically retried.

At that point the sender cannot determine whether COMPLETE failed to reach the receiver or whether the receiver already verified and committed the file but VERIFIED was lost.

The sender therefore reports the receiver completion status as unknown.

Automatic reconciliation of this state would require additional protocol identity or status semantics, such as a transfer identifier and completion-status query.

## 9. `receiver.rs`

`receiver.rs` controls incoming transfers.

The receiver is persistent at the process level.

Conceptually:

```text
start receiver
     |
     v
accept connection
     |
     v
receive one file
     |
     v
accept next connection
     |
     v
...
```

Each TCP connection still represents one file-transfer session.

Responsibilities include:

- listen on the TCP transfer port;
- perform WFP negotiation;
- validate incoming filenames;
- reject unsafe or conflicting destinations;
- inspect existing `.part` files;
- classify partial state as fresh, resumable or unusable;
- rebuild BLAKE3 state from retained prefixes;
- send RESUME with offset and prefix digest;
- handle ACCEPT, RESTART and CANCEL during resume negotiation;
- append new DATA after an accepted resume;
- receive DATA frames;
- update BLAKE3 incrementally;
- verify final size and hash;
- promote the partial file after validation;
- preserve partial data after recoverable connection loss;
- remove partial data after cancellation or invalid transfer state;
- return to the accept loop after a client error.

A failed client transfer therefore does not terminate the whole receiver process.

## 10. Partial-file state machine

Incoming files use:

```text
filename.part
```

The receiver never exposes the final filename before complete size and BLAKE3 verification.

Conceptually:

```text
                     OFFER
                       |
                       v
              final file exists?
                 /          \
               yes          no
                |            |
             REJECT          v
                         .part exists?
                         /          \
                       no           yes
                       |             |
                       v             v
                 fresh transfer   inspect size
                                     |
                         +-----------+-----------+
                         |                       |
                      usable                 unusable
                         |                       |
                         v                       v
                    hash .part              discard .part
                         |                       |
                         v                       v
                     RESUME                fresh ACCEPT
```

A partial file is currently considered unusable if:

```text
partial size == 0
```

or:

```text
partial size > announced file size
```

A non-empty partial file no larger than the announced file is proposed to the sender through RESUME.

The sender is responsible for validating that the retained bytes actually match its source file.

## 11. Recoverable connection loss

Unexpected connection loss is treated differently from explicit cancellation.

Recoverable network failure:

```text
DATA received
     |
     v
connection disappears
     |
     v
preserve .part
```

The current implementation recognizes selected underlying I/O failures as recoverable connection loss.

The preserved file becomes a resume candidate on a later transfer attempt.

For selected recoverable sender-side network failures, the current sender automatically creates that later attempt, subject to its bounded retry policy.

This does not mean it is automatically trusted.

The prefix must still pass BLAKE3 verification against the sender's current source file.

## 12. Explicit cancellation

During an active send, the sender listens for `Ctrl+C`.

When cancellation is requested:

```text
Ctrl+C
  |
  v
finish current frame boundary
  |
  v
send CANCEL
  |
  v
sender exits
```

The implementation avoids intentionally interrupting a DATA frame halfway through writing it to the TCP stream.

The receiver handles CANCEL as an explicit abort and removes the current `.part`.

Therefore:

```text
connection loss
Ã¢â€ â€™ retain partial

CANCEL
Ã¢â€ â€™ remove partial
```

This distinction is intentional.

## 13. Invalid transfer state

Partial state is not retained after failures that make the data untrustworthy.

Examples include:

- invalid final BLAKE3 digest;
- impossible received size;
- malformed transfer protocol state;
- invalid CANCEL payload;
- unexpected WFP message during transfer.

Conceptually:

```text
suspicious / invalid state
          |
          v
      remove .part
```

Resume should recover network interruption, not preserve known-invalid data.

## 14. Discovery and receiver concurrency

A running receiver serves two independent functions:

```text
TCP 42069
file-transfer listener

UDP 42070
discovery responder
```

The receiver orchestration keeps both active.

This allows a receiver to remain discoverable while waiting for or processing sequential file transfers.

Transfer sessions are currently processed sequentially rather than concurrently.

## 15. `progress.rs`

`progress.rs` provides transfer progress output.

It tracks:

- transferred bytes;
- total bytes;
- percentage;
- average throughput;
- estimated remaining time.

Progress rendering is throttled rather than redrawn on every transferred chunk.

It also ensures an interrupted progress line is terminated cleanly before error text is printed.

For a resumed transfer, the current sender and receiver progress trackers operate on the remaining transfer portion rather than pretending that already retained bytes are being transferred again.

## 16. Protocol module

The `protocol` directory contains WFP-specific encoding and validation.

```text
protocol/
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ frame.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ message.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ encoder.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ decoder.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ io.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ offer.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ reject.rs
Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ resume.rs
Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ discovery.rs
```

### `frame.rs`

Defines:

- WFP magic;
- protocol version;
- frame header size;
- general payload limit;
- DATA payload limit;
- low-level `Frame` representation.

Conceptually:

```text
Frame {
    version,
    message_type,
    flags,
    payload
}
```

### `message.rs`

Defines recognized WFP message type identifiers.

Current message types:

```text
HELLO
HELLO_ACK

OFFER
ACCEPT
REJECT
RESUME
RESTART

DATA

COMPLETE
VERIFIED

CANCEL

DISCOVER
ANNOUNCE

ERROR
```

### `encoder.rs`

Converts validated frames to network bytes.

Responsibilities include:

- magic;
- version;
- type;
- flags;
- payload length;
- payload;
- size validation.

### `decoder.rs`

Converts received bytes into validated frames.

It rejects invalid:

- magic values;
- protocol versions;
- message types;
- flags;
- payload sizes;
- incomplete frames.

### `io.rs`

Provides asynchronous WFP frame reading and writing over byte streams.

TCP is a byte stream and does not preserve application-message boundaries.

`io.rs` therefore reads:

```text
12-byte header
      |
      v
payload length
      |
      v
exact payload bytes
```

before returning one complete frame.

It preserves the distinction between:

- underlying I/O failures;
- frame decoding failures;
- frame encoding failures.

That distinction is useful when deciding whether an interrupted partial transfer may be retained.

### `offer.rs`

Encodes and decodes OFFER payloads:

```text
filename length
filename
file size
```

### `reject.rs`

Encodes and decodes structured REJECT payloads.

Current codes:

```text
FILE_EXISTS
UNSAFE_FILENAME
CANNOT_PREPARE_DESTINATION
```

### `resume.rs`

Encodes and decodes RESUME payloads.

Current structure:

```text
ResumeRequest {
    offset: u64,
    prefix_hash: [u8; 32]
}
```

Wire payload:

```text
8-byte big-endian offset
+
32-byte BLAKE3 prefix digest
=
40 bytes
```

The protocol represents resume positions as byte offsets rather than chunk numbers.

This keeps resume independent from current or future DATA chunk sizing.

### `discovery.rs`

Encodes and decodes ANNOUNCE payloads.

The announcement contains:

```text
device name
TCP transfer port
```

DISCOVER itself uses an empty payload.

## 17. Streaming model

WarpFile must not load complete files into memory.

Current DATA payload maximum:

```text
64 KiB
```

Conceptually:

```text
File
 |
 +-- 64 KiB --> DATA
 |
 +-- 64 KiB --> DATA
 |
 +-- 64 KiB --> DATA
 |
 +-- ...
```

Memory use therefore remains approximately bounded with respect to transfer size.

A very large file should not require memory proportional to its total size.

## 18. Integrity model

BLAKE3 is the current file-integrity mechanism.

For a fresh transfer:

Sender:

```text
                       +--> Network
File --> chunk --------+
                       +--> BLAKE3
```

Receiver:

```text
                        +--> File
Network --> chunk ------+
                        +--> BLAKE3
```

The final digest is transferred in COMPLETE.

The receiver sends VERIFIED only when its independently calculated digest matches and the byte count is correct.

## 19. Resume integrity model

Resume requires both peers to reconstruct hash state for the retained prefix.

Receiver:

```text
filename.part
      |
      v
read retained prefix
      |
      +----> BLAKE3 state
      |
      +----> prefix digest in RESUME
```

Sender:

```text
source file
      |
      v
read same prefix
      |
      +----> BLAKE3 state
      |
      +----> compare digest
```

If equal:

```text
prefix BLAKE3 state
        |
        v
continue with new DATA
        |
        v
complete-file BLAKE3
```

This provides two integrity checks:

```text
resume stage:
verify retained prefix

completion stage:
verify complete resulting file
```

Filename, file size and partial length alone are not sufficient proof that a `.part` belongs to the current source file.

## 20. Resume performance model

The current design avoids retransmitting validated network data but does require local rereads of retained prefixes.

Example:

```text
100 GiB file
8 GiB retained .part
```

Current resume requires:

Receiver:

```text
read/hash 8 GiB locally
```

Sender:

```text
read/hash 8 GiB locally
```

Network:

```text
transmit only remaining 92 GiB
```

This is intentionally correct before being maximally optimized.

Future improvements may persist:

- BLAKE3 checkpoints;
- chunk hashes;
- transfer metadata;
- manifests.

Those could reduce the amount of retained prefix that must be reread locally.

## 21. Testing strategy

WarpFile uses both unit and end-to-end tests.

Unit tests cover components such as:

- frame encoding;
- frame decoding;
- payload validation;
- offer parsing;
- rejection parsing;
- resume payload encoding and decoding;
- discovery announcements;
- interface broadcast calculations;
- Tailscale JSON parsing;
- destination-name resolution;
- progress formatting.

End-to-end tests use real local TCP or UDP sockets.

Current E2E coverage includes:

- complete file transfer;
- empty files;
- destination conflicts;
- unexpected sender disconnection;
- retained partial files;
- corrupted hashes;
- explicit cancellation;
- UDP discovery;
- multiple sequential transfers through a persistent receiver;
- receiver-generated RESUME negotiation;
- sender validation of resume prefixes;
- RESTART after mismatched prefixes;
- transfer of only the missing suffix;
- complete `.part` state with zero DATA retransmission;
- recovery after a real TCP connection loss;
- empty partial state;
- oversized impossible partial state;
- real sender and real receiver resume flow;
- automatic sender reconnect after connection loss;
- verified resume inside the automatically created retry session;
- maximum retry-attempt enforcement;
- permanent receiver REJECT without retry;
- ambiguous COMPLETE / VERIFIED loss without retry.

The current suite contains 80 passing tests at the time this architecture state was documented.

## 22. Resume recovery test model

One important integration test does not fabricate the receiver's `.part` file manually.

Instead:

```text
first TCP connection
      |
      v
real receiver
      |
      v
receive 123,457 bytes
      |
      v
connection disappears
      |
      v
.part survives
```

Then:

```text
second TCP connection
      |
      v
real run_sender()
      |
      v
real receiver
      |
      v
RESUME
      |
      v
prefix verification
      |
      v
only missing suffix transferred
      |
      v
VERIFIED
```

Using an offset that is not aligned to 64 KiB demonstrates that resume is byte-position based rather than chunk-number based.

## 23. Async runtime

WarpFile uses Tokio.

Tokio currently provides:

- asynchronous TCP;
- asynchronous UDP;
- asynchronous file I/O;
- signal handling;
- async scheduling.

Concurrency is introduced only where required.

The current receiver handles sequential file transfers rather than simultaneous multi-client transfers.

Correctness currently takes priority over maximum throughput.

## 24. Platform independence

Core transfer and WFP logic should avoid unnecessary operating-system-specific behavior.

Current real multi-machine development has focused on Windows.

Linux remains a target platform.

Platform-specific connectivity integrations should remain isolated behind dedicated modules or providers.

Tailscale integration is one example.

## 25. Security boundaries

All network input is untrusted.

The implementation validates:

- WFP magic;
- protocol version;
- message types;
- flags;
- payload lengths;
- UTF-8 fields;
- filename safety;
- announced file sizes;
- resume offsets;
- resume prefix hashes;
- actual received byte count;
- final BLAKE3 integrity.

Sender-provided filesystem paths are never accepted as destination paths.

WFP/0.1 currently does not provide encryption or authenticated peer identity.

Resume hashes prove content equality for a prefix.

They do not prove peer identity and are not a replacement for authentication.

## 26. Route selection

WarpFile can currently discover the same conceptual machine through different connectivity paths.

Example:

```text
EDBOOK 192.168.1.20:42069
EDBOOK 100.68.8.15:42069
```

The current destination resolver treats this as ambiguous rather than guessing.

Future route selection may evaluate:

- direct LAN;
- IPv6;
- overlay-network paths;
- NAT traversal;
- relay fallback;
- latency;
- throughput;
- availability.

The selection mechanism should be based on measured or meaningful connectivity information rather than hard-coded assumptions.

## 27. Current resume limitations

Resume is functional, but the current implementation intentionally remains simple.

Current limitations include:


- no persistent sender-side transfer database;
- no persistent transfer identity;
- no BLAKE3 checkpoint cache;
- retained prefixes are reread to rebuild hash state;
- one file per TCP transfer session;
- no directory-transfer manifest;
- sequential rather than concurrent receiver sessions.

For selected recoverable network failures, the sender automatically creates a new transfer attempt. Other failures still require explicit user action.

The receiver then proposes the retained state through RESUME.

## 28. Future reliability work

Possible next reliability improvements include:

```text
persistent transfer metadata
        |
        v
transfer identity and completion reconciliation
        |
        v
hash checkpoints
        |
        v
improved chunk management
```

Directory transfers will require additional design because a directory is a collection of multiple file states rather than one byte stream.

Resume should remain protocol-driven and explicitly testable.

## 29. Future WorldLink integration

WarpFile is intentionally being built before WorldLink.

The intended process is:

```text
working implementation
        |
        v
observed reusable networking concept
        |
        v
generalization
```

Potential functionality that may eventually be extracted includes:

- peer discovery;
- identity;
- session establishment;
- authenticated encryption;
- NAT traversal;
- relay connectivity;
- capability negotiation.

File-transfer semantics remain WarpFile responsibilities.

## 30. Engineering principle

WarpFile prefers:

```text
working implementation
        |
        v
measured behavior
        |
        v
observed reusable concept
        |
        v
generalization
```

over:

```text
hypothetical abstraction
        |
        v
large framework
        |
        v
hope that applications need it
```

The architecture should grow from real requirements.
