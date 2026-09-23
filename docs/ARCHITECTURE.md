# WarpFile Architecture

**Status:** Experimental

**Reference state:** development after `v0.1.0-alpha.2`

**Current protocol:** WFP/0.2

This document describes the current architecture of the WarpFile reference implementation.

---

## 1. Architecture goals

WarpFile should remain:

- cross-platform;
- protocol-driven;
- modular;
- streaming-oriented;
- memory-efficient;
- testable;
- transport-extensible;
- recoverable after connection loss;
- explicit about persistent transfer state;
- conservative when completion state cannot be verified.

A central design principle is separation of responsibilities.

File-transfer code should not need to know how a peer address was discovered.

Discovery code should not implement file-transfer semantics.

Protocol encoding should remain separate from CLI behavior.

Resume and completion reconciliation should be explicit protocol behaviors rather than implicit filesystem shortcuts.

Persistent metadata must assist recovery without becoming a substitute for verification of the actual file bytes.

---

## 2. Current high-level architecture

```text
                         CLI
                          |
          +---------------+---------------+
          |                               |
 destination resolver                  receive
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
                     WFP/0.2
                     WFP/0.3 (opt-in)
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

The receiver additionally maintains local persistent transfer state:

```text
receiver
   |
   +--> .warpfile/
          |
          +--> partials/
          |      +--> <filename>.part
          |      +--> <filename>.part.warpmeta
          |
          +--> receipts/
                 |
                 +--> <transfer-id>.json
```

The `.part` file contains physical transfer bytes.

The `.warpmeta` file tracks partial-transfer ownership and metadata.

The completion receipt records a successfully verified logical transfer and allows a later TCP session using the same Transfer ID to reconcile completion safely.

---

## 3. Current source layout

```text
src/
|-- main.rs
|-- lib.rs
|-- chunk.rs
|-- chunk_manifest.rs
|-- chunk_state.rs
|-- completion_receipt.rs
|-- destination.rs
|-- discovery.rs
|-- progress.rs
|-- receiver.rs
|-- receiver_v03.rs
|-- sender.rs
|-- sender_v03.rs
|-- tailscale.rs
|-- transfer_metadata.rs
`-- protocol/
    |-- mod.rs
    |-- frame.rs
    |-- message.rs
    |-- encoder.rs
    |-- decoder.rs
    |-- io.rs
    |-- offer.rs
    |-- reject.rs
    |-- resume.rs
    |-- transfer_id.rs
    |-- discovery.rs
    `-- v03.rs

tests/
|-- automatic_reconnect_e2e.rs
|-- completion_reconciliation_e2e.rs
|-- discovery_e2e.rs
|-- receiver_persistent_e2e.rs
|-- resume_receiver_edge_e2e.rs
|-- resume_recovery_e2e.rs
|-- resume_sender_e2e.rs
|-- retry_policy_e2e.rs
|-- transfer_e2e.rs
|-- transfer_metadata_e2e.rs
`-- wfp_v03_e2e.rs
```

The layout separates:

```text
wire protocol
    |
    v
protocol/

transfer orchestration
    |
    +--> sender.rs
    |
    `--> receiver.rs

persistent recovery state
    |
    +--> transfer_metadata.rs
    |
    `--> completion_receipt.rs

connectivity/discovery
    |
    +--> discovery.rs
    |
    +--> destination.rs
    |
    `--> tailscale.rs
```

---

## 4. `main.rs`

`main.rs` is the command-line entry point.

Current commands include:

```text
warpfile receive
warpfile discover
warpfile send
```

Responsibilities:

- read CLI arguments;
- dispatch commands;
- resolve send destinations;
- display discovery results;
- call sender or receiver orchestration.

It should contain minimal protocol logic.

---

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

---

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

Discovery uses the current WFP framing version, so WFP/0.2 applies to both TCP transfer frames and UDP discovery frames.

---

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

---

## 8. `sender.rs`

`sender.rs` controls outgoing file-transfer orchestration.

`run_sender` owns one logical send job.

Each `send_once` invocation represents one WFP TCP session belonging to that logical job.

The important distinction is:

```text
run_sender
logical transfer
Transfer ID = X
      |
      +--> send_once / TCP session 1
      |
      +--> send_once / TCP session 2
      |
      `--> send_once / TCP session 3
```

Responsibilities include:

- validate the source file;
- generate one Transfer ID for the logical send job;
- preserve that Transfer ID across automatic retries;
- connect to the destination;
- perform HELLO / HELLO_ACK negotiation;
- send OFFER containing Transfer ID, filename and file size;
- handle ACCEPT, REJECT, RESUME or direct VERIFIED;
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
- reconcile lost final verification through a later WFP session.

### 8.1 Transfer identity

The sender generates one 128-bit Transfer ID at the beginning of `run_sender`.

Conceptually:

```text
run_sender()
    |
    v
generate Transfer ID
    |
    +--> attempt 1
    |
    +--> attempt 2
    |
    `--> attempt 3
```

The same ID is reused for every automatic retry.

It is not:

- peer authentication;
- content identity;
- a file hash;
- a secret token.

It identifies one logical send operation.

If the sender process itself is restarted, the current implementation generates a new Transfer ID.

Persistent sender-side job identity is future work.

### 8.2 Fresh transfer

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

### 8.3 Resume negotiation

When the receiver sends RESUME:

```text
RESUME
  |
  +-- offset
  |
  `-- BLAKE3 prefix digest
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
     `----> same BLAKE3 state
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

### 8.4 Resume and network traffic

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

### 8.5 Automatic reconnect and retry policy

The sender currently allows:

```text
maximum attempts: 3
retry delay:      1 second
```

Retry policy belongs to `run_sender` orchestration rather than to one WFP session.

For a selected recoverable network failure:

```text
session fails
    |
    v
run_sender waits
    |
    v
new TCP connection
    |
    v
HELLO / HELLO_ACK
    |
    v
OFFER with same Transfer ID
```

Permanent receiver decisions such as REJECT, protocol validation failures, local source-file failures and explicit cancellation do not trigger automatic retry.

### 8.6 Finalization retry

Connection loss around COMPLETE / VERIFIED is now recoverable.

Previously the sender had an `AmbiguousCompletion` state because it could not determine whether the receiver had already committed the file.

WFP/0.2 transfer identity and persistent receiver completion receipts remove that ambiguity for retries belonging to the same running sender job.

Conceptually:

```text
attempt 1
    |
    v
COMPLETE
    |
    v
receiver verifies transfer
    |
    v
receiver persists completion receipt
    |
    X
VERIFIED is lost
```

The sender treats the transport failure as recoverable:

```text
attempt 2
    |
    v
same Transfer ID
    |
    v
OFFER
```

If the receiver confirms that this logical transfer was already completed:

```text
OFFER
  |
  v
VERIFIED
```

The sender considers the transfer successful without retransmitting DATA.

### 8.7 Direct VERIFIED after OFFER

WFP/0.2 permits VERIFIED as a valid direct response to OFFER.

The sender validates that the VERIFIED payload is empty.

This branch runs before opening the source file for DATA transmission inside the session.

Conceptually:

```text
OFFER
  |
  v
VERIFIED
  |
  v
success
```

There is no:

```text
ACCEPT
DATA
COMPLETE
```

in that reconciled session.

---

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
handle one WFP session
     |
     v
accept next connection
     |
     v
...
```

Each TCP connection represents one WFP transfer session.

One logical transfer may, however, span more than one session through the same Transfer ID.

Responsibilities include:

- listen on the TCP transfer port;
- perform WFP negotiation;
- validate incoming filenames;
- decode Transfer IDs from OFFER;
- reject unsafe or conflicting destinations;
- check completion receipts before normal FILE_EXISTS handling;
- inspect existing `.part` files;
- inspect persistent `.warpmeta` state;
- classify partial state as fresh, resumable or unusable;
- rebuild BLAKE3 state from retained prefixes;
- send RESUME with offset and prefix digest;
- handle ACCEPT, RESTART and CANCEL during resume negotiation;
- adopt cryptographically verified partial data when appropriate;
- append new DATA after an accepted resume;
- receive DATA frames;
- update BLAKE3 incrementally;
- verify final size and hash;
- synchronize completed partial data;
- persist completion receipts;
- promote the partial file after validation;
- reconcile previously completed transfers;
- send VERIFIED directly after OFFER when reconciliation succeeds;
- preserve partial data after recoverable connection loss;
- remove partial data after cancellation or invalid transfer state;
- return to the accept loop after a client error.

A failed client transfer therefore does not terminate the whole receiver process.

---

## 10. `transfer_metadata.rs`

`transfer_metadata.rs` implements persistent metadata for incomplete transfers.

For:

```text
.warpfile/partials/video.mkv.part
```

the sidecar path is:

```text
.warpfile/partials/video.mkv.part.warpmeta
```

The current metadata schema conceptually contains:

```text
format_version
transfer_id
filename
file_size
state = partial
```

Its local metadata format version is independent from WFP/0.2.

Responsibilities include:

- derive metadata paths;
- encode metadata;
- decode metadata;
- validate schema fields;
- parse Transfer IDs;
- write metadata through a temporary file;
- synchronize metadata contents;
- replace existing metadata;
- remove metadata and stale temporary state.

The metadata is auxiliary state.

It does not prove that `.part` bytes match the sender's current source.

That proof still comes from BLAKE3 prefix verification.

### 10.1 Fresh transfer state

A fresh receiver state is prepared conceptually as:

```text
create .part
    |
    v
write .warpmeta
    |
    v
ACCEPT
```

The receiver therefore records the intended partial-transfer identity before telling the sender to begin DATA transmission.

### 10.2 Verified partial adoption

A useful `.part` is not automatically discarded just because metadata is:

- missing;
- corrupt;
- associated with another Transfer ID.

Instead:

```text
existing .part
      |
      v
receiver hashes actual prefix
      |
      v
RESUME
      |
      v
sender hashes source prefix
      |
      v
matching BLAKE3?
```

If yes:

```text
ACCEPT
   |
   v
receiver rewrites .warpmeta
for current Transfer ID
   |
   v
append suffix
```

This permits a new sender process with a newly generated Transfer ID to adopt already valid partial bytes.

---

## 11. `completion_receipt.rs`

`completion_receipt.rs` implements persistent immutable records for completed logical transfers.

Receipts are stored under:

```text
<destination>/
`-- .warpfile/
    `-- receipts/
        `-- <transfer-id>.json
```

A receipt conceptually contains:

```text
format_version
transfer_id
filename
file_size
blake3
```

The receipt format version is independent from the WFP protocol version.

Responsibilities include:

- derive receipt paths from Transfer IDs;
- encode and decode the local JSON format;
- validate Transfer IDs;
- validate BLAKE3 values;
- create the receipt directory;
- persist through a temporary file;
- flush and synchronize receipt contents;
- promote the temporary receipt to its final path;
- read existing receipts;
- remove receipts;
- enforce immutable/idempotent receipt semantics.

### 11.1 Immutable receipt behavior

If a receipt already exists for a Transfer ID:

```text
existing == requested
```

the write is idempotent.

If:

```text
existing != requested
```

the operation reports a conflicting receipt.

The implementation does not silently redefine an existing completed logical transfer.

### 11.2 Receipt trust boundary

A receipt is persistent receiver state.

It is not trusted as proof that the current physical file is still correct.

During reconciliation, the receiver verifies the physical file's size and BLAKE3 digest before sending VERIFIED.

---

## 12. Partial-file state machine

Incoming incomplete files use:

```text
.warpfile/partials/filename.part
```

and normally have:

```text
.warpfile/partials/filename.part.warpmeta
```

The receiver never exposes the final filename before complete size and BLAKE3 validation.

For an OFFER without an applicable completion receipt, the partial path behaves conceptually as:

```text
                     OFFER
                       |
                       v
                  .part exists?
                  /           \
                no             yes
                |               |
                v               v
          fresh state       inspect size
                |               |
                |        +------+------+
                |        |             |
                |      usable       unusable
                |        |             |
                |        v             v
                |     hash .part    discard state
                |        |             |
                |        v             v
                |      RESUME      fresh state
                |                      |
                +----------+-----------+
                           |
                           v
                     transfer flow
```

A partial file is unusable if:

```text
partial size == 0
```

or:

```text
partial size > announced file size
```

A non-empty partial file no larger than the announced file is a candidate for RESUME.

Candidate does not mean trusted.

The sender must validate that the retained bytes actually match its source.

---

## 13. Completion state machine

WFP/0.2 adds a persistent completion path before normal destination-conflict handling.

Conceptually:

```text
                        OFFER
                          |
                          v
              receipt for Transfer ID?
                    /             \
                  no               yes
                  |                 |
                  v                 v
          normal transfer      validate receipt
                                    |
                                    v
                        OFFER metadata matches?
                              /           \
                            no             yes
                            |               |
                            v               v
                         REJECT       final exists?
                                      /          \
                                    yes           no
                                    |              |
                                    v              v
                              verify final      full .part?
                                    |           /        \
                                    |         yes         no
                                    |          |           |
                                    |          v           v
                                    |    verify .part    REJECT
                                    |          |
                                    +-----+----+
                                          |
                                          v
                                   physical hash valid?
                                      /          \
                                    no            yes
                                    |              |
                                    v              v
                                 REJECT       ensure final
                                                   |
                                                   v
                                                VERIFIED
```

A receipt is therefore not enough by itself.

The receiver must connect:

```text
persistent logical state
+
physical verified bytes
```

before reporting completion.

---

## 14. Receiver finalization ordering

Normal successful finalization follows this architecture:

```text
receive all DATA
      |
      v
receive COMPLETE
      |
      v
flush / sync .part
      |
      v
validate byte count
      |
      v
validate BLAKE3
      |
      v
persist immutable completion receipt
      |
      v
rename .part -> final
      |
      v
remove .warpmeta
      |
      v
VERIFIED
```

The important ordering rule is:

```text
verified bytes
    BEFORE
completion receipt

completion receipt
    BEFORE
final rename / VERIFIED
```

This creates a recoverable state for a process/network interruption between receipt persistence and final rename.

For example:

```text
.warpfile/partials/video.mkv.part
.warpfile/partials/video.mkv.part.warpmeta
.warpfile/receipts/<id>.json
```

can represent a completed, verified transfer whose final rename has not yet occurred.

The receiver can reconcile that state later.

---

## 15. Completion reconciliation

Completion reconciliation is intentionally a rare recovery path.

### 15.1 Existing final file

If the receipt exists and the final file exists:

```text
receipt
  |
  v
compare Transfer ID / filename / size
  |
  v
inspect final physical file
  |
  v
rehash final with BLAKE3
  |
  v
matches receipt?
  |
  +-- no --> reject
  |
  `-- yes -> VERIFIED
```

The physical file is rehashed because local persistent metadata must not be blindly trusted.

### 15.2 Complete `.part`

If the receipt exists, the final path is absent, and the completed `.part` exists:

```text
receipt
   |
   v
verify .part size + BLAKE3
   |
   v
rename .part -> final
   |
   v
remove partial metadata
   |
   v
VERIFIED
```

This repairs the finalization window after a receipt was written but before the final file was committed.

### 15.3 Invalid receipt state

If a receipt exists but physical data cannot confirm it:

```text
receipt
+
missing / altered / mismatched file
        |
        v
      REJECT
```

The receiver never sends VERIFIED solely because the receipt file exists.

---

## 16. Recoverable connection loss

Unexpected connection loss is treated differently from explicit cancellation.

During DATA transfer:

```text
DATA received
     |
     v
connection disappears
     |
     v
preserve .part
     |
     v
preserve .warpmeta
```

The preserved file becomes a resume candidate on a later transfer attempt.

For selected recoverable sender-side network failures, `run_sender` automatically creates that later attempt using the same Transfer ID.

The partial data is still not automatically trusted.

The prefix must pass BLAKE3 verification against the sender's source file.

During finalization, the same retry infrastructure is now also used.

If completion had already been committed, the new OFFER can be reconciled through the receipt path instead of retransmitting DATA.

---

## 17. Explicit cancellation

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

The receiver handles CANCEL as an explicit abort and removes current partial state.

Therefore:

```text
unexpected connection loss
        |
        v
retain recoverable partial state


CANCEL
   |
   v
remove partial state
```

This distinction is intentional.

---

## 18. Invalid transfer state

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
 remove partial state
```

Resume should recover network interruption, not preserve known-invalid data.

Completion receipts are handled more conservatively: an inconsistent receipt or inconsistent physical completed file does not authorize VERIFIED.

---

## 19. Discovery and receiver concurrency

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

This simplifies:

- destination conflict handling;
- partial metadata ownership;
- receipt creation;
- completion reconciliation.

Concurrency safety for simultaneous transfers to the same destination is not yet a design goal.

---

## 20. `progress.rs`

`progress.rs` provides transfer progress output.

It tracks:

- transferred bytes;
- total bytes;
- percentage;
- average throughput;
- estimated remaining time.

Progress rendering is throttled rather than redrawn on every transferred chunk.

It also ensures an interrupted progress line is terminated cleanly before error text is printed.

For a resumed transfer, progress operates on the remaining network-transfer portion rather than pretending that already retained bytes are being transferred again.

---

## 21. Protocol module

The `protocol` directory contains WFP-specific framing, payload encoding and validation.

```text
protocol/
|-- frame.rs
|-- message.rs
|-- encoder.rs
|-- decoder.rs
|-- io.rs
|-- offer.rs
|-- reject.rs
|-- resume.rs
|-- transfer_id.rs
`-- discovery.rs
```

### `frame.rs`

Defines:

- WFP magic;
- protocol version;
- frame header size;
- general payload limit;
- DATA payload limit;
- low-level `Frame` representation.

Current protocol version:

```text
WFP/0.2
0x02
```

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

That distinction is used by sender retry classification and receiver recovery behavior.

### `offer.rs`

Encodes and decodes WFP/0.2 OFFER payloads.

Current wire structure:

```text
16 bytes    Transfer ID
2 bytes     filename length
N bytes     UTF-8 filename
8 bytes     file size
```

Conceptually:

```text
FileOffer {
    transfer_id,
    filename,
    file_size
}
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

### `transfer_id.rs`

Defines the WFP Transfer ID primitive.

Current representation:

```text
16 bytes
128 bits
```

Responsibilities include:

- preserve raw ID bytes;
- generate IDs using operating-system randomness;
- expose byte representation;
- format IDs as lowercase hexadecimal text.

Human-readable form:

```text
0123456789abcdef0123456789abcdef
```

Transfer IDs are correlation identifiers, not authentication credentials.

### `discovery.rs`

Encodes and decodes ANNOUNCE payloads.

The announcement contains:

```text
device name
TCP transfer port
```

DISCOVER itself uses an empty payload.

---

## 22. Streaming model

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

The normal transfer path also updates BLAKE3 incrementally rather than pre-hashing the complete source before transmission.

---

## 23. Integrity model

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

The receiver sends VERIFIED only when:

- the byte count is correct;
- the resulting BLAKE3 matches;
- required persistent completion state has been established;
- the completed file is committed or has been successfully reconciled.

BLAKE3 is used for integrity.

It is not peer authentication.

---

## 24. Resume integrity model

Resume requires both peers to reconstruct hash state for the retained prefix.

Receiver:

```text
.warpfile/partials/filename.part
      |
      v
read retained prefix
      |
      +----> BLAKE3 state
      |
      `----> prefix digest in RESUME
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
      `----> compare digest
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

Filename, file size, partial length, Transfer ID and `.warpmeta` alone are not sufficient proof that a `.part` belongs to the current source file.

---

## 25. Resume performance model

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

- BLAKE3 checkpoints.

Chunk hashes and chunk manifests are implemented in WFP/0.3 (chunk-aware resume) via `--wfp-version=0.3`.

Transfer metadata itself is no longer future work; `.warpmeta` is already implemented.

---

## 26. Completion reconciliation performance

The normal successful transfer path hashes bytes while they are already being streamed.

It does not perform a second complete-file read merely to create a receipt.

Reconciliation is different.

A later session that finds a completion receipt rehashes the physical final file or completed `.part` before returning direct VERIFIED.

Conceptually:

```text
normal transfer

network DATA
    |
    +--> disk
    |
    `--> BLAKE3

one streaming pass
```

versus:

```text
rare reconciliation

completion receipt
      |
      v
physical completed file
      |
      v
full BLAKE3 rehash
      |
      v
VERIFIED
```

The extra read is intentional because a receipt must not be trusted without checking the actual stored bytes.

Future optimization may improve this without weakening verification guarantees.

---

## 27. Persistence and durability model

WarpFile now has two different receiver-side persistent recovery mechanisms.

### Partial state

```text
.warpfile/partials/<filename>.part
.warpfile/partials/<filename>.part.warpmeta
```

Purpose:

```text
resume interrupted transfer
```

### Completion state

```text
.warpfile/receipts/<transfer-id>.json
```

Purpose:

```text
reconcile an already verified logical transfer
```

The reference implementation uses temporary files, flushing, synchronization and rename operations when writing metadata.

This improves resilience against process interruption and ordinary network loss.

It must not be interpreted as an absolute durability guarantee against all sudden machine power-loss or filesystem-failure scenarios.

In particular, file-data synchronization and directory-entry persistence are separate concerns whose guarantees vary by platform/filesystem.

---

## 28. Testing strategy

WarpFile uses both unit and end-to-end tests.

The currently validated suite contains:

```text
304 unit tests
5 CLI tests in main.rs
32 end-to-end tests
-------------------
341 tests total
```

All 341 were passing when this architecture state was documented.

Unit tests cover components such as:

- frame encoding;
- frame decoding;
- payload validation;
- offer parsing;
- Transfer ID representation and generation;
- rejection parsing;
- resume payload encoding and decoding;
- completion receipt encoding and persistence;
- transfer metadata encoding and persistence;
- discovery announcements;
- interface broadcast calculations;
- Tailscale JSON parsing;
- destination-name resolution;
- progress formatting;
- sender retry classification.

End-to-end coverage includes:

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
- real sender and receiver resume flow;
- automatic sender reconnect after connection loss;
- verified resume inside the automatically created retry session;
- maximum retry-attempt enforcement;
- permanent receiver REJECT without retry;
- Transfer ID preservation across automatic reconnect;
- persistent partial transfer metadata;
- adoption of a cryptographically verified partial by a new Transfer ID;
- metadata replacement after RESTART;
- metadata cleanup after successful transfer;
- persistent completion receipts;
- reconciliation from an existing final file;
- reconciliation from a completed `.part`;
- refusal to send VERIFIED when the physical file disagrees with its receipt;
- retry after lost VERIFIED;
- direct VERIFIED after OFFER during reconciliation.

---

## 29. Resume recovery test model

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

---

## 30. Completion reconciliation test model

The finalization retry test models a different failure boundary.

First session:

```text
sender
  |
  v
OFFER
  |
  v
ACCEPT
  |
  v
DATA
  |
  v
COMPLETE
  |
  v
receiver reaches completed state
  |
  X
VERIFIED confirmation is lost
```

The sender does not create a new logical transfer.

It starts another TCP session using the same Transfer ID:

```text
second connection
      |
      v
HELLO / HELLO_ACK
      |
      v
OFFER with same Transfer ID
      |
      v
VERIFIED directly
      |
      v
sender success
```

The test verifies that:

- a retry actually occurs;
- the same Transfer ID is preserved;
- direct VERIFIED is accepted after OFFER;
- no DATA retransmission is required;
- no unnecessary third session is opened.

This test closes the failure window that the old `AmbiguousCompletion` behavior intentionally left unresolved.

---

## 31. Async runtime

WarpFile uses Tokio.

Tokio currently provides:

- asynchronous TCP;
- asynchronous UDP;
- asynchronous file I/O;
- signal handling;
- async scheduling.

Concurrency is introduced only where required.

The current receiver handles sequential file-transfer sessions rather than simultaneous multi-client transfers.

Correctness currently takes priority over maximum throughput.

---

## 32. Platform independence

Core transfer and WFP logic should avoid unnecessary operating-system-specific behavior.

Current real multi-machine development has focused on Windows.

Linux remains a target platform.

Platform-specific connectivity and future performance optimizations should remain isolated behind dedicated abstractions.

Examples include:

- Tailscale integration;
- future Linux `sendfile`/related paths;
- future Windows `TransmitFile`/related paths.

WFP itself should not depend on a specific operating-system syscall.

---

## 33. Security boundaries

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
- Transfer ID representation;
- resume offsets;
- resume prefix hashes;
- actual received byte count;
- final BLAKE3 integrity;
- completion receipt identity fields;
- physical completed-file integrity before reconciliation.

Sender-provided filesystem paths are never accepted as destination paths.

WFP/0.2 currently does not provide:

- encryption;
- authenticated peer identity;
- authenticated device identity;
- authenticated Transfer IDs.

Resume hashes prove content equality for a prefix.

Completion hashes prove equality with expected completed content.

Neither mechanism proves peer identity.

Transfer IDs correlate protocol sessions.

They are not authentication credentials.

Local `.warpmeta` files and completion receipts must also be treated as untrusted if an attacker can modify the receiver filesystem.

The receiver therefore verifies actual file bytes before using a receipt to return direct VERIFIED.

Future cryptographic work must use established cryptographic primitives rather than custom algorithms.

---

## 34. Route selection

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

The long-term path-selection order is expected to prefer direct connectivity before fallback paths:

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

The selection mechanism should be based on measured or meaningful connectivity information rather than hard-coded assumptions.

---

## 35. Current reliability limitations

Resume, persistent partial identity and completion reconciliation are functional.

The current implementation intentionally remains simpler than the long-term design.

Current limitations include:

- no persistent sender-side transfer database;
- Transfer ID survives automatic retries but not sender-process restart;
- no BLAKE3 checkpoint cache;
- retained prefixes are reread to rebuild hash state;
- reconciliation rehashes completed physical files;
- one file per TCP transfer session;
- no directory-transfer manifest;
- WFP/0.2 resume is prefix-only; WFP/0.3 provides a sparse verified-chunk inventory.
- no partial content deduplication;
- no receipt garbage-collection policy;
- sequential rather than concurrent receiver sessions;
- no protocol STATUS query;
- no authenticated session;
- no absolute cross-platform guarantee for sudden-power-loss durability.

A sender-process restart creates a new Transfer ID.

Partial bytes can still be adopted through BLAKE3 prefix verification.

A completion receipt created under the previous Transfer ID cannot currently be rediscovered by a new sender process that no longer knows that ID.

That is a separate future problem from in-process automatic retry.

---

## 36. Future reliability and transfer-efficiency work

Persistent transfer metadata, Transfer ID and completion reconciliation are already implemented and are no longer future milestones.

The next reliability/performance layers may include:

```text
current verified prefix resume
        |
        v
BLAKE3 checkpoints
        |
        v
chunk-addressable transfer state
        |
        v
non-contiguous missing chunks
        |
        v
partial deduplication
        |
        v
multi-source/private swarm
```

Chunk-addressable transfer state is delivered in WFP/0.3 via `--wfp-version=0.3`; the progression above is kept as historical record.

Potential work includes:

- persistent sender job identity where useful;
- receipt lifecycle and garbage collection;
- hash checkpoints;
- adaptive chunk sizing;
- transfer pipelining;
- selective compression;
- batch transfer for many small files;
- platform-specific zero-copy paths behind portable abstractions.

Chunk hashes and chunk manifests are delivered in WFP/0.3 via `--wfp-version=0.3`.

The central efficiency principle remains:

```text
Never transfer a byte that does not need to be transferred.
```

Correctness and reproducible measurement come before claims of performance.

---

## 37. Directory and multi-file transfers

Directory transfers will require additional design because a directory is a collection of multiple logical file states rather than one byte stream.

A future design may require:

- a manifest;
- multiple content identities;
- per-file completion state;
- partial directory recovery;
- batching to avoid OFFER / ACCEPT round-trip overhead for every tiny file.

The current architecture deliberately keeps one file per TCP transfer session while the core transfer semantics are still being stabilized.

---

## 38. Future WorldLink integration

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
        |
        v
WorldLink
```

Potential functionality that may eventually be extracted includes:

- peer identity;
- discovery;
- session establishment;
- capability negotiation;
- authenticated encryption;
- multiplexing;
- rendezvous;
- NAT traversal;
- relay connectivity.

File-transfer-specific semantics remain WarpFile responsibilities.

Examples that are likely to remain WarpFile-specific include:

- OFFER;
- RESUME;
- RESTART;
- DATA;
- COMPLETE;
- transfer receipts;
- file completion reconciliation.

WorldLink should absorb abstractions only after they have appeared as real reusable requirements.

---

## 39. Performance philosophy

WarpFile should not be described as faster or more efficient than mature alternatives without reproducible evidence.

Performance work should measure at least:

- throughput;
- CPU usage;
- memory usage;
- protocol overhead;
- retransmitted bytes;
- recovery cost after failure.

Optimization should progress approximately as:

```text
correct streaming
      |
      v
verified recovery
      |
      v
chunk-aware recovery
      |
      v
pipeline / adaptive chunks
      |
      v
platform-specific zero-copy
      |
      v
batching / selective compression
      |
      v
dedup / topology-aware distribution
```

Chunk-aware recovery is delivered in WFP/0.3 via `--wfp-version=0.3`; the progression above is kept as historical record.

Any platform-specific optimization must remain behind an explicit abstraction so that the core protocol remains portable.

---

## 40. Engineering principle

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

Transfer identity is an example of this process.

It was not added merely because identifiers are common in protocols.

It became necessary when real retry behavior exposed a concrete question:

```text
If VERIFIED is lost,
how can another TCP session know that
the previous logical transfer already completed?
```

WFP/0.2, persistent completion receipts and reconciliation are the current answer to that requirement.
