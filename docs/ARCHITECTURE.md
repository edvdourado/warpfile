# WarpFile Architecture

**Status:** Experimental  
**Reference release:** `0.1.0-alpha.1`

This document describes the current architecture of the WarpFile reference implementation.

## 1. Architecture goals

WarpFile should remain:

- cross-platform;
- protocol-driven;
- modular;
- streaming-oriented;
- memory-efficient;
- testable;
- transport-extensible.

A central design principle is separation of responsibilities.

File-transfer code should not need to know how a peer address was discovered.

Discovery code should not implement file-transfer semantics.

Protocol encoding should remain separate from CLI behavior.

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

## 3. Current source layout

```text
src/
├── main.rs
├── lib.rs
├── destination.rs
├── discovery.rs
├── progress.rs
├── receiver.rs
├── sender.rs
├── tailscale.rs
└── protocol/
    ├── mod.rs
    ├── frame.rs
    ├── message.rs
    ├── encoder.rs
    ├── decoder.rs
    ├── io.rs
    ├── offer.rs
    ├── reject.rs
    └── discovery.rs

tests/
├── discovery_e2e.rs
├── receiver_persistent_e2e.rs
└── transfer_e2e.rs
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

Those addresses are treated only as **candidate discovery targets**.

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

`sender.rs` controls one outgoing file-transfer session.

Responsibilities include:

- validate the source file;
- connect to the destination;
- perform HELLO / HELLO_ACK negotiation;
- send OFFER;
- handle ACCEPT or structured REJECT;
- stream file data;
- update BLAKE3 incrementally;
- update progress reporting;
- send CANCEL after user interruption;
- send COMPLETE;
- wait for VERIFIED.

The sender does not need to know whether the destination came from:

- an explicit IP address;
- LAN discovery;
- Tailscale-assisted discovery;
- a future connectivity provider.

This separation is important for future automatic path selection.

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
- create `.part` files;
- receive DATA frames;
- update BLAKE3 incrementally;
- verify size and hash;
- atomically promote the partial file after validation;
- clean partial files after current failure cases;
- return to the accept loop after a client error.

A failed client transfer therefore does not terminate the whole receiver process.

## 10. Discovery and receiver concurrency

A running receiver serves two independent functions:

```text
TCP 42069
file-transfer listener

UDP 42070
discovery responder
```

The receiver orchestration keeps both active.

This allows a receiver to remain discoverable while waiting for or processing sequential file transfers.

## 11. `progress.rs`

`progress.rs` provides transfer progress output.

It tracks:

- transferred bytes;
- total bytes;
- percentage;
- average throughput;
- estimated remaining time.

Progress rendering is throttled rather than redrawn on every transferred chunk.

It also ensures an interrupted progress line is terminated cleanly before error text is printed.

## 12. Protocol module

The `protocol` directory contains WFP-specific encoding and validation.

```text
protocol/
├── frame.rs
├── message.rs
├── encoder.rs
├── decoder.rs
├── io.rs
├── offer.rs
├── reject.rs
└── discovery.rs
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

### `discovery.rs`

Encodes and decodes ANNOUNCE payloads.

The announcement contains:

```text
device name
TCP transfer port
```

DISCOVER itself uses an empty payload.

## 13. Streaming model

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

Memory usage should therefore remain roughly bounded regardless of total file size.

A very large file should not require memory proportional to its total size.

## 14. Integrity model

BLAKE3 is updated during streaming.

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

The receiver sends VERIFIED only when its independently calculated digest matches.

## 15. Partial files

Incoming files use a temporary path:

```text
filename.part
```

The final destination name does not appear until verification succeeds.

Current behavior:

```text
receive DATA
    |
    v
filename.part
    |
    +-- failure ------> remove
    |
    +-- verified -----> rename to filename
```

Resume support will intentionally change part of this model by retaining validated partial data across recoverable interruptions.

## 16. Cancellation

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

The implementation avoids interrupting a DATA frame halfway through writing it to the TCP stream.

The receiver handles CANCEL as an explicit interrupted transfer and cleans the current partial file.

## 17. Testing strategy

WarpFile uses both unit and end-to-end tests.

Unit tests cover components such as:

- frame encoding;
- frame decoding;
- payload validation;
- offer parsing;
- rejection parsing;
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
- sender disconnects;
- corrupted hashes;
- explicit cancellation;
- UDP discovery;
- multiple sequential transfers through a persistent receiver.

This is intentional: protocol code is tested both as isolated logic and as actual asynchronous network behavior.

## 18. Async runtime

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

## 19. Platform independence

Core transfer and WFP logic should avoid unnecessary operating-system-specific behavior.

Current real multi-machine development has focused on Windows.

Linux remains a target platform.

Platform-specific connectivity integrations should remain isolated behind dedicated modules or providers.

Tailscale integration is one example.

## 20. Security boundaries

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
- actual received byte count;
- BLAKE3 integrity.

Sender-provided filesystem paths are never accepted as destination paths.

WFP/0.1 currently does not provide encryption or authenticated peer identity.

## 21. Route selection

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

## 22. Future resume architecture

The next major reliability milestone is resumable transfer.

Current behavior:

```text
disconnect
    |
    v
delete .part
    |
    v
restart from byte 0
```

Future target behavior:

```text
disconnect
    |
    v
retain validated partial state
    |
    v
reconnect
    |
    v
negotiate safe offset
    |
    v
continue missing data
```

This will require coordinated changes to:

- WFP messages;
- partial-file persistence;
- sender file seeking;
- hash state strategy;
- safe chunk boundaries;
- failure classification.

It should be implemented as an explicit protocol feature rather than as an implicit filesystem trick.

## 23. Future WorldLink integration

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

## 24. Engineering principle

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