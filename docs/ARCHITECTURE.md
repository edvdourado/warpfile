WarpFile Architecture

Status: Experimental

This document describes the initial architecture of the WarpFile reference implementation.

1. Architecture goals

WarpFile should remain:

cross-platform;

modular;

protocol-driven;

streaming-oriented;

memory-efficient;

testable.

The protocol implementation must remain independent from the CLI and file transfer orchestration whenever possible.

2. Initial architecture

                       WarpFile
                          │
                ┌─────────┴─────────┐
                │                   │
             Sender             Receiver
                │                   │
                └─────────┬─────────┘
                          │
                    WFP Protocol
                          │
                       TCP I/O
                          │
                        Network

3. Proposed source layout

src/
├── main.rs
├── cli.rs
├── sender.rs
├── receiver.rs
└── protocol/
    ├── mod.rs
    ├── frame.rs
    ├── message.rs
    ├── encoder.rs
    └── decoder.rs

4. Responsibilities

main.rs

Application entry point.

Responsibilities:

initialize the CLI;

dispatch commands;

display fatal errors;

control process exit status.

It should contain minimal application logic.

cli.rs

Command-line interface definitions.

Initial command model:

warpfile send <FILE> --to <ADDRESS>
warpfile receive --port <PORT>

Future commands may include:

warpfile discover
warpfile devices
warpfile receive <TRANSFER_CODE>

sender.rs

Controls the sender-side transfer lifecycle.

Responsibilities:

connect to receiver;

perform WFP negotiation;

offer a file;

stream file contents;

calculate BLAKE3;

send completion information;

wait for verification;

expose transfer progress.

The sender should use protocol abstractions rather than manually constructing network bytes.

Conceptually:

sender
  │
  └── send(Message::Hello)

instead of:

sender
  │
  └── write([0x57, 0x46, 0x50, ...])

receiver.rs

Controls the receiver-side transfer lifecycle.

Responsibilities:

listen for incoming connections;

negotiate WFP;

validate file offers;

accept or reject transfers;

safely choose output paths;

stream incoming file contents to disk;

calculate BLAKE3;

verify integrity;

report final status.

Incoming filenames must always be considered untrusted.

5. Protocol module

The protocol module implements WFP independently of sender and receiver orchestration.

protocol/
├── frame
├── message
├── encoder
└── decoder

frame.rs

Represents the low-level WFP frame.

Conceptually:

struct Frame {
    version: u8,
    message_type: MessageType,
    flags: u16,
    payload: Vec<u8>,
}

The exact implementation may differ.

message.rs

Represents semantic protocol messages.

Examples:

Message::Hello
Message::HelloAck
Message::Offer
Message::Accept
Message::Reject
Message::Data
Message::Complete
Message::Verified
Message::Cancel
Message::Error

The rest of WarpFile should work primarily with these semantic messages.

encoder.rs

Converts semantic protocol structures into bytes suitable for network transmission.

Message
   │
   ▼
Frame
   │
   ▼
bytes

decoder.rs

Converts incoming network bytes into validated WFP frames and messages.

bytes
   │
   ▼
Frame
   │
   ▼
Message

The decoder must validate:

WFP magic;

protocol version;

message type;

flags;

payload size;

message-specific payload format.

Untrusted payload lengths must never cause uncontrolled memory allocations.

6. Streaming model

WarpFile must not load entire files into memory.

Example:

File
 │
 ├── 64 KiB
 │      ↓
 │    DATA
 │
 ├── 64 KiB
 │      ↓
 │    DATA
 │
 ├── 64 KiB
 │      ↓
 │    DATA
 │
 └── ...

Memory usage should remain roughly constant regardless of file size.

A 100 GiB file must not require significantly more transfer buffer memory than a 100 MiB file.

7. Async runtime

The initial implementation will use Tokio.

Tokio will provide:

asynchronous TCP networking;

asynchronous file operations where useful;

task scheduling;

future support for concurrent transfers.

Concurrency must not be added without a clear need.

The first implementation prioritizes correctness over maximum throughput.

8. Integrity

BLAKE3 will be updated incrementally while data is streamed.

Sender:

                       ┌──→ Network
File → chunk →─────────┤
                       └──→ BLAKE3

Receiver:

Network → chunk ───────┬──→ File
                       │
                       └──→ BLAKE3

This avoids an additional full-file read.

9. Platform independence

Core protocol and transfer logic should avoid operating-system-specific APIs.

Platform-specific functionality should eventually live behind explicit abstractions.

Initial target systems:

Windows
Linux

Future support may include:

macOS
Android

10. Error model

Internal implementation errors and protocol errors are different concepts.

Example:

std::io::Error

is an implementation-level error.

WFP ERROR 0x0008

is a protocol-level representation that may be sent to another peer.

These layers should not be conflated.

11. Security boundaries

Network input must always be treated as hostile.

The implementation must validate:

frame lengths;

filenames;

UTF-8 data;

state transitions;

protocol versions;

message types;

declared file sizes.

The receiver must never trust a sender-provided path.

For WFP/0.1, received files should be created only inside a receiver-controlled destination directory.

12. Future WorldLink integration

WarpFile is being developed before WorldLink intentionally.

The goal is to discover real shared networking requirements through a working application.

Potential functionality that may later move into WorldLink includes:

device identity;

peer discovery;

session establishment;

authenticated encryption;

NAT traversal;

connection multiplexing;

capability negotiation.

File transfer semantics remain WarpFile responsibilities.

This avoids designing WorldLink around hypothetical requirements.

13. Engineering principle

WarpFile should prefer:

working implementation
        ↓
observed reusable concept
        ↓
generalization

instead of:

hypothetical abstraction
        ↓
large framework
        ↓
hope that applications need it

WorldLink will be extracted from real requirements rather than invented in isolation.