# WarpFile

**WarpFile** is an experimental peer-to-peer file transfer application written in Rust.

It transfers files directly between devices using **WFP (WarpFile Protocol)**, its own binary application-layer protocol, without requiring WarpFile cloud storage or permanent user accounts.

The current prototype supports integrity-verified TCP transfers, persistent receiving, automatic device discovery across local networks and Tailscale, and sending files by device name.

> WarpFile is under active development and is not yet intended for untrusted networks.

## Current capabilities

WarpFile currently supports:

- WFP/0.1 binary framing over TCP;
- streaming transfers without loading the entire file into memory;
- 64 KiB DATA frames;
- incremental BLAKE3 integrity verification;
- file offer acceptance and structured rejection;
- transfer cancellation with `Ctrl+C`;
- cleanup of incomplete `.part` files;
- persistent receivers that can accept multiple sequential transfers;
- UDP peer discovery with WFP `DISCOVER` / `ANNOUNCE`;
- discovery across IPv4 local network interfaces;
- optional Tailscale-assisted peer discovery;
- device-name resolution;
- direct `IP:port` transfers as a fallback;
- unit and end-to-end tests covering protocol, discovery, cancellation, corruption, cleanup and real transfer flows.

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

A successful WFP/0.1 file transfer currently looks like this:

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

The receiver calculates the same digest while writing the incoming data and only sends `VERIFIED` when the received file size and BLAKE3 digest are both correct.

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
                 +-- message payloads
                 |
                 +-- TCP / UDP I/O
```

The transfer layer does not need to know whether an address came from LAN discovery, Tailscale discovery or explicit user input.

This separation is intentional so additional connectivity mechanisms can be introduced later without rewriting the file transfer protocol.

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

Incomplete files are written using a `.part` file and are not promoted to the final destination until integrity verification succeeds.

Transfers detect:

- unexpected disconnection;
- explicit cancellation;
- incorrect file size;
- corrupted data;
- invalid BLAKE3 digest;
- unsafe filenames;
- existing destination files;
- malformed WFP frames.

Resumable transfers are the next major reliability milestone.

## Security

**WFP/0.1 currently provides no encryption, authentication or peer identity verification.**

The current implementation should therefore only be used in trusted development environments or over a trusted network layer.

WarpFile will not design custom cryptographic algorithms. Future authenticated and encrypted sessions will use established cryptographic primitives and libraries.

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
- partial-file cleanup;
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

In progress / next.

- resumable transfers;
- retained and validated partial files;
- safe resume offsets;
- improved chunk management;
- retry and recovery behavior;
- directory transfer design;
- improved path selection.

### M4 — Distribution

Planned.

- verified Linux support;
- automated CI;
- release binaries;
- installation workflow;
- broader documentation;
- performance benchmarks.

## Performance philosophy

WarpFile is intended to become performance-oriented, but it will not claim to be faster than other tools without reproducible benchmarks.

Future optimization work includes:

- minimizing unnecessary copies;
- one-pass streaming and hashing;
- adaptive chunks;
- pipelining;
- selective compression;
- deduplication;
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