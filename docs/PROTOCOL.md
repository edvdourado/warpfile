# WarpFile Protocol — WFP/0.1

**Status:** Experimental  
**Protocol version:** 0.1  
**Reference implementation:** WarpFile `0.1.0-alpha.1`

WFP is the application-layer protocol used by WarpFile.

WFP/0.1 currently covers two related operations:

1. file-transfer sessions over TCP;
2. WarpFile peer discovery over UDP.

The protocol is intentionally small and experimental. Compatibility is not guaranteed before a stable WarpFile release.

## 1. Design goals

WFP/0.1 prioritizes:

- explicit binary framing;
- deterministic parsing;
- streaming without loading entire files into memory;
- bounded payload sizes;
- integrity verification;
- implementation portability;
- clear separation between discovery and file transfer.

WFP/0.1 currently does **not** provide:

- encryption;
- peer authentication;
- identity verification;
- NAT traversal;
- relay transport;
- resumable transfers;
- multiplexing;
- compression;
- multiple files in one TCP session.

## 2. Byte order

All multi-byte integer fields use **big-endian** byte order, also known as network byte order.

## 3. Frame format

Every WFP message uses the same frame format.

The fixed header is 12 bytes.

```text
Offset    Size    Field
------    ----    ----------------
0         4       Magic
4         1       Version
5         1       Message Type
6         2       Flags
8         4       Payload Length
12        N       Payload
```

Visual representation:

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

### 3.1 Magic

The magic bytes are:

```text
57 46 50 00
```

ASCII representation:

```text
W F P \0
```

Frames with an invalid magic value must be rejected.

### 3.2 Version

WFP/0.1 uses:

```text
0x01
```

### 3.3 Flags

The flags field is currently reserved.

WFP/0.1 uses:

```text
0x0000
```

### 3.4 Payload length

Payload Length is an unsigned 32-bit integer.

The general maximum WFP frame payload is:

```text
1 MiB
```

DATA frames have a stricter maximum:

```text
64 KiB
65536 bytes
```

Implementations must validate payload lengths before allocating or accepting data.

## 4. Message types

```text
Value    Name
-----    ---------
0x01     HELLO
0x02     HELLO_ACK

0x10     OFFER
0x11     ACCEPT
0x12     REJECT

0x20     DATA

0x30     COMPLETE
0x31     VERIFIED

0x40     CANCEL

0x50     DISCOVER
0x51     ANNOUNCE

0xFF     ERROR
```

Unknown message types must be rejected.

`ERROR` is currently recognized as a message type, but WFP/0.1 does not yet define or implement a stable structured ERROR payload.

## 5. TCP file-transfer model

A single TCP connection transfers one file.

The receiver process may remain running and accept multiple sequential connections, but each TCP session carries one transfer.

Normal lifecycle:

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
  |                   close                    |
```

The receiver may send `REJECT` after `OFFER`.

The sender may send `CANCEL` before transfer completion.

## 6. HELLO

Message type:

```text
0x01
```

Payload:

```text
Offset    Size    Field
------    ----    ----------------
0         1       Protocol version
```

For WFP/0.1:

```text
01
```

The sender uses HELLO to announce the protocol version it intends to use.

## 7. HELLO_ACK

Message type:

```text
0x02
```

Payload:

```text
Offset    Size    Field
------    ----    ----------------
0         1       Selected version
```

For WFP/0.1:

```text
01
```

The current reference implementation expects the peer to use the same WFP version.

## 8. OFFER

Message type:

```text
0x10
```

Payload:

```text
Offset    Size    Field
------    ----    ----------------
0         2       Filename length
2         N       Filename
2 + N     8       File size
```

Filename length:

```text
u16
```

Filename encoding:

```text
UTF-8
```

File size:

```text
u64
```

Only the basename of the file may be offered.

Valid:

```text
photo.png
```

Invalid:

```text
C:\Users\User\photo.png
../../photo.png
/home/user/photo.png
```

Incoming filenames are untrusted input.

The receiver must not honor absolute paths, directory separators or traversal attempts supplied by the sender.

## 9. ACCEPT

Message type:

```text
0x11
```

Payload:

```text
empty
```

ACCEPT indicates that the receiver has prepared the destination and is ready for DATA frames.

## 10. REJECT

Message type:

```text
0x12
```

Payload:

```text
Offset    Size    Field
------    ----    ----------------
0         2       Reason code
2         2       Message length
4         N       UTF-8 message
```

Current reason codes:

```text
0x0001    FILE_EXISTS
0x0002    UNSAFE_FILENAME
0x0003    CANNOT_PREPARE_DESTINATION
```

The message is diagnostic text.

Protocol logic should rely on the numeric reason code rather than parsing the human-readable message.

## 11. DATA

Message type:

```text
0x20
```

Payload:

```text
raw file bytes
```

Maximum payload:

```text
65536 bytes
```

WFP/0.1 does not include DATA sequence numbers because the current transfer transport is TCP, which already provides ordered reliable byte delivery.

The receiver writes DATA payloads sequentially.

Both peers update their BLAKE3 state while processing file data.

## 12. COMPLETE

Message type:

```text
0x30
```

Payload:

```text
32-byte BLAKE3 digest
```

The sender sends COMPLETE after all file bytes have been transmitted.

The digest represents the complete original file.

## 13. VERIFIED

Message type:

```text
0x31
```

Payload:

```text
empty
```

The receiver sends VERIFIED only after:

- the total received byte count equals the file size announced in OFFER;
- the locally calculated BLAKE3 digest matches the digest received in COMPLETE.

Only after receiving VERIFIED may the sender consider the transfer successful.

## 14. CANCEL

Message type:

```text
0x40
```

Payload:

```text
empty
```

CANCEL indicates an intentional sender-side cancellation.

The current sender sends CANCEL when the user interrupts an active transfer with `Ctrl+C`.

The current receiver removes the incomplete `.part` file after cancellation.

## 15. Partial-file behavior

Incoming data is written to:

```text
<filename>.part
```

The `.part` file is renamed to the final filename only after successful size and BLAKE3 verification.

Current WFP/0.1 behavior removes incomplete partial files after transfer failure or cancellation.

Persistent resumable partial files are planned for a future protocol extension.

## 16. BLAKE3 integrity

Hashing happens during streaming.

Sender:

```text
File
 |
 v
chunk --------> DATA
 |
 +------------> BLAKE3
```

Receiver:

```text
DATA
 |
 v
chunk --------> File
 |
 +------------> BLAKE3
```

This avoids performing a second full-file read solely for integrity verification.

## 17. Discovery model

WarpFile discovery uses WFP frames over UDP.

Default discovery port:

```text
42070/UDP
```

A discovering peer sends `DISCOVER`.

A WarpFile receiver responds with `ANNOUNCE`.

Discovery does not establish a transfer session by itself.

The ANNOUNCE response tells the discovering peer which TCP port is accepting file transfers.

Typical flow:

```text
Discoverer                                  Receiver
    |                                          |
    | ------------- DISCOVER ----------------> |
    | <------------ ANNOUNCE ----------------- |
    |                                          |
```

## 18. DISCOVER

Message type:

```text
0x50
```

Payload:

```text
empty
```

A valid DISCOVER frame asks another host whether a WarpFile receiver is available.

Malformed frames, non-DISCOVER frames and DISCOVER frames with non-empty payloads are ignored by the discovery responder.

## 19. ANNOUNCE

Message type:

```text
0x51
```

Payload:

```text
Offset        Size    Field
------        ----    ----------------
0             2       Device name length
2             N       Device name
2 + N         2       TCP transfer port
```

Device name length:

```text
u16
```

Device name encoding:

```text
UTF-8
```

TCP transfer port:

```text
u16
```

Port zero is invalid.

Example semantic announcement:

```text
Device name: EDBOOK
TCP port:   42069
```

The source IP address of the UDP ANNOUNCE packet combined with the announced TCP port produces the transfer address.

Example:

```text
100.68.8.15 + 42069
        |
        v
100.68.8.15:42069
```

## 20. Discovery providers

The WFP discovery messages are independent from the mechanism used to find candidate IP addresses.

The current reference implementation uses two candidate providers.

### 20.1 Local network discovery

WarpFile enumerates local IPv4 interfaces and calculates directed broadcast addresses from each address and subnet mask.

It sends WFP DISCOVER to those network broadcasts.

Loopback, unspecified, link-local and `/32` addresses are not used as broadcast targets.

Responses originating from the local machine are filtered to avoid self-discovery.

### 20.2 Tailscale-assisted discovery

When the `tailscale` CLI is available, WarpFile reads online Tailscale peers and treats their IPv4 addresses as discovery candidates.

WarpFile does **not** assume that a Tailscale peer is a WarpFile peer.

It sends a WFP DISCOVER datagram directly to each candidate.

Only a peer that responds with a valid WFP ANNOUNCE becomes a discovered WarpFile device.

Tailscale is optional. WarpFile continues to operate without it.

## 21. Device-name resolution

The CLI may use a discovered device name instead of an explicit socket address.

Example:

```text
warpfile send README.md EDBOOK
```

Discovery may resolve:

```text
EDBOOK -> 100.68.8.15:42069
```

Matching is case-insensitive.

If no matching device exists, resolution fails.

If multiple discovered devices have the same name, the current implementation refuses to choose silently and requires the user to provide an explicit address.

Automatic route selection is planned for future work.

## 22. Default ports

Current development defaults:

```text
TCP transfer: 42069
UDP discovery: 42070
```

These ports are implementation defaults and are not permanently reserved protocol assignments.

They may change before stable release.

## 23. Security

WFP/0.1 currently provides:

- no encryption;
- no peer authentication;
- no device identity verification.

Network input must be treated as untrusted.

The current version should only be used in trusted development environments or over a trusted network layer.

Future security work will use established cryptographic algorithms and libraries.

WarpFile will not invent custom cryptographic primitives.

## 24. Resume status

WFP/0.1 does **not** currently support resumable transfers.

If a transfer is interrupted, the current receiver removes the incomplete `.part` file.

A future reliability milestone will introduce:

- retained partial-file state;
- validation of resumable partial data;
- safe resume offsets;
- sender seeking;
- continued integrity verification.

## 25. Compatibility

WFP/0.1 is experimental.

No backward compatibility guarantee exists before a stable protocol release.