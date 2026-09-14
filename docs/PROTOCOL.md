# WarpFile Protocol ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â WFP/0.1

**Status:** Experimental
**Protocol version:** 0.1
**Reference implementation:** WarpFile development branch after `0.1.0-alpha.1`

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
- verified resumable transfers;
- avoiding retransmission of already validated file data;
- implementation portability;
- clear separation between discovery and file transfer.

WFP/0.1 currently does **not** provide:

- encryption;
- peer authentication;
- identity verification;
- NAT traversal;
- relay transport;
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
0x13     RESUME
0x14     RESTART

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

A fresh transfer normally follows:

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

If the receiver has retained usable partial data, the negotiation may instead follow:

```text
Sender                                      Receiver
  |                                            |
  | ---------------- HELLO -----------------> |
  | <------------- HELLO_ACK ---------------- |
  |                                            |
  | ---------------- OFFER -----------------> |
  | <--------------- RESUME ----------------- |
  |          offset + prefix hash              |
  |                                            |
  | validates local prefix                     |
  |                                            |
  | ---------------- ACCEPT ----------------> |
  |                                            |
  | -------- DATA from resume offset --------> |
  |                   ...                      |
  |                                            |
  | -------------- COMPLETE ----------------> |
  | <-------------- VERIFIED ---------------- |
```

If the sender determines that the retained receiver prefix does not match the source file:

```text
Sender                                      Receiver
  |                                            |
  | ---------------- OFFER -----------------> |
  | <--------------- RESUME ----------------- |
  |                                            |
  | --------------- RESTART ----------------> |
  |                                            |
  |                      discard stale .part   |
  |                                            |
  | <--------------- ACCEPT ----------------- |
  |                                            |
  | ------------ DATA from byte 0 ----------> |
  |                   ...                      |
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

ACCEPT is used in two negotiation states.

After `OFFER`, a receiver may send ACCEPT to indicate that a fresh destination has been prepared and the sender should begin at byte zero.

After `RESUME`, the sender sends ACCEPT when:

- the requested offset is valid for the source file;
- the sender has hashed its local prefix up to that offset;
- the sender's prefix digest matches the digest supplied by the receiver.

In that state ACCEPT means that the sender agrees to continue from the proposed resume offset.

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

## 11. RESUME

Message type:

```text
0x13
```

Payload:

```text
Offset    Size    Field
------    ----    ----------------
0         8       Resume offset
8         32      BLAKE3 prefix digest
```

Total payload size:

```text
40 bytes
```

Resume offset:

```text
u64
```

The offset represents the exact number of file bytes already retained by the receiver.

It is a byte offset, not a DATA-frame or chunk number.

This allows resume points to remain independent from DATA frame sizing and future chunk-management changes.

The BLAKE3 prefix digest represents exactly:

```text
file bytes [0, offset)
```

Before sending RESUME, the receiver hashes the retained `.part` file.

The sender then reads and hashes exactly the same prefix from the source file.

If the hashes match, the sender sends ACCEPT and continues from that offset.

If they do not match, the sender sends RESTART.

A valid resume offset may equal the total file size.

In that case, if the prefix digest matches the complete source file, no DATA frames need to be retransmitted. The sender may proceed directly to COMPLETE.

## 12. RESTART

Message type:

```text
0x14
```

Payload:

```text
empty
```

RESTART is sent by the sender after RESUME when the proposed partial state cannot safely be used.

Typical reasons include:

- the resume offset exceeds the source file size;
- the sender's local prefix digest differs from the receiver's prefix digest.

After RESTART, the receiver discards the stale partial state, prepares a fresh `.part` file and sends ACCEPT.

The sender then starts DATA transmission from byte zero.

RESTART means:

```text
discard this resume state and restart the same transfer
```

It is distinct from CANCEL, which aborts the transfer.

## 13. DATA

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

For fresh transfers, DATA begins at byte zero.

For accepted resumed transfers, DATA begins at the negotiated resume offset.

Both peers update their BLAKE3 state while processing file data.

## 14. COMPLETE

Message type:

```text
0x30
```

Payload:

```text
32-byte BLAKE3 digest
```

The sender sends COMPLETE after all file bytes have been processed.

The digest represents the complete original file, including any prefix that was not retransmitted during a resumed transfer.

## 15. VERIFIED

Message type:

```text
0x31
```

Payload:

```text
empty
```

The receiver sends VERIFIED only after:

- the total retained plus newly received byte count equals the file size announced in OFFER;
- the locally calculated BLAKE3 digest matches the digest received in COMPLETE.

Only after receiving VERIFIED may the sender consider the transfer successful.

## 16. CANCEL

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

CANCEL therefore does not request later resume.

## 17. Partial-file behavior

Incoming data is written to:

```text
<filename>.part
```

The `.part` file is renamed to the final filename only after successful size and BLAKE3 verification.

The current reference behavior distinguishes recoverable connection loss from invalid or intentionally aborted transfer state.

Unexpected recoverable connection loss:

```text
connection lost
      |
      v
retain <filename>.part
      |
      v
offer verified resume on next matching transfer
```

Explicit cancellation:

```text
CANCEL
  |
  v
remove .part
```

Integrity or protocol failure:

```text
invalid state
     |
     v
remove .part
```

When a new OFFER arrives and an existing partial file is usable:

1. the receiver reads its current length;
2. the receiver hashes the retained bytes;
3. the receiver sends RESUME with the byte offset and BLAKE3 prefix digest.

A zero-length `.part` provides no useful resume state and is discarded before starting a fresh transfer.

A `.part` larger than the file size announced in OFFER is impossible as a valid prefix and is discarded before starting a fresh transfer.

## 18. BLAKE3 integrity and resume

Fresh transfers hash the file while streaming.

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

A resumed transfer additionally rebuilds BLAKE3 state from the retained prefix.

Receiver:

```text
existing .part
      |
      v
read prefix
      |
      +------> BLAKE3 state
      |
      +------> RESUME prefix digest
```

Sender:

```text
source prefix
      |
      v
read locally
      |
      +------> BLAKE3 state
      |
      +------> compare with RESUME digest
```

If the prefix digests match, both peers retain their BLAKE3 state and continue feeding only the remaining bytes into the same logical file digest.

Conceptually:

```text
BLAKE3(prefix)
      |
      v
continue with suffix
      |
      v
BLAKE3(complete file)
```

The already validated prefix is read locally to rebuild hash state but is not retransmitted over the network.

The current implementation therefore avoids a second complete-file pre-hash before starting a normal transfer while still supporting verified resume.

Future optimizations may persist hash checkpoints so very large retained prefixes do not need to be fully re-read on each resume attempt.

## 19. Resume safety properties

WFP/0.1 does not trust a partial file based only on:

- filename;
- announced file size;
- partial length.

Two different files may have the same filename and size.

Resume therefore verifies the exact retained prefix using BLAKE3 before accepting continuation.

For a proposed offset `X`:

```text
Receiver:
BLAKE3(receiver .part bytes 0..X)

Sender:
BLAKE3(source bytes 0..X)
```

Continuation is accepted only when those digests match.

A mismatch causes RESTART rather than blind continuation.

The final COMPLETE / VERIFIED exchange still verifies the complete resulting file.

Resume verification provides integrity of the retained prefix, but it does not provide peer authentication or protection against a malicious network peer.

## 20. Discovery model

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

## 21. DISCOVER

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

## 22. ANNOUNCE

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

## 23. Discovery providers

The WFP discovery messages are independent from the mechanism used to find candidate IP addresses.

The current reference implementation uses two candidate providers.

### 23.1 Local network discovery

WarpFile enumerates local IPv4 interfaces and calculates directed broadcast addresses from each address and subnet mask.

It sends WFP DISCOVER to those network broadcasts.

Loopback, unspecified, link-local and `/32` addresses are not used as broadcast targets.

Responses originating from the local machine are filtered to avoid self-discovery.

### 23.2 Tailscale-assisted discovery

When the `tailscale` CLI is available, WarpFile reads online Tailscale peers and treats their IPv4 addresses as discovery candidates.

WarpFile does **not** assume that a Tailscale peer is a WarpFile peer.

It sends a WFP DISCOVER datagram directly to each candidate.

Only a peer that responds with a valid WFP ANNOUNCE becomes a discovered WarpFile device.

Tailscale is optional. WarpFile continues to operate without it.

## 24. Device-name resolution

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

## 25. Default ports

Current development defaults:

```text
TCP transfer: 42069
UDP discovery: 42070
```

These ports are implementation defaults and are not permanently reserved protocol assignments.

They may change before stable release.

## 26. Security

WFP/0.1 currently provides:

- no encryption;
- no peer authentication;
- no device identity verification.

Network input must be treated as untrusted.

The current version should only be used in trusted development environments or over a trusted network layer.

Resume prefix hashes are integrity checks, not authentication.

Future security work will use established cryptographic algorithms and libraries.

WarpFile will not invent custom cryptographic primitives.

## 27. Current resume limitations

The current verified resume design intentionally remains simple.

Current limitations include:

- no automatic reconnect loop in the sender;
- no persistent sender-side transfer database;
- no persisted BLAKE3 hash checkpoints;
- retained prefixes must currently be re-read locally to rebuild BLAKE3 state;
- one file is still transferred per TCP connection;
- resume state is based on the receiver's `.part` file rather than a multi-file transfer manifest.

These limitations affect performance and orchestration, not the integrity requirement for an accepted resume offset.

## 28. Compatibility

WFP/0.1 is experimental.

The addition and behavior of RESUME and RESTART are part of the current experimental WFP/0.1 development state.

No backward compatibility guarantee exists before a stable protocol release.
