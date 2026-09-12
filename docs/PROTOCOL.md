WarpFile Protocol — WFP/0.1

Status: Experimental
Version: 0.1

WFP is the application-layer protocol used by WarpFile.

WFP/0.1 is intentionally small. Its purpose is to establish the minimum protocol necessary to transfer one file reliably between two peers over an ordered and reliable byte stream.

The initial transport is TCP.

1. Design goals

WFP/0.1 prioritizes:

simplicity;

explicit framing;

deterministic parsing;

streaming without loading entire files into memory;

integrity verification;

implementation portability.

WFP/0.1 does not attempt to provide:

encryption;

authentication;

NAT traversal;

peer discovery;

resumable transfers;

multiplexing;

compression;

multiple simultaneous files.

These features belong to future versions.

2. Byte order

All multi-byte integer values use big-endian byte order, also known as network byte order.

3. Connection model

A WFP/0.1 session transfers exactly one file.

The sender initiates the TCP connection. The receiver listens for incoming connections.

Normal lifecycle:

SENDER                                      RECEIVER
   │                                            │
   │ ───────────── HELLO ─────────────────────> │
   │ <────────── HELLO_ACK ──────────────────── │
   │                                            │
   │ ───────────── OFFER ─────────────────────> │
   │ <──────────── ACCEPT ───────────────────── │
   │                                            │
   │ ───────────── DATA ──────────────────────> │
   │ ───────────── DATA ──────────────────────> │
   │ ───────────── DATA ──────────────────────> │
   │                  ...                       │
   │                                            │
   │ ──────────── COMPLETE ───────────────────> │
   │ <─────────── VERIFIED ──────────────────── │
   │                                            │
   │                  close                     │

Either peer may send ERROR when a protocol failure occurs.

The sender may send CANCEL before the transfer completes.

4. Frame format

Every WFP message is encoded as a frame.

The fixed header is 12 bytes.

Offset    Size    Field
------    ----    ----------------
0         4       Magic
4         1       Version
5         1       Message Type
6         2       Flags
8         4       Payload Length
12        N       Payload

Visual representation:

+----------------------+ 0
| Magic                | 4 bytes
+----------------------+
| Version              | 1 byte
+----------------------+
| Type                 | 1 byte
+----------------------+
| Flags                | 2 bytes
+----------------------+
| Payload Length       | 4 bytes
+----------------------+ 12
| Payload              | N bytes
+----------------------+

4.1 Magic

Magic bytes:

57 46 50 00

ASCII representation:

W F P \0

A frame with an invalid magic value must be rejected.

4.2 Version

WFP/0.1 uses:

0x01

4.3 Flags

The flags field is reserved for future protocol extensions.

For WFP/0.1:

flags = 0x0000

A receiver should reject unsupported non-zero flags.

4.4 Payload length

Unsigned 32-bit integer.

Represents the number of payload bytes immediately following the header.

Implementations must validate this value before allocating memory.

WFP/0.1 defines a general maximum frame payload of:

1 MiB

DATA frames have a more restrictive maximum:

64 KiB
65536 bytes

5. Message types

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

0xFF     ERROR

Unknown message types must not be interpreted as another known message.

In WFP/0.1, receiving an unsupported message type is a protocol error.

6. HELLO

Message type:

0x01

Payload:

Offset    Size    Field
------    ----    ------------------
0         1       Protocol version

For WFP/0.1:

01

Purpose: the sender announces the WFP version it intends to use.

7. HELLO_ACK

Message type:

0x02

Payload:

Offset    Size    Field
------    ----    ------------------
0         1       Selected version

For WFP/0.1:

01

If the receiver cannot support the requested version, it should send ERROR and close the connection.

8. OFFER

Message type:

0x10

Payload:

Offset    Size        Field
------    ----------  ----------------
0         2           Filename length
2         N           Filename
2 + N     8           File size

Filename length:

u16

Filename encoding:

UTF-8

File size:

u64

The filename must contain only the file name itself.

Valid example:

photo.png

Invalid examples:

C:\Users\User\photo.png
../../photo.png
/home/user/photo.png

Receivers must treat incoming filenames as untrusted input.

Absolute paths and path traversal sequences must never be honored.

9. ACCEPT

Message type:

0x11

Payload: empty.

An ACCEPT indicates that the receiver is ready to receive file data.

10. REJECT

Message type:

0x12

Payload:

Offset    Size    Field
------    ----    ----------------
0         2       Reason code
2         2       Message length
4         N       Message

Message encoding: UTF-8.

Initial reason codes:

0x0001    User rejected transfer
0x0002    Invalid filename
0x0003    File too large
0x0004    Insufficient storage
0xFFFF    Unspecified reason

11. DATA

Message type:

0x20

Payload: raw file bytes.

Maximum payload:

65536 bytes

No sequence number is required in WFP/0.1 because TCP already guarantees:

ordered delivery;

reliable delivery;

duplicate suppression.

The receiver writes the payload sequentially to the output file.

Both peers update their BLAKE3 state while processing the file stream.

12. COMPLETE

Message type:

0x30

Payload:

32-byte BLAKE3 digest

The sender sends COMPLETE after all DATA frames have been transmitted.

The hash represents the complete original file.

13. VERIFIED

Message type:

0x31

Payload: empty.

The receiver sends VERIFIED only when:

the number of received file bytes equals the announced file size;

the locally calculated BLAKE3 digest matches the digest contained in COMPLETE.

After receiving VERIFIED, the sender may consider the transfer successful.

14. CANCEL

Message type:

0x40

Payload: empty in WFP/0.1.

Indicates that the sender intentionally aborted the transfer.

The receiver should remove incomplete output files unless explicitly configured otherwise.

15. ERROR

Message type:

0xFF

Payload:

Offset    Size    Field
------    ----    ----------------
0         2       Error code
2         2       Message length
4         N       Message

Initial error codes:

0x0001    Invalid frame
0x0002    Invalid magic
0x0003    Unsupported version
0x0004    Unsupported message
0x0005    Invalid state
0x0006    Invalid payload
0x0007    Integrity failure
0x0008    I/O failure
0xFFFF    Internal error

Error messages are UTF-8 and intended for diagnostic purposes.

Protocol logic must rely on the numeric error code rather than parsing the text.

16. Sender state machine

CONNECTED
    │
    ▼
HELLO_SENT
    │
    ▼
NEGOTIATED
    │
    ▼
OFFER_SENT
    │
    ▼
ACCEPTED
    │
    ▼
TRANSFERRING
    │
    ▼
COMPLETE_SENT
    │
    ▼
VERIFIED
    │
    ▼
DONE

Receiving a message that is invalid for the current state is a protocol error.

17. Receiver state machine

CONNECTED
    │
    ▼
NEGOTIATED
    │
    ▼
OFFER_RECEIVED
    │
    ▼
ACCEPTED
    │
    ▼
RECEIVING
    │
    ▼
VERIFYING
    │
    ▼
DONE

18. File integrity

WarpFile uses BLAKE3 for file integrity verification.

Hashing occurs while data is streamed.

Sender:

read chunk
    │
    ├── update BLAKE3
    │
    └── send DATA

Receiver:

receive DATA
    │
    ├── update BLAKE3
    │
    └── write file

At the end:

sender hash == receiver hash

must be true before the receiver sends VERIFIED.

19. Security

WFP/0.1 provides:

no encryption;

no peer authentication;

no identity verification.

Therefore WFP/0.1 must only be used on trusted networks during development.

Future versions will introduce authenticated and encrypted sessions.

Cryptographic primitives will use established cryptographic libraries and algorithms.

WarpFile will not design custom cryptographic algorithms.

20. Transport

The reference WFP/0.1 implementation uses TCP.

Default development port:

42069

The port is not considered permanently reserved by the protocol and may change before stable release.

21. Compatibility

WFP/0.1 is experimental.

No backward compatibility is guaranteed until the protocol reaches a stable release.