# WarpFile Protocol — WFP/0.2

**Status:** Experimental

**Protocol version:** 0.2

**Reference implementation:** WarpFile development after `v0.1.0-alpha.2`

WFP is the application-layer protocol used by WarpFile.

WFP/0.2 currently covers two related operations:

1. file-transfer sessions over TCP;
2. WarpFile peer discovery over UDP.

The protocol is intentionally small and experimental. Compatibility is not guaranteed before a stable WarpFile release.

WFP/0.2 extends the previous experimental WFP/0.1 design with persistent transfer identity and completion-reconciliation semantics.

---

## 1. Design goals

WFP/0.2 prioritizes:

- explicit binary framing;
- deterministic parsing;
- streaming without loading entire files into memory;
- bounded payload sizes;
- integrity verification;
- verified resumable transfers;
- avoiding retransmission of already validated file data;
- preserving one logical transfer identity across reconnect attempts;
- safely reconciling transfers whose final `VERIFIED` confirmation was lost;
- implementation portability;
- clear separation between discovery and file transfer.

WFP/0.2 currently does **not** provide:

- encryption;
- peer authentication;
- cryptographic device identity verification;
- NAT traversal;
- relay transport;
- multiplexing;
- compression;
- directory or multi-file transfer in one TCP session;
- persistent sender-side transfer identity across process restarts.

A WFP Transfer ID is a correlation identifier, not an authentication mechanism.

---

## 2. Byte order

All multi-byte integer fields use **big-endian** byte order, also known as network byte order.

---

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

WFP/0.2 uses:

```text
0x02
```

The frame version is independent from the WarpFile application package version.

For example, the reference implementation may still report package version:

```text
0.1.0-alpha.2
```

while using:

```text
WFP/0.2
```

### 3.3 Flags

The flags field is currently reserved.

WFP/0.2 uses:

```text
0x0000
```

Implementations must not assign undocumented meaning to these bits.

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

---

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

`ERROR` is currently recognized as a message type, but WFP/0.2 does not yet define or implement a stable structured ERROR payload.

---

## 5. TCP file-transfer model

A single TCP connection represents one WFP transfer session.

The receiver process may remain running and accept multiple sequential connections.

One logical file transfer may span more than one TCP session when automatic reconnection or resume occurs.

The Transfer ID carried by `OFFER` correlates those sessions.

### 5.1 Fresh transfer

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

### 5.2 Resumed transfer

If the receiver has retained usable partial data:

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

### 5.3 Restart after incompatible partial state

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
  |                      prepare fresh state   |
  |                                            |
  | <--------------- ACCEPT ----------------- |
  |                                            |
  | ------------ DATA from byte 0 ----------> |
  |                   ...                      |
```

### 5.4 Reconciliation after lost final confirmation

WFP/0.2 also permits `VERIFIED` as a direct response to `OFFER`.

This occurs when the receiver has persistent evidence that the same logical transfer was completed and can verify the physical stored file.

```text
Sender                                      Receiver
  |                                            |
  | ---------------- HELLO -----------------> |
  | <------------- HELLO_ACK ---------------- |
  |                                            |
  | ------ OFFER same Transfer ID ----------> |
  |                                            |
  |                  load completion receipt  |
  |                  verify stored file       |
  |                                            |
  | <-------------- VERIFIED ---------------- |
  |                                            |
  |                   close                    |
```

In this flow:

```text
OFFER -> VERIFIED
```

is a successful WFP/0.2 exchange.

The sender must not send DATA after a valid direct `VERIFIED`.

The receiver may send `REJECT` after `OFFER`.

The sender may send `CANCEL` before transfer completion.

---

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

For WFP/0.2:

```text
02
```

The sender uses HELLO to announce the protocol version it intends to use.

---

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

For WFP/0.2:

```text
02
```

The current reference implementation expects the peer to use the same WFP version.

---

## 8. OFFER

Message type:

```text
0x10
```

WFP/0.2 OFFER introduces a persistent logical transfer identifier.

Payload:

```text
Offset        Size    Field
------        ----    ----------------
0             16      Transfer ID
16            2       Filename length
18            N       Filename
18 + N        8       File size
```

Minimum payload size:

```text
26 bytes
```

excluding any filename bytes.

### 8.1 Transfer ID

Transfer ID size:

```text
16 bytes
128 bits
```

The Transfer ID identifies one logical send job across TCP reconnect attempts.

It is distinct from:

- file-content identity;
- filename;
- file hash;
- peer identity;
- cryptographic authentication.

Two transfers of identical file contents may use different Transfer IDs.

The current reference sender generates a Transfer ID using operating-system randomness when `run_sender` begins and preserves that same ID for every automatic retry belonging to that send job.

A new sender process currently generates a new Transfer ID.

The hexadecimal human-readable form used by WarpFile is:

```text
32 lowercase hexadecimal characters
```

For example:

```text
0123456789abcdef0123456789abcdef
```

The hexadecimal representation is for logs and local metadata.

On the WFP wire, the identifier is transmitted as its raw 16-byte value.

Transfer IDs must not be treated as secrets or authentication tokens.

### 8.2 Filename length

Filename length:

```text
u16
```

It counts the UTF-8 bytes of the filename.

### 8.3 Filename

Filename encoding:

```text
UTF-8
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

### 8.4 File size

File size:

```text
u64
```

The field contains the total source-file size in bytes.

---

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

---

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

Examples of states that may produce `CANNOT_PREPARE_DESTINATION` in the current reference receiver include:

- inability to create or inspect receiver-side persistent state;
- corrupted completion metadata;
- conflicting completion metadata for the same Transfer ID;
- a completion receipt whose physical file cannot be verified.

---

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

Before sending RESUME, the receiver hashes the actual retained `.part` file.

The sender then reads and hashes exactly the same prefix from the source file.

If the hashes match, the sender sends ACCEPT and continues from that offset.

If they do not match, the sender sends RESTART.

A valid resume offset may equal the total file size.

In that case, if the prefix digest matches the complete source file, no DATA frames need to be retransmitted.

The sender may proceed directly to COMPLETE.

---

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

---

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

WFP/0.2 does not include DATA sequence numbers because the current transfer transport is TCP, which already provides ordered reliable byte delivery.

The receiver writes DATA payloads sequentially.

For fresh transfers, DATA begins at byte zero.

For accepted resumed transfers, DATA begins at the negotiated resume offset.

Both peers update their BLAKE3 state while processing file data.

---

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

COMPLETE means that the sender has finished transmitting the logical file contents and is presenting the expected final digest.

It does **not** by itself tell the sender that the receiver has committed the completed file.

The sender considers the transfer successful only after receiving `VERIFIED`.

If the connection is lost while COMPLETE is being written or while the sender waits for VERIFIED, the reference sender may start another WFP session using the same Transfer ID.

WFP/0.2 reconciliation semantics make that retry safe.

---

## 15. VERIFIED

Message type:

```text
0x31
```

Payload:

```text
empty
```

WFP/0.2 defines two valid contexts for VERIFIED.

### 15.1 VERIFIED after COMPLETE

During normal transfer finalization, the receiver sends VERIFIED only after:

- the total retained plus newly received byte count equals the file size announced in OFFER;
- the locally calculated BLAKE3 digest matches the digest received in COMPLETE;
- the reference receiver has persisted the completion state required by its reconciliation design;
- the completed file has been committed to its final path.

Only after receiving VERIFIED may the sender consider that WFP session successful.

### 15.2 VERIFIED directly after OFFER

A receiver may reply directly to OFFER with VERIFIED when it can establish that the same logical transfer was already completed.

For the reference implementation this requires:

- the same Transfer ID;
- the same filename;
- the same announced file size;
- an existing valid completion receipt;
- physical file bytes whose size and BLAKE3 digest match that receipt.

The receiver must not send VERIFIED solely because a receipt file exists.

The physical completed data must also be verified.

The valid state machine therefore includes:

```text
OFFER
  |
  +--> ACCEPT
  |
  +--> RESUME
  |
  +--> REJECT
  |
  +--> VERIFIED
```

When the sender receives VERIFIED directly after OFFER:

- the VERIFIED payload must be empty;
- no file contents need to be retransmitted;
- the sender may immediately consider the logical transfer successful.

---

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

The current receiver removes incomplete partial transfer state after cancellation.

CANCEL therefore does not request later resume.

CANCEL must not be confused with an unexpected transport failure.

---

## 17. Transfer identity semantics

WFP/0.2 distinguishes a **logical transfer** from an individual TCP session.

Conceptually:

```text
logical transfer
Transfer ID = X
      |
      +---- TCP session 1
      |
      +---- TCP session 2
      |
      +---- TCP session 3
```

Every reconnect belonging to the same sender job uses the same Transfer ID.

This makes receiver-side persistent state correlatable across network sessions.

### 17.1 Transfer ID is not content identity

Transfer ID does not mean:

```text
"these bytes uniquely identify this file content"
```

BLAKE3 provides content-integrity evidence.

Transfer ID instead means approximately:

```text
"these protocol sessions belong to the same logical send job"
```

### 17.2 Transfer ID is not peer identity

Transfer ID does not authenticate:

- the sender;
- the receiver;
- a device;
- a user.

WFP/0.2 currently has no authenticated peer identity.

### 17.3 Transfer ID reuse

An implementation should not intentionally reuse the same Transfer ID for unrelated logical transfers.

The reference receiver treats conflicting persistent completion information for the same ID conservatively rather than silently replacing it.

### 17.4 Process lifetime

The current reference sender generates one Transfer ID when a new `run_sender` operation begins.

Automatic retry preserves it.

A sender process restart currently does not.

Persistent sender-side transfer identity is future work.

---

## 18. Partial-file behavior

Incoming incomplete data is written to:

```text
.warpfile/partials/<filename>.part
```

For example:

```text
.warpfile/partials/video.mkv.part
```

The final destination is not created from incomplete bytes.

Recoverable transport loss preserves useful partial state.

Cancellation, protocol failure or integrity failure may discard partial state according to the reference receiver's failure policy.

A zero-length `.part` provides no useful resume state and is discarded before starting a fresh transfer.

A `.part` larger than the file size announced in OFFER cannot be a valid prefix and is discarded before starting a fresh transfer.

---

## 19. Receiver-side partial metadata

The current reference receiver associates a local metadata file with a partial transfer.

For:

```text
.warpfile/partials/video.mkv.part
```

the metadata path is:

```text
.warpfile/partials/video.mkv.part.warpmeta
```

This file is local receiver state.

It is **not** sent over WFP.

The current metadata format contains conceptually:

```json
{
  "format_version": 1,
  "transfer_id": "0123456789abcdef0123456789abcdef",
  "filename": "video.mkv",
  "file_size": 500000000,
  "state": "partial"
}
```

The metadata format version is independent from the WFP protocol version.

### 19.1 Fresh partial creation

For a fresh transfer, the reference receiver:

```text
create .part
    |
    v
persist .warpmeta
    |
    v
send ACCEPT
```

The metadata is persisted before the receiver tells the sender that fresh transfer state is ready.

### 19.2 Metadata is not proof of byte validity

The `.warpmeta` file is auxiliary state.

It does not make `.part` bytes trusted.

Resume still verifies the actual prefix with BLAKE3.

### 19.3 Missing, corrupt or mismatched metadata

A useful partial file must not automatically be discarded only because metadata is:

- missing;
- unreadable;
- corrupt;
- associated with another Transfer ID.

The reference receiver may still hash the actual `.part` bytes and send RESUME.

If the sender independently proves that the prefix matches its source by returning ACCEPT, the reference receiver may adopt the verified partial for the new Transfer ID and rewrite the metadata.

This permits useful data to survive a sender-process restart, even though that new process generated a different Transfer ID.

### 19.4 RESTART

When the sender sends RESTART, the receiver discards both:

```text
.warpfile/partials/<filename>.part
.warpfile/partials/<filename>.part.warpmeta
```

and prepares fresh state for the current OFFER.

---

## 20. BLAKE3 integrity and resume

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

If the prefix digests match, both peers retain their logical BLAKE3 state and continue feeding only the remaining bytes into the complete-file digest.

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

The already retained prefix is read locally to rebuild hash state but is not retransmitted over the network.

The current implementation therefore avoids a second complete-file pre-hash before starting a normal transfer while still supporting verified resume.

Future optimizations may persist hash checkpoints so very large retained prefixes do not need to be fully re-read on each resume attempt.

---

## 21. Resume safety properties

WFP/0.2 does not trust a partial file based only on:

- Transfer ID;
- filename;
- announced file size;
- partial length;
- local metadata.

Two different files may share some or all of those attributes.

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

Resume verification provides integrity of the retained prefix.

It does not provide peer authentication or protection against a malicious network peer.

---

## 22. Persistent completion receipts

WFP/0.2 completion reconciliation is supported by persistent receiver-side completion state.

The reference implementation stores immutable completion receipts under:

```text
<destination-directory>/
└── .warpfile/
    └── receipts/
        └── <transfer-id>.json
```

For example:

```text
received/
├── video.mkv
└── .warpfile/
    └── receipts/
        └── 0123456789abcdef0123456789abcdef.json
```

A completion receipt contains conceptually:

```json
{
  "format_version": 1,
  "transfer_id": "0123456789abcdef0123456789abcdef",
  "filename": "video.mkv",
  "file_size": 500000000,
  "blake3": "..."
}
```

The receipt format version is local metadata versioning.

It is independent from WFP/0.2.

Completion receipts are not transmitted over the network.

### 22.1 Receipt meaning

A valid receipt means that the receiver previously reached a state where all bytes for that Transfer ID were received and the complete-file BLAKE3 value was validated.

The receipt does not replace verification of the physical file during reconciliation.

### 22.2 Immutable behavior

The current receipt design is immutable.

If the exact same receipt is written again:

```text
same Transfer ID
same filename
same size
same BLAKE3
```

the operation is idempotent.

If an existing receipt for that Transfer ID contains different completion information, the reference implementation reports a conflict rather than silently replacing it.

### 22.3 Receipt persistence

The current reference writer uses a temporary receipt file, writes and synchronizes its contents, then promotes it to the final receipt path.

This design protects the normal process-crash/network-loss reconciliation path.

It must not be interpreted as an absolute guarantee against every sudden power-loss or filesystem failure scenario on every operating system.

Directory-entry durability and platform-specific rename guarantees remain separate implementation concerns.

---

## 23. Completion commit ordering

Before a normal successful VERIFIED response, the reference receiver establishes all information necessary for later reconciliation.

Conceptually:

```text
receive DATA and COMPLETE
        |
        v
persist completed .part contents
        |
        v
validate size + BLAKE3
        |
        v
persist immutable completion receipt
        |
        v
rename .part -> final file
        |
        v
remove partial metadata
        |
        v
send VERIFIED
```

The key invariant is:

```text
receipt must not represent unverified file bytes
```

and:

```text
once a completion receipt exists,
failure to rename must not destroy the complete .part
```

A state such as:

```text
.warpfile/partials/file.part
.warpfile/partials/file.part.warpmeta
completion receipt
```

is intentionally recoverable.

---

## 24. Completion reconciliation

When a new OFFER arrives, the reference receiver checks whether a completion receipt exists for the offered Transfer ID before applying the normal `FILE_EXISTS` decision.

### 24.1 Receipt absent

If no receipt exists:

```text
normal fresh/resume negotiation
```

continues.

### 24.2 Receipt + final file

If:

```text
receipt exists
final file exists
```

the receiver:

```text
compare receipt identity with OFFER
        |
        v
verify final file size
        |
        v
rehash actual final file with BLAKE3
        |
        v
compare with receipt
        |
        v
send VERIFIED
```

No DATA is retransmitted.

### 24.3 Receipt + complete partial file

If:

```text
receipt exists
final file absent
complete .part exists
```

the receiver:

```text
compare receipt identity with OFFER
        |
        v
verify .part size
        |
        v
rehash actual .part
        |
        v
compare with receipt
        |
        v
rename .part -> final
        |
        v
clean partial metadata
        |
        v
send VERIFIED
```

This repairs the crash window between receipt persistence and final rename.

### 24.4 Receipt identity mismatch

For the same receipt path, a filename or file-size mismatch with the new OFFER is treated as a conflict.

The receiver must not reinterpret the old receipt as completion proof for unrelated transfer parameters.

### 24.5 Receipt exists but physical data is invalid

A receipt must never be trusted blindly.

If the final file or complete partial file:

- has the wrong size;
- has the wrong BLAKE3 digest;
- is not a valid regular file;
- is otherwise inconsistent with the receipt;

the receiver must not send VERIFIED.

The reference implementation rejects reconciliation conservatively.

### 24.6 Receipt exists but data is missing

If a receipt exists but neither usable final data nor usable completed partial data exists, the receiver cannot prove successful completion.

It must not send VERIFIED.

---

## 25. Automatic reconnect and retry semantics

WFP/0.2 defines the protocol states that make reconnection and completion reconciliation possible.

It does **not** mandate:

- retry timing;
- delay strategy;
- a maximum retry count;
- exponential backoff;
- user-interface policy.

Those are implementation decisions.

The current WarpFile sender uses:

```text
maximum attempts: 3
delay between attempts: 1 second
```

for selected recoverable network failures.

Each attempt creates a new TCP connection and performs:

```text
HELLO
HELLO_ACK
OFFER
```

again.

The same logical send job reuses the same Transfer ID.

### 25.1 Failure before completion

If a recoverable connection loss occurs while transferring DATA:

```text
connection lost
      |
      v
receiver retains useful .part
      |
      v
sender reconnects
      |
      v
same Transfer ID OFFER
      |
      v
RESUME
```

### 25.2 Failure during finalization

WFP/0.1 could not safely retry every loss around COMPLETE / VERIFIED because no persistent transfer identity connected the new session to previous committed completion state.

WFP/0.2 removes that protocol ambiguity for a running sender job.

If the connection fails while COMPLETE is being sent or while the sender waits for VERIFIED:

```text
network loss
    |
    v
sender retries
    |
    v
same Transfer ID
```

The receiver can then resolve the actual state.

If COMPLETE did not reach a completed state:

```text
partial state
   |
   v
RESUME / normal recovery
```

If completion was already persisted:

```text
completion receipt
   |
   v
verify physical file
   |
   v
VERIFIED directly after OFFER
```

### 25.3 Permanent failures

Permanent states do not trigger automatic retry in the current reference sender.

Examples include:

- receiver REJECT;
- malformed protocol state;
- protocol decode/encode failure that is not a transient transport condition;
- invalid local source;
- explicit user cancellation.

---

## 26. Why direct VERIFIED does not create blind trust

The direct:

```text
OFFER -> VERIFIED
```

path does not mean:

```text
"the receiver remembers this Transfer ID, therefore success"
```

The reference receiver verifies persisted state against physical bytes.

Conceptually:

```text
Transfer ID
   |
   v
receipt
   |
   +--> expected filename
   +--> expected size
   +--> expected BLAKE3
               |
               v
         physical file
               |
               v
          rehash bytes
               |
               v
         exact comparison
```

Only then is VERIFIED sent.

This makes the uncommon reconciliation path more expensive than the normal path because it re-reads the completed file.

That is intentional.

Normal successful transfers still hash the data during streaming in one pass.

The extra rehash occurs only when reconciliation is required.

---

## 27. Discovery model

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

WFP/0.2 uses the same frame version for TCP transfer frames and UDP discovery frames.

---

## 28. DISCOVER

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

---

## 29. ANNOUNCE

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

---

## 30. Discovery providers

The WFP discovery messages are independent from the mechanism used to find candidate IP addresses.

The current reference implementation uses two candidate providers.

### 30.1 Local network discovery

WarpFile enumerates local IPv4 interfaces and calculates directed broadcast addresses from each address and subnet mask.

It sends WFP DISCOVER to those network broadcasts.

Loopback, unspecified, link-local and `/32` addresses are not used as broadcast targets.

Responses originating from the local machine are filtered to avoid self-discovery.

### 30.2 Tailscale-assisted discovery

When the `tailscale` CLI is available, WarpFile reads online Tailscale peers and treats their IPv4 addresses as discovery candidates.

WarpFile does **not** assume that a Tailscale peer is a WarpFile peer.

It sends a WFP DISCOVER datagram directly to each candidate.

Only a peer that responds with a valid WFP ANNOUNCE becomes a discovered WarpFile device.

Tailscale is optional.

WarpFile continues to operate without it.

---

## 31. Device-name resolution

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

---

## 32. Default ports

Current development defaults:

```text
TCP transfer: 42069
UDP discovery: 42070
```

These ports are implementation defaults and are not permanently reserved protocol assignments.

They may change before stable release.

---

## 33. Security

WFP/0.2 currently provides:

- no encryption;
- no peer authentication;
- no authenticated device identity;
- no authenticated Transfer ID.

Network input must be treated as untrusted.

The current version should only be used in trusted development environments or over an independently trusted network layer.

BLAKE3 prefix hashes and final hashes provide integrity checks.

They do not authenticate a network peer.

Transfer IDs provide correlation.

They do not authenticate a network peer.

Receiver-side `.warpmeta` files and completion receipts are local implementation state.

They must not be treated as cryptographic proof against a malicious actor with local filesystem write access.

The reference receiver therefore verifies actual file bytes before using a completion receipt to send direct VERIFIED.

Future security work will use established cryptographic algorithms and libraries.

WarpFile will not invent custom cryptographic primitives.

---

## 34. Current limitations

The current WFP/0.2 and reference implementation intentionally remain simpler than the long-term WarpFile design.

Current limitations include:

- no encryption or authenticated session;
- no authenticated peer/device identity;
- no persistent sender-side Transfer ID database;
- sender process restart generates a new Transfer ID;
- completion receipts are receiver-local implementation state;
- no protocol STATUS query message;
- no persisted BLAKE3 hash checkpoints;
- retained prefixes must currently be re-read locally to rebuild BLAKE3 state;
- completion reconciliation rehashes the physical completed file;
- resume is based on one contiguous prefix rather than an arbitrary chunk map;
- one file is transferred per TCP connection;
- no directory or batch transfer manifest;
- no content-defined deduplication;
- no NAT traversal;
- no relay fallback;
- no multi-source transfer;
- no receipt garbage-collection policy is yet defined by WFP;
- absolute sudden-power-loss durability is not guaranteed uniformly across operating systems and filesystems.

These limitations do not weaken the integrity requirement for an accepted resume offset or a successful completion reconciliation.

### 34.1 Sender restart and completion identity

There is an important distinction between partial recovery and completed-transfer reconciliation.

Partial bytes can be adopted by a new sender process with a new Transfer ID when BLAKE3 prefix verification proves those bytes match the new source.

Completed-transfer receipts are indexed by the previous Transfer ID.

Because the current sender does not persist its Transfer ID across process restarts, a completely new sender process cannot automatically reproduce the previous logical completion identity.

Persistent sender-side job identity is future work.

---

## 35. Evolution beyond prefix resume

The current resume model represents receiver progress as one verified contiguous prefix.

Future designs may move toward chunk-aware recovery.

Conceptually:

```text
current:

[####################........]
 ^ verified prefix


future:

[####][....][####][....][####]
 have       have        have
```

That could support:

- non-contiguous missing ranges;
- partial deduplication;
- chunk-level recovery;
- multiple sources;
- private swarm distribution;
- more efficient transfer of modified versions of large files.

Such future work must preserve the fundamental rule:

```text
Never transfer a byte that does not need to be transferred.
```

Any chunk design must still validate content cryptographically rather than trusting only filenames, offsets or local metadata.

---

## 36. Compatibility

WFP/0.2 is experimental.

The protocol version byte is:

```text
0x02
```

WFP/0.2 changes the OFFER payload relative to WFP/0.1 by prepending a 16-byte Transfer ID.

Therefore a WFP/0.1 OFFER and WFP/0.2 OFFER are not wire-compatible.

WFP/0.2 also adds protocol semantics permitting:

```text
OFFER -> VERIFIED
```

for verified completion reconciliation.

The current implementation expects both peers to speak the same WFP version.

No backward compatibility guarantee exists before a stable protocol release.

---

## 37. WFP/0.2 summary

The central state transitions are:

```text
Fresh:

OFFER
  |
  v
ACCEPT
  |
  v
DATA*
  |
  v
COMPLETE
  |
  v
VERIFIED
```

```text
Resume:

OFFER
  |
  v
RESUME
  |
  v
ACCEPT
  |
  v
DATA*
  |
  v
COMPLETE
  |
  v
VERIFIED
```

```text
Invalid retained prefix:

OFFER
  |
  v
RESUME
  |
  v
RESTART
  |
  v
ACCEPT
  |
  v
DATA from byte 0
```

```text
Lost final confirmation:

session 1:

OFFER
  |
  v
ACCEPT / RESUME
  |
  v
DATA*
  |
  v
COMPLETE
  |
  v
receiver persists completion
  |
  X connection lost before sender receives VERIFIED


session 2:

same Transfer ID
  |
  v
OFFER
  |
  v
receiver verifies receipt + physical file
  |
  v
VERIFIED
```

The key WFP/0.2 addition is therefore not merely a new field.

It establishes a persistent logical transfer identity that allows a later TCP session to safely answer the question:

```text
"Was this exact logical transfer already completed?"
```

without retransmitting the file.
