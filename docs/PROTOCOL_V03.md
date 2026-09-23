# WarpFile Protocol — WFP/0.3

**Status:** Experimental

**Protocol version:** 0.3 (opt-in extension of WFP/0.2)

**Base framing:** WFP/0.2 (12-byte frame header)

**Reference implementation:** WarpFile development after `v0.1.0-alpha.2`

WFP/0.3 is an opt-in extension of WFP/0.2. It reuses the 12-byte frame header and most message types, adding chunk-aware resume. It is selected explicitly by the peer (via CLI flag in the reference implementation); there is no on-the-wire version negotiation.

WFP/0.3 keeps the WFP/0.2 goal of never transferring a byte that does not need to be transferred, and replaces the WFP/0.2 contiguous-prefix resume model with deterministic, chunk-level resume.

---

## 1. Relationship to WFP/0.2

WFP/0.3 is not a replacement for WFP/0.2. It reuses:

- the 12-byte binary frame header (magic, version, message type, flags, payload length);
- the big-endian byte order;
- the 16-byte Transfer ID semantics;
- the filename rules and the completion-receipt model.

The differences introduced by WFP/0.3 are:

- the frame version byte is `0x03`;
- `OFFER`, `RESUME` and `DATA` have version-specific payloads;
- two new message types, `CHUNK_HASHES` and `CHUNK_START`;
- receiver progress is expressed as a sparse set of verified chunks instead of one contiguous prefix.

Unless stated otherwise in this document, the WFP/0.2 rules described in `PROTOCOL.md` apply.

The reference implementation selects the version externally (a CLI flag). A peer announces the version it intends to use in `HELLO`, but there is no negotiation: the receiver expects the announced version and rejects frames carrying a different version.

---

## 2. Byte order

Same as WFP/0.2: all multi-byte integer fields use big-endian (network) byte order.

---

## 3. Frame format and version

WFP/0.3 reuses the WFP/0.2 12-byte header.

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

Same as WFP/0.2:

```text
57 46 50 00
```

ASCII representation:

```text
W F P \0
```

Frames with an invalid magic value must be rejected.

### 3.2 Version

WFP/0.3 uses:

```text
0x03
```

The frame version is independent from the WarpFile application package version.

The default active version remains `0x02`. Frames with version `0x03` are used only when the peer has explicitly selected WFP/0.3.

### 3.3 Flags

Reserved, same as WFP/0.2:

```text
0x0000
```

Implementations must not assign undocumented meaning to these bits.

### 3.4 Payload length

Same limits as WFP/0.2:

```text
general maximum payload: 1048576 bytes (1 MiB)
DATA maximum payload:    65536 bytes (64 KiB)
```

### 3.5 Version-specific message-type rules

The message type is validated against the frame version. `CHUNK_HASHES` (`0x15`) and `CHUNK_START` (`0x16`) are valid only under version `0x03`; under `0x02` they are rejected. Every message type valid under `0x02` remains valid under `0x03`, so WFP/0.3 is a superset of the WFP/0.2 message-type space.

---

## 4. Message types

```text
Value    Name            0.2        0.3
-----    ----------      ---        ---
0x01     HELLO           yes        yes
0x02     HELLO_ACK       yes        yes

0x10     OFFER           yes        yes (v0.3 payload)
0x11     ACCEPT          yes        yes
0x12     REJECT          yes        allocated*
0x13     RESUME          yes        yes (v0.3 payload)
0x14     RESTART         yes        allocated*
0x15     CHUNK_HASHES    no         yes
0x16     CHUNK_START     no         yes

0x20     DATA            yes        yes (v0.3 payload)

0x30     COMPLETE        yes        yes
0x31     VERIFIED        yes        yes

0x40     CANCEL          yes        allocated*

0x50     DISCOVER        yes        allocated*
0x51     ANNOUNCE        yes        allocated*

0xFF     ERROR           yes        allocated*
```

`allocated*` means the message type is decodable under `0x03` at the framing level but is not part of the WFP/0.3 transfer flow. The WFP/0.3 flow does not define `REJECT`, `RESTART`, `CANCEL`, `DISCOVER`, `ANNOUNCE` or `ERROR` behavior; in particular, WFP/0.3 has no UDP discovery and no on-the-wire version negotiation.

The message types used by the WFP/0.3 transfer flow are:

```text
HELLO
HELLO_ACK
OFFER
RESUME
CHUNK_HASHES
ACCEPT
CHUNK_START
DATA
COMPLETE
VERIFIED
```

Shared message payloads in WFP/0.3:

- `ACCEPT` and `VERIFIED` keep an empty payload;
- `COMPLETE` keeps the 32-byte full-file BLAKE3 payload;
- `HELLO` and `HELLO_ACK` keep the one-version-byte payload.

Unknown message types must be rejected.

---

## 5. HELLO and HELLO_ACK

Message types:

```text
HELLO      0x01
HELLO_ACK  0x02
```

Payload (both):

```text
Offset    Size    Field
------    ----    ----------------
0         1       Protocol version
```

For WFP/0.3 both peers use:

```text
0x03
```

Both the frame header version and the `HELLO` / `HELLO_ACK` payload byte are `0x03`. A peer receiving a `HELLO` or `HELLO_ACK` whose payload is not exactly one byte with value `0x03` rejects the session.

---

## 6. OFFER (v0.3)

Message type:

```text
0x10
```

WFP/0.3 `OFFER` adds a chunk size to the WFP/0.2 `OFFER`.

Payload:

```text
Offset        Size    Field
------        ----    ----------------
0             16      Transfer ID
16            2       Filename length
18            N       Filename
18 + N        8       File size
18 + N + 8    8       Chunk size
```

Minimum payload size, excluding any filename bytes:

```text
34 bytes
```

Fields:

- **Transfer ID** — 16 bytes (128 bits). Same semantics as WFP/0.2: a correlation identifier, not content identity and not authentication.
- **Filename length** — `u16`, the UTF-8 byte count of the filename. Must be non-zero.
- **Filename** — UTF-8 basename only. Same safety rules as WFP/0.2: no absolute paths, directory separators or traversal attempts.
- **File size** — `u64`, the total source-file size in bytes.
- **Chunk size** — `u64`, must be greater than zero. It defines the chunk layout described in section 10.

The decoder requires the payload length to match the declared filename length exactly; truncated or trailing bytes are rejected. The total payload is bounded by the general frame limit of `1048576` bytes.

---

## 7. RESUME (v0.3)

Message type:

```text
0x13
```

Payload:

```text
Offset    Size    Field
------    ----    ----------------
0         8       Chunk record count
```

Total payload size:

```text
8 bytes
```

Field:

- **Chunk record count** — `u64`, the total number of chunk hash records the receiver will send in the `CHUNK_HASHES` frames that follow `RESUME`.

Comparison with WFP/0.2 `RESUME`:

The WFP/0.2 `RESUME` is 40 bytes:

```text
u64 resume offset + 32-byte BLAKE3 prefix digest
```

It describes one verified contiguous prefix. WFP/0.3 `RESUME` instead declares the size of a sparse chunk inventory; the actual chunk indices and hashes travel in `CHUNK_HASHES`. A record count of zero means the receiver holds no reusable chunks.

---

## 8. CHUNK_HASHES (v0.3)

Message type:

```text
0x15
```

`CHUNK_HASHES` carries one batch of the receiver's reusable-chunk inventory.

Payload:

```text
Offset    Size        Field
------    ----        ----------------
0         4           Record count (u32)
4         40 * N      Records
```

Each record:

```text
Offset    Size    Field
------    ----    ----------------
0         8       Chunk index
8         32      BLAKE3 chunk hash
```

Record size:

```text
40 bytes
```

The record count is the number of records in this frame. The payload is bounded by the general frame limit of `1048576` bytes, which allows at most:

```text
26214 records per CHUNK_HASHES frame
```

because `26214 * 40 + 4 = 1048564` bytes.

Relationship with `RESUME`:

- `RESUME` declares the total record count as `u64`; each `CHUNK_HASHES` frame declares its own batch count as `u32`.
- The records follow in one or more `CHUNK_HASHES` frames until the declared total is reached.
- Every chunk index must exist within the offered layout.
- Chunk indices must be strictly increasing across the whole inventory.

The sender rejects an empty batch while records are still expected, an out-of-range index, and duplicate or decreasing indices.

---

## 9. CHUNK_START (v0.3)

Message type:

```text
0x16
```

Payload:

```text
Offset    Size    Field
------    ----    ----------------
0         8       Chunk index
8         32      Expected chunk hash
```

Total payload size:

```text
40 bytes
```

`CHUNK_START` begins the transmission of one chunk. The expected hash is the BLAKE3 digest of exactly that chunk's byte range (section 11).

Semantics on the receiver:

- the chunk index must exist within the offered layout;
- no other chunk may already be active;
- the index must be strictly greater than the index of the previously started chunk;
- a chunk whose hash already matches the receiver inventory must not be started again.

After `CHUNK_START`, the sender sends one or more `DATA` frames covering exactly that chunk's byte range, in order.

---

## 10. DATA (v0.3)

Message type:

```text
0x20
```

Payload:

```text
Offset    Size    Field
------    ----    ----------------
0         8       Absolute offset
8         N       File bytes
```

Fields:

- **Absolute offset** — `u64`, the file offset at which the following bytes must be written.
- **File bytes** — `N` raw bytes, with `N >= 1`.

Limits:

```text
DATA frame payload:  <= 65536 bytes
raw file bytes (N):  <= 65528 bytes
```

The frame-level `DATA` limit of `65536` bytes is shared with WFP/0.2, but in WFP/0.3 the first 8 payload bytes are the absolute offset, so the maximum raw-byte count per `DATA` frame is:

```text
65536 - 8 = 65528 bytes
```

Contiguity: within an active chunk, the first `DATA` frame must carry the chunk's start offset and every subsequent `DATA` frame must carry the offset immediately following the previously received bytes. The receiver rejects:

- `DATA` without an active chunk;
- an offset that does not match the next expected byte;
- bytes that extend past the end of the active chunk;
- bytes that exceed the offered file size.

---

## 11. Chunk layout

The chunk layout is deterministic and derived entirely from the `OFFER`:

```text
chunk_size   = OFFER.chunk_size
file_size    = OFFER.file_size
chunk_count  = ceil(file_size / chunk_size)
```

Chunk `i` covers the byte range:

```text
offset(i) = i * chunk_size
length(i) = min(chunk_size, file_size - offset(i))
```

Properties:

- An empty file (`file_size = 0`) has `0` chunks.
- Every non-empty file has at least one chunk.
- All chunks except the last have exactly `chunk_size` bytes.
- The last chunk may be partial.

`chunk_size` must be non-zero. The reference sender uses a default of `1048576` bytes and transmits it in `OFFER`; the receiver derives the layout from the offered value.

---

## 12. Chunk hash

The chunk hash is the BLAKE3 digest of exactly that chunk's byte range:

```text
hash(i) = BLAKE3(file[offset(i) .. offset(i) + length(i)])
```

Chunk hashes determine which regions may potentially be reused. The complete-file digest carried in `COMPLETE` is a separate BLAKE3 over the entire file and is the integrity check that governs final acceptance.

---

## 13. Chunk state (`.warpchunks`)

The receiver may persist an advisory snapshot of verified chunks next to the partial file.

For a partial file:

```text
.warpfile/partials/video.mkv.part
```

the snapshot path is:

```text
.warpfile/partials/video.mkv.part.warpchunks
```

and the temporary snapshot path is:

```text
.warpfile/partials/video.mkv.part.warpchunks.tmp
```

Format (JSON, pretty-printed):

```json
{
  "format_version": 1,
  "file_size": 500000000,
  "chunk_size": 1048576,
  "chunks": [
    { "index": 0, "blake3": "..." },
    { "index": 3, "blake3": "..." }
  ]
}
```

Rules:

- `format_version` must equal `1`.
- `file_size` and `chunk_size` must reproduce the offered layout.
- `chunks` is a sparse, strictly ordered list; indices must be unique, increasing, and within the layout.
- `blake3` is the 32-byte chunk hash encoded as 64 lowercase hexadecimal characters.

The snapshot is advisory. It must never be treated as proof that physical bytes are correct. Before a recorded chunk is treated as reusable, the receiver reads the corresponding physical bytes and recomputes their BLAKE3; only chunks that still match their recorded hash are retained. Missing, malformed, mismatched or stale snapshots yield an empty inventory rather than causing useful data to be trusted.

The snapshot format version is local metadata versioning and is independent from the WFP protocol version.

---

## 14. Completion receipts

WFP/0.3 uses the same completion-receipt model as WFP/0.2, stored under:

```text
<destination-directory>/
└── .warpfile/
    └── receipts/
        └── <transfer-id>.json
```

A receipt contains:

```json
{
  "format_version": 1,
  "transfer_id": "0123456789abcdef0123456789abcdef",
  "filename": "video.mkv",
  "file_size": 500000000,
  "blake3": "..."
}
```

The receipt format version is local metadata versioning, independent from the WFP version.

A receipt is written only after the complete `.part` has passed size and full-file BLAKE3 validation, and it is persisted before the `.part` is renamed to the final path. Receipts are immutable: writing the same receipt again is idempotent; a different receipt for the same Transfer ID is a conflict and is not silently replaced.

---

## 15. Receiver flow

The receiver flow for a fresh or resumed WFP/0.3 transfer:

```text
Sender                                      Receiver
  |                                            |
  | ---------------- HELLO -----------------> |
  | <------------- HELLO_ACK ---------------- |
  |                                            |
  | ---------------- OFFER -----------------> |
  |                                            |
  |                 reconcile receipt?         |
  |                 (direct VERIFIED if done)  |
  |                                            |
  | <-------------- VERIFIED ---------------- |   (only when reconciled)
  |                                            |
  |                 else prepare partial       |
  |                 prepare chunk inventory    |
  |                                            |
  | <--------------- RESUME ----------------- |
  | <----------- CHUNK_HASHES -------------- |
  | <----------- CHUNK_HASHES -------------- |
  |                   ...                      |
  |                                            |
  | ---------------- ACCEPT -----------------> |
  |                                            |
  | -------------- CHUNK_START --------------> |
  | ---------------- DATA -------------------> |
  |                   ...                      |
  |                                            |
  | -------------- COMPLETE -----------------> |
  |                                            |
  |                 verify complete file       |
  |                 persist receipt            |
  |                 rename .part -> final      |
  |                 clean .warpchunks          |
  |                                            |
  | <-------------- VERIFIED ----------------- |
```

Phases:

1. **Hello.** The receiver reads `HELLO` (frame version and payload byte both `0x03`) and answers `HELLO_ACK`.
2. **Offer.** The receiver reads and decodes the v0.3 `OFFER`.
3. **Reconciliation.** If a valid completion receipt exists for the Transfer ID (section 16), the receiver may answer `VERIFIED` directly and skip data transfer.
4. **Prepare.** The receiver derives the layout, loads and revalidates any persisted chunk inventory, and opens the partial file. The reference receiver sizes the `.part` to the full offered file size (sparse) and writes received chunks in place.
5. **Negotiate.** The receiver sends `RESUME` (total record count) followed by one or more `CHUNK_HASHES` frames, then waits for `ACCEPT` with an empty payload.
6. **Receive.** The receiver accepts `CHUNK_START` followed by `DATA` for each chunk the sender decides to transmit. It verifies each chunk hash as the chunk completes and persists a verified-chunk snapshot before accepting the next chunk.
7. **Complete.** The receiver expects `COMPLETE` with a 32-byte full-file hash. It requires that every chunk of the layout has been verified and that no chunk is still active or waiting for persistence.
8. **Verify and finalize.** The receiver re-reads the assembled file, checks its size and full-file BLAKE3 against `COMPLETE`, persists the completion receipt, renames `.part` to the final path, and cleans the `.warpchunks` snapshot.
9. **Verified.** The receiver sends `VERIFIED` (empty payload).

Chunks are received sequentially; the receiver does not accept out-of-order chunk indices or data offsets.

---

## 16. Receiver-side reconciliation

Given an `OFFER`, the receiver first checks for a completion receipt for the offered Transfer ID.

- **No receipt** — proceed with the normal prepare/negotiate flow.
- **Conflicting receipt** — if the receipt's embedded Transfer ID, filename or file size differs from the `OFFER`, the session aborts as a conflict.
- **Receipt + final file** — if the final file exists and its size and BLAKE3 match the receipt, the receiver sends `VERIFIED` directly.
- **Receipt + complete `.part`** — if the final file is absent but a complete `.part` exists and verifies, the receiver renames it to the final path, cleans the chunk snapshot, and sends `VERIFIED`.
- **Receipt present but physical data invalid or missing** — the receiver must not send `VERIFIED`; it falls back to the normal flow and re-downloads.

A receipt is never trusted by itself. The physical final file or complete `.part` must pass size and BLAKE3 verification before `VERIFIED` is sent.

---

## 17. Sender flow

The sender flow:

```text
Sender                                      Receiver
  |                                            |
  | ---------------- HELLO -----------------> |
  | <------------- HELLO_ACK ---------------- |
  |                                            |
  | ---------------- OFFER -----------------> |
  |                                            |
  | <--------------- RESUME ----------------- |
  | <----------- CHUNK_HASHES -------------- |
  |                   ...                      |
  |                                            |
  | ---------------- ACCEPT -----------------> |
  |                                            |
  | scan source chunks                         |
  |   Reuse:    skip (hash already held)       |
  |   Transmit: send chunk                     |
  |                                            |
  | -------------- CHUNK_START --------------> |
  | ---------------- DATA -------------------> |
  |                   ...                      |
  |                                            |
  | -------------- COMPLETE -----------------> |
  | <-------------- VERIFIED ----------------- |
```

Phases:

1. **Hello.** The sender sends `HELLO` (version `0x03`) and requires `HELLO_ACK` (version `0x03`).
2. **Offer.** The sender sends the v0.3 `OFFER` with the Transfer ID, filename, file size and chunk size.
3. **Negotiate.** The sender reads `RESUME` (record count), then `CHUNK_HASHES` frames until the declared count is reached. It validates that every chunk index is within the layout and strictly increasing, then answers `ACCEPT`.
4. **Scan.** The sender reads the source chunk by chunk, computing each chunk hash and the full-file hash in one pass.
   - If the receiver inventory already contains the chunk index with a matching hash, the chunk is classified **Reuse** and is not transmitted.
   - Otherwise the chunk is classified **Transmit**.
5. **Transmit.** For each Transmit chunk the sender sends `CHUNK_START` followed by one or more `DATA` frames (fragmenting at `65528` raw bytes).
6. **Complete.** After all chunks are scanned, the sender sends `COMPLETE` with the full-file hash.
7. **Verified.** The sender requires `VERIFIED` with an empty payload.

A fully reconciled inventory causes the sender to transmit no `DATA` at all; `COMPLETE` still carries the full-file hash and the receiver still verifies the assembled file.

---

## 18. Sender-side retry

The reference sender retries recoverable network failures automatically.

```text
maximum attempts: 3
delay between attempts: 1 second
```

All attempts reuse the same Transfer ID. Each attempt opens a new connection and restarts the session from `HELLO`; the receiver-side reconciliation and chunk inventory make the retry safe.

Failures are classified:

- **Retryable** — transport-level failures: connection refused, connection reset, connection aborted, not connected, broken pipe, timeout, unexpected end of stream, interrupted, or zero-length write. These are retried up to the maximum attempt count.
- **Permanent** — protocol decode/encode failures, malformed frames or payloads, invalid source files, receiver conflicts, and any other invalid protocol state. These abort without retry.

A permanent failure is reported immediately and does not trigger another attempt.

---

## 19. Integrity and safety properties

WFP/0.3 never trusts a chunk based only on metadata:

- A persisted chunk record proves only that a hash was recorded; the receiver re-reads and rehashes the physical bytes before advertising the chunk as reusable.
- The sender reuses a chunk only when its own source hash matches the receiver-advertised hash exactly.
- Final acceptance requires the complete assembled file to match the `COMPLETE` digest, both in size and in full-file BLAKE3.

These checks provide content integrity. They do not provide encryption, peer authentication, or authenticated device identity. Transfer IDs are correlation identifiers, not authentication tokens.

---

## 20. Compatibility

WFP/0.2 and WFP/0.3 coexist and are selected externally by the peer (a CLI flag in the reference implementation). There is no on-the-wire version negotiation.

- The default active version is `0x02`.
- Frames carry the selected version in the header byte; `0x03` frames are used only when WFP/0.3 was explicitly selected.
- A peer reading frames for a different version rejects them. A WFP/0.2 peer and a WFP/0.3 peer do not interoperate.
- `CHUNK_HASHES` and `CHUNK_START` are invalid under `0x02`.
- WFP/0.3 defines no UDP discovery and no version negotiation; both peers must be configured for the same version.

---

## 21. Summary of state transitions

```text
Fresh / resumed chunk transfer:

OFFER
  |
  +--> VERIFIED            (reconciliation short-circuit)
  |
  +--> RESUME
        |
        v
      CHUNK_HASHES*
        |
        v
      ACCEPT
        |
        v
      (CHUNK_START -> DATA*)*
        |
        v
      COMPLETE
        |
        v
      VERIFIED
```

The central difference from WFP/0.2 is that receiver progress is a sparse set of verified chunks (carried by `RESUME` + `CHUNK_HASHES`) rather than a single contiguous prefix, while the final `COMPLETE` / `VERIFIED` handshake and completion-receipt reconciliation remain the same.
