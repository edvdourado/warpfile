# WarpFile

WarpFile is an experimental peer-to-peer file transfer application focused on direct device-to-device communication.

The long-term goal is to provide fast, secure and zero-configuration file transfers between devices without requiring cloud storage or permanent user accounts.

> WarpFile is currently in early development.

## Goals

WarpFile aims to provide:

- direct peer-to-peer file transfers;
- cross-platform support;
- automatic device discovery;
- integrity verification;
- resumable transfers;
- encrypted sessions;
- NAT traversal;
- relay fallback when direct connectivity is unavailable;
- a simple CLI and, later, a desktop interface.

## Current milestone

### M0 — First Byte

Establish a TCP connection between two WarpFile instances and successfully exchange a valid `HELLO` frame using WFP/0.1.

## Roadmap

### M0 — First Byte

- [ ] CLI structure
- [ ] TCP listener
- [ ] TCP client
- [ ] WFP frame encoder
- [ ] WFP frame decoder
- [ ] `HELLO`
- [ ] `HELLO_ACK`

### M1 — First File

- [ ] `OFFER`
- [ ] `ACCEPT`
- [ ] file streaming
- [ ] progress reporting
- [ ] BLAKE3 integrity verification
- [ ] `COMPLETE`
- [ ] `VERIFIED`

### M2 — Zero Config

- [ ] LAN discovery
- [ ] device identity
- [ ] human-friendly transfer codes

### M3 — Reliable Transfer

- [ ] chunk management
- [ ] cancellation
- [ ] resume
- [ ] improved error handling
- [ ] directory transfers

### M4 — WarpFile 0.1

- [ ] usable cross-platform CLI
- [ ] Windows support
- [ ] Linux support
- [ ] automated tests
- [ ] release binaries
- [ ] documentation

## WarpFile Protocol

WarpFile uses its own application-layer protocol called **WFP**.

The first experimental version is:

`WFP/0.1`

The protocol specification is available at:

[`docs/PROTOCOL.md`](docs/PROTOCOL.md)

## Architecture

Architecture documentation is available at:

[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

## Security

WFP/0.1 is intentionally minimal and currently provides **no encryption or authentication**.

It must only be used in trusted development environments.

Encryption, device identity and authenticated sessions will be introduced in later protocol versions.

## Technology

The initial implementation is written in Rust.

Current target platforms:

- Windows
- Linux

## Status

Experimental.

The protocol and internal architecture may change without backward compatibility before the first stable release.