# Cursor SDK for Go

A Go implementation of the Cursor SDK bridge's local agent protocol. It can run as a standalone `cursor-sdk-bridge` binary or as an embedded library. The implementation targets the Cursor SDK v1.2.0 protocol and is maintained independently of Cursor.

```sh
go build -o cursor-sdk-bridge ./cmd/cursor-sdk-bridge
./cursor-sdk-bridge --workspace /path/to/workspace --state-root /path/to/private-state
```

The bridge prints a discovery line to stderr with its loopback URL and authentication-token file. For embedding, use `bridge.Start(ctx, bridge.Options{...})`, then `Server.Close(ctx)` on shutdown. SDK Connect clients and protobuf messages are available under `sdk/v1`; the bridge implementation is in `bridge`.

The bridge preserves native checkpoints, run history and client-owned custom tool callbacks. It supports image inputs and tool-returned images. Local persistence requires an exclusive writer per state root; runtime API keys and callback tokens are not stored in agent state. Host shell and filesystem execution, external MCP servers and cloud agents are unavailable. Local artifact downloads are unsupported.

```sh
go test ./...
go test -race ./...
```

Schema extractors in `scripts/` check the pinned reference bridge binary's hash before deriving native tool and control descriptors. The SDK protocol definitions and upstream license are retained under `third_party/cursor-sdk-bridge/`. See `NOTICE` and `LICENSE.ccLoad` for ccLoad attribution.
