# Instructions for coding agents

This repository provides the native Go Cursor SDK bridge as a standalone binary and an embeddable library. Read `README.md`, inspect `git status`, and preserve existing work before making changes.

- Keep Cursor API keys and callback tokens out of logs, persisted agent state, tests, and commits. Use fake credentials in tests.
- Execute client-owned custom tools only through authenticated SDK callbacks. Never infer executable tool calls from assistant text or SDK metadata. Native host shell and filesystem operations remain unavailable unless the client-tool boundary is explicitly redesigned.
- Preserve durable checkpoint, KV, image, run-history and callback-receipt recovery. Unknown callback outcomes must not repeat executable actions. Keep writer exclusion, atomic publication and integrity checks.
- Preserve native protobuf oneof semantics, tool-call identifiers and arguments, usage counters, client cancellation, terminal events and callback resumption. Validate malformed upstream messages before execution.
- The SDK schemas in `third_party/cursor-sdk-bridge/` are pinned to Cursor SDK v1.2.0. Regenerate Go protobufs with `buf generate`; do not edit generated code manually. Schema extractor scripts verify the pinned reference binary hash before updating native descriptors.
- Use private state roots and mock servers in tests. Tests that change process environment remain serial. Run `go test ./...` and `go test -race ./...` for behavior or concurrency changes. Use opt-in live tests only with an independent test credential.
- Follow existing multiline Doxygen comments for new or modified declarations. Keep diagnostics in English.
