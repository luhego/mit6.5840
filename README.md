# MIT 6.5840 Lab Workspace

This repo is my working area for MIT 6.5840. It includes the upstream labs
under `labs/` and my own notes/experiments alongside them.

## Layout
- `labs/`: upstream lab code (kept intact for reference)
- notes/ (optional): personal notes, invariants, and experiments

## Common commands
From `labs/src`:
```
go test ./...
```

For MapReduce:
```
cd labs/src/main
go build -buildmode=plugin ../mrapps/wc.go
go run mrcoordinator.go pg*.txt
go run mrworker.go wc.so
```

## Debugging
VS Code launch config lives in `.vscode/launch.json`.
