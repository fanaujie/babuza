# Simple Example

A minimal single-node Raft example demonstrating the basic usage of Babuza.

## Overview

This example shows how to:

- Bootstrap a single-node Raft cluster
- Implement a simple key-value state machine
- Propose and apply commands through Raft consensus
- Handle Raft lifecycle events via listener

## Run

```bash
go run .
```

## Key Components

### State Machine

`SimpleKVStore` implements `ibabuza.StateMachine`:

| Method | Description |
|--------|-------------|
| `Apply()` | Applies committed log entries to state |
| `SaveSnapshot()` | Serializes state for snapshots |
| `RestoreFromSnapshot()` | Restores state from snapshot |
| `Query()` | Reads value by key |

### Raft Listener

`SimpleListener` implements `ibabuza.RaftListener` to receive cluster events:

- `OnLeaderChange()` - Leader election notification
- `OnAcquiredLeader()` / `OnLostLeader()` - Leadership transitions
- `OnMemberChange()` - Cluster membership changes
- `OnRaftShutdown()` - Shutdown notification
