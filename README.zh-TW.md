# Babuza

**語言：** [English](./README.md) | 繁體中文

Babuza 把 [etcd Raft](https://github.com/etcd-io/raft) 包成可以直接嵌進 Go 服務的 framework。你不用從 Ready loop、WAL、snapshot、transport、session 和測試框架開始重造一套，只要專注在自己的 state machine 和產品邏輯。

## 為什麼用 Babuza？

etcd/raft 很穩，但直接拿來做產品服務時，你還是要補很多周邊工程：

- **Raft Ready loop**：你要自己處理 `Ready()`，並照正確順序處理 entry、message、snapshot 和 hard state
- **Storage 整合**：WAL 和 snapshot storage 要自己處理順序和一致性，例如先寫 entry 再送 message、原子套用 snapshot
- **Transport**：你要自己在 peer 之間收送 Raft message，還要處理連線失敗、重試和 snapshot transfer
- **State machine 生命週期**：snapshot 建立、還原、log compaction 都要跟 state machine 一起協調
- **Cluster membership**：新增／移除 peer、joint consensus、learner promotion 都要自己接起來

| 挑戰 | etcd Raft | Babuza |
|------|-----------|--------|
| **Memory** | log entry 全部放在記憶體 | index-based cache，節省 94-99% 記憶體 |
| **Transport** | 只有基礎 HTTP transport | 可插拔 TCP / HTTP / gRPC transport |
| **WAL** | etcd WAL | 多種後端：native、Badger、Pebble |
| **Snapshot transfer** | HTTP 全量傳輸 | 壓縮、分塊、可限速的 snapshot transfer |
| **Cluster operation** | 你要自己管理 peer | 內建 add/remove/update/transfer API |
| **Idempotency** | application 自己去重 | session-based exactly-once 語意 |
| **Observability** | 你要自己接 | 內建 Prometheus / OpenTelemetry 整合點 |
| **Disaster recovery** | 手動流程複雜 | 支援 standalone recovery |
| **Integration test** | 你要自己寫 harness | testcluster 支援故障注入與 network partition |

**Babuza 的目標是讓你寫產品邏輯，而不是一直補 Raft plumbing。**

## 核心功能

| 功能 | Babuza 提供 |
|------|-------------|
| **Raft runtime** | Ready loop、proposal、linearizable read 和生命週期管理 |
| **WAL backend** | Native Babuza WAL、etcd WAL、Badger、Pebble |
| **Snapshot 管理** | Durable、volatile、S3-compatible snapshot storage，支援 chunked transfer |
| **Transport 層** | 可插拔 TCP、HTTP、gRPC transport，包含 HTTP stream mode |
| **Client session** | no-op、expire、LRU session manager，可選 exactly-once 語意 |
| **Cluster operation** | add/remove/update peer、promote learner、transfer leadership、disaster recovery |
| **測試框架** | 多節點 testcluster，支援 network partition、node failure、restart 和 fault injection |
| **Observability** | Prometheus / OpenTelemetry 整合點 |

## 效能

### 記憶體效率

| Entry 數量 | 資料大小 | etcd 記憶體 | Babuza 記憶體 | 節省 |
|-----------|---------|------------|--------------|------|
| 100K      | 1 KB    | 102 MB     | 5.35 MB      | **94.8%** |
| 100K      | 10 KB   | 981 MB     | 5.35 MB      | **99.5%** |

Babuza 只把 log entry metadata 留在記憶體，entry payload 需要時才從 WAL 讀，所以記憶體使用量大致不會跟 entry data size 一起成長。完整數據請看[記憶體使用量 benchmark 報告](./docs/benchmarks/memory-usage-comparison.md)。

### HTTP Stream Transport

HTTP transport 支援 opt-in 的 stream mode，會重用長連線 HTTP request body 來傳送 framed Raft message 和 snapshot chunks。本機 benchmark 顯示，stream mode 可以明顯降低每筆訊息的 HTTP request 開銷：

| 工作負載 | Short Request | HTTP Stream | 改善幅度 |
|----------|---------------|-------------|----------|
| Batch message | 26.806 us/op | 2.349 us/op | 快 11.4x |
| Snapshot，32 x 256 B chunks | 990.327 us/op | 148.066 us/op | 快 6.7x |
| Snapshot，4 x 8 KiB chunks | 175.458 us/op | 90.616 us/op | 快 1.9x |

完整數據請看 [HTTP Stream Benchmark Comparison](./docs/benchmarks/http-stream-benchmark-comparison.md)，裡面有 benchmark 細節和 allocation 結果。

## 架構

![architecture](images/babuza_architecture.svg)

## 快速開始

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/raft"
)

// 1. 實作你的 state machine
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func (s *KVStore) Apply(e ibabuza.Entry) ibabuza.ApplyResult {
	var cmd struct{ Key, Value string }
	json.Unmarshal(e.Command, &cmd)
	s.mu.Lock()
	s.data[cmd.Key] = cmd.Value
	s.mu.Unlock()
	return ibabuza.ApplyResult{LogIndex: e.Index}
}
func (s *KVStore) Query(key any) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data[key.(string)], nil
}
func (s *KVStore) SaveSnapshot(ibabuza.StateMachineSnapshotContext, ibabuza.StateMachineSnapshotWriter) error { return nil }
func (s *KVStore) RestoreFromSnapshot(ibabuza.StateMachineSnapshotReader) error                             { return nil }
func (s *KVStore) Close() error                                                                             { return nil }

func main() {
	// 2. 以預設設定啟動 Raft
	r, _ := raft.NewDefaultBuilder().
		DataDir("/tmp/babuza").
		StateMachine(&KVStore{data: make(map[string]string)}).
		Start()

	// 3. 等待 leader 選舉完成
	time.Sleep(2 * time.Second)

	// 4. 透過 Raft 共識提交資料
	data, _ := json.Marshal(map[string]string{"Key": "hello", "Value": "world"})
	r.Propose(context.Background(), raft.ClientSession{}, data).WaitForApplyResult()

	fmt.Println("Data committed through Raft consensus!")
	r.Shutdown().Wait()
}
```

## 範例

| 範例 | 說明 |
|------|------|
| [Simple](./examples/simple/README.md) | 最小單節點 Raft 範例 |
| [KV Store](./examples/kvstore/README.md) | 有 REST API 的單 Raft 分散式 key-value store |
| [Distributed Lock](./examples/distlock/README.md) | lease-based distributed lock，支援 fencing token 和 wait queue |
| [Redis Cluster](./examples/redis-cluster/README.md) | Multi-raft Redis 相容分散式快取 |

## AI 輔助開發

使用 [babuza-skills](https://github.com/fanaujie/babuza-skills) 幫 AI coding assistant（Claude Code、Cursor、Aider）加入 Babuza 專屬知識，讓它更懂這個 codebase 的架構和用法。

## 文件

### 核心套件

| 套件 | 說明 |
|------|------|
| [ibabuza](./ibabuza/README.md) | 所有可插拔元件的核心 interface |
| [raft](./raft/README.md) | consensus layer、cluster bootstrap 和 Raft API |
| [pkg/builder](./pkg/builder/README.md) | 用 builder 組裝 Babuza component |

### 基礎設施套件

| 套件 | 說明 |
|------|------|
| [pkg/cluster](./pkg/cluster/README.md) | cluster membership 和 peer management |
| [pkg/transport](./pkg/transport/README.md) | 網路 transport 層（TCP、HTTP、gRPC） |
| [pkg/session](./pkg/session/README.md) | client session 管理，用來支援 idempotency |
| [pkg/snapshot](./pkg/snapshot/README.md) | snapshot 建立、儲存與還原 |
| [pkg/wal](./pkg/wal/README.md) | write-ahead log 實作 |

## 設定

### 元件類型

| 元件 | 可用類型 |
|------|---------|
| **Session** | `noop`、`expire`、`lru` |
| **Transport** | `tcp`、`tcp-memory`、`http`、`grpc` |
| **WAL** | `babuza-wal`、`etcd-wal`、`badger-wal`、`badger-wal-memory`、`pebble-wal`、`pebble-wal-memory` |
| **Snapshot** | `durable`、`volatile`、`s3` |
| **Metrics** | `otel`、`prometheus` |

## 實驗性 Multi-Raft

[raft/experimental](./raft/experimental/README.md) 在不修改 upstream etcd Raft 的情況下，實作 multi-Raft group 支援：

- **Coalesced Heartbeats** — 合併多個 Raft group 的 heartbeat，降低網路開銷
- **Shared WAL** — 多個 Raft group 共用同一個 WAL instance
- **Sharded Scheduling** — 分 shard 處理多個 Raft group 的工作

## 測試叢集框架

Babuza 提供 [testcluster](./test/testcluster/README.md)，用來測分散式系統常見的故障情境：

**支援的故障情境：**

| 情境 | 說明 |
|------|------|
| **Node disconnect** | 模擬單一節點網路故障 |
| **Network partition** | 把 cluster 切成隔離的群組 |
| **Leader failure** | 停止／重啟 leader 節點 |
| **Quorum loss** | 斷開多數節點，模擬失去 quorum |
| **Node restart** | 停止並重啟節點，驗證 WAL / snapshot recovery |
| **Disaster recovery** | 從失效 cluster 中做 standalone recovery |

## 貢獻

歡迎貢獻。送 PR 前請確認：

1. 新功能包含測試
2. 相關文件有同步更新

## 授權條款

Apache License 2.0，詳見 [LICENSE](./LICENSE)。

Copyright 2025 Chen Chunchieh
