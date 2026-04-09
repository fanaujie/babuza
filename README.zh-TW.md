# Babuza

**語言：** [English](./README.md) | 繁體中文 | [简体中文](./README.zh-CN.md)

一個基於 [etcd Raft](https://github.com/etcd-io/raft) 的 Go 框架，簡化建構分散式共識系統的複雜度。

## 為什麼選擇 Babuza？

使用 Raft 建構分散式系統相當困難。雖然 etcd 提供了經過實戰驗證的 Raft 實作，直接使用仍需投入大量心力：

- **Raft Ready 迴圈**：您必須實作一個 goroutine 來處理 `Ready()` 結構，按正確順序處理 entry、訊息、snapshot 以及 hard state 持久化
- **Storage 整合**：WAL 與 snapshot storage 必須謹慎協調——在傳送訊息之前先寫入 entry，並以原子方式套用 snapshot
- **網路層**：建構自己的 transport，在節點間傳送／接收 Raft 訊息，處理連線失敗與重試
- **State Machine 生命週期**：管理 snapshot 建立、還原與日誌壓縮，同時確保一致性
- **叢集成員管理**：實作新增／移除節點的協定，處理 joint consensus 與 learner 晉升

| 挑戰 | etcd Raft | Babuza |
|------|-----------|--------|
| **記憶體管理** | 所有日誌 entry 保存於記憶體中 | 基於索引的快取節省 94-99% 記憶體 |
| **網路 Transport** | 提供基礎 HTTP transport | 可插拔的 TCP/HTTP/gRPC transport |
| **WAL** | 提供 etcd WAL | 多種後端：native、Badger、Pebble |
| **Snapshot 傳輸** | 透過 HTTP 全量傳輸 | 壓縮分塊傳輸，支援速率限制 |
| **叢集操作** | 手動管理節點 | 內建新增／移除／轉移 API |
| **冪等性** | 由應用程式處理去重 | 基於 session 的 exactly-once 語意 |
| **可觀測性** | 自行建構 | 內建 Prometheus 與 OpenTelemetry |
| **災難復原** | 複雜的手動流程 | 單一指令獨立還原 |
| **整合測試** | 自行撰寫測試框架 | testcluster 支援故障注入與網路分區模擬 |

**Babuza 讓您專注於應用程式邏輯，而非 Raft 底層細節。**

## 記憶體效率

| Entry 數量 | 資料大小 | etcd 記憶體 | Babuza 記憶體 | 節省 |
|-----------|---------|------------|--------------|------|
| 100K      | 1 KB    | 102 MB     | 5.35 MB      | **94.8%** |
| 100K      | 10 KB   | 981 MB     | 5.35 MB      | **99.5%** |

詳見完整的[記憶體使用量基準測試報告](./docs/benchmarks/memory-usage-comparison.md)。

## 實驗性 Multi-Raft（無需修改 etcd Raft）

[experimental](./raft/experimental/README.md) 套件實作了 multi-Raft group 支援：

- **Coalesced Heartbeats** — 合併來自多個 Raft group 的心跳，減少網路負擔
- **Shared WAL** — 多個 Raft group 共用單一 WAL 實例
- **Sharded Scheduling** — 跨多個 Raft group 的高效處理排程
- 所有功能均在不修改 etcd Raft 函式庫的前提下實作

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

// 1. 實作您的 state machine
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

	fmt.Println("資料已透過 Raft 共識成功提交！")
	r.Shutdown().Wait()
}
```

## 範例

| 範例 | 說明 |
|------|------|
| [Simple](./examples/simple/README.md) | 最小化單節點 Raft 範例 |
| [KV Store](./examples/kvstore/README.md) | 具備 REST API 的單 Raft 分散式 key-value store |
| [Distributed Lock](./examples/distlock/README.md) | 基於租約的分散式鎖，支援 fencing token 與等待佇列 |
| [Redis Cluster](./examples/redis-cluster/README.md) | Multi-raft Redis 相容分散式快取 |

## AI 輔助開發

使用 [babuza-skills](https://github.com/fanaujie/babuza-skills) 為 AI 程式碼助理（Claude Code、Cursor、Aider）加入 Babuza 專屬知識，提升程式碼生成與解說能力。

## 元件文件

### 核心套件

| 套件 | 說明 |
|------|------|
| [ibabuza](./ibabuza/README.md) | 所有可插拔元件的核心 interface |
| [raft](./raft/README.md) | 共識層、叢集引導啟動與 Raft API |
| [raft/experimental](./raft/experimental/README.md) | 具備 coalesced heartbeat 與 shared WAL 的 multi-Raft（實驗性） |
| [pkg/builder](./pkg/builder/README.md) | 易於組裝的元件 builder 模式 |

### 基礎設施套件

| 套件 | 說明 |
|------|------|
| [pkg/cluster](./pkg/cluster/README.md) | 叢集成員管理與節點管理 |
| [pkg/transport](./pkg/transport/README.md) | 網路 transport 層（TCP、HTTP、gRPC） |
| [pkg/session](./pkg/session/README.md) | 用戶端 session 管理，確保冪等性 |
| [pkg/snapshot](./pkg/snapshot/README.md) | Snapshot 建立、儲存與還原 |
| [pkg/wal](./pkg/wal/README.md) | Write-ahead log 實作 |

## 設定

### 元件類型

| 元件 | 可用類型 |
|------|---------|
| **Session** | `noop`、`expire`、`lru` |
| **Transport** | `tcp`、`tcp-memory`、`http`、`grpc` |
| **WAL** | `babuza-wal`、`etcd-wal`、`badger-wal`、`badger-wal-memory`、`pebble-wal`、`pebble-wal-memory` |
| **Snapshot** | `durable`、`volatile`、`s3` |
| **Metrics** | `otel`、`prometheus` |


## 測試叢集框架

Babuza 提供 [testcluster](./test/testcluster/README.md) 框架，用於測試分散式系統故障情境：

**支援的故障情境：**

| 情境 | 說明 |
|------|------|
| **節點斷線** | 模擬單一節點網路故障 |
| **網路分區** | 將叢集分割為隔離的群組 |
| **Leader 故障** | 停止／重啟 leader 節點 |
| **法定人數喪失** | 斷開多數節點的連線 |
| **節點重啟** | 停止並重啟，含 WAL／snapshot 復原 |
| **災難復原** | 從遺失的叢集中獨立還原 |



## 貢獻

歡迎貢獻！請確保：

1. 新功能包含測試
2. 視需要更新文件

## 授權條款

Apache License 2.0，詳見 [LICENSE](./LICENSE)。

Copyright 2025 Chen Chunchieh

