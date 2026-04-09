# Babuza

**语言：** [English](./README.md) | [繁體中文](./README.zh-TW.md) | 简体中文

一个基于 [etcd Raft](https://github.com/etcd-io/raft) 的 Go 框架，简化构建分布式共识系统的复杂度。

## 为什么选择 Babuza？

使用 Raft 构建分布式系统相当困难。虽然 etcd 提供了经过实战验证的 Raft 实现，直接使用仍需投入大量精力：

- **Raft Ready 循环**：您必须实现一个 goroutine 来处理 `Ready()` 结构，按正确顺序处理 entry、消息、snapshot 以及 hard state 持久化
- **Storage 集成**：WAL 与 snapshot storage 必须谨慎协调——在发送消息之前先写入 entry，并以原子方式应用 snapshot
- **网络层**：构建自己的 transport，在节点间发送／接收 Raft 消息，处理连接失败与重试
- **State Machine 生命周期**：管理 snapshot 创建、恢复与日志压缩，同时确保一致性
- **集群成员管理**：实现添加／移除节点的协议，处理 joint consensus 与 learner 晋升

| 挑战 | etcd Raft | Babuza |
|------|-----------|--------|
| **内存管理** | 所有日志 entry 保存于内存中 | 基于索引的缓存节省 94-99% 内存 |
| **网络 Transport** | 提供基础 HTTP transport | 可插拔的 TCP/HTTP/gRPC transport |
| **WAL** | 提供 etcd WAL | 多种后端：native、Badger、Pebble |
| **Snapshot 传输** | 通过 HTTP 全量传输 | 压缩分块传输，支持速率限制 |
| **集群操作** | 手动管理节点 | 内置添加／移除／转移 API |
| **幂等性** | 由应用程序处理去重 | 基于 session 的 exactly-once 语义 |
| **可观测性** | 自行构建 | 内置 Prometheus 与 OpenTelemetry |
| **灾难恢复** | 复杂的手动流程 | 单一命令独立恢复 |
| **集成测试** | 自行编写测试框架 | testcluster 支持故障注入与网络分区模拟 |

**Babuza 让您专注于应用程序逻辑，而非 Raft 底层细节。**

## 内存效率

| Entry 数量 | 数据大小 | etcd 内存 | Babuza 内存 | 节省 |
|-----------|---------|----------|------------|------|
| 100K      | 1 KB    | 102 MB   | 5.35 MB    | **94.8%** |
| 100K      | 10 KB   | 981 MB   | 5.35 MB    | **99.5%** |

详见完整的[内存使用量基准测试报告](./docs/benchmarks/memory-usage-comparison.md)。

## 实验性 Multi-Raft（无需修改 etcd Raft）

[experimental](./raft/experimental/README.md) 包实现了 multi-Raft group 支持：

- **Coalesced Heartbeats** — 合并来自多个 Raft group 的心跳，减少网络负担
- **Shared WAL** — 多个 Raft group 共用单一 WAL 实例
- **Sharded Scheduling** — 跨多个 Raft group 的高效处理调度
- 所有功能均在不修改 etcd Raft 库的前提下实现

## 架构

![architecture](images/babuza_architecture.svg)


## 快速开始

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

// 1. 实现您的 state machine
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
	// 2. 以默认设置启动 Raft
	r, _ := raft.NewDefaultBuilder().
		DataDir("/tmp/babuza").
		StateMachine(&KVStore{data: make(map[string]string)}).
		Start()

	// 3. 等待 leader 选举完成
	time.Sleep(2 * time.Second)

	// 4. 通过 Raft 共识提交数据
	data, _ := json.Marshal(map[string]string{"Key": "hello", "Value": "world"})
	r.Propose(context.Background(), raft.ClientSession{}, data).WaitForApplyResult()

	fmt.Println("数据已通过 Raft 共识成功提交！")
	r.Shutdown().Wait()
}
```

## 示例

| 示例 | 说明 |
|------|------|
| [Simple](./examples/simple/README.md) | 最小化单节点 Raft 示例 |
| [KV Store](./examples/kvstore/README.md) | 具备 REST API 的单 Raft 分布式 key-value store |
| [Distributed Lock](./examples/distlock/README.md) | 基于租约的分布式锁，支持 fencing token 与等待队列 |
| [Redis Cluster](./examples/redis-cluster/README.md) | Multi-raft Redis 兼容分布式缓存 |

## AI 辅助开发

使用 [babuza-skills](https://github.com/fanaujie/babuza-skills) 为 AI 代码助手（Claude Code、Cursor、Aider）加入 Babuza 专属知识，提升代码生成与解释能力。

## 组件文档

### 核心包

| 包 | 说明 |
|----|------|
| [ibabuza](./ibabuza/README.md) | 所有可插拔组件的核心 interface |
| [raft](./raft/README.md) | 共识层、集群引导启动与 Raft API |
| [raft/experimental](./raft/experimental/README.md) | 具备 coalesced heartbeat 与 shared WAL 的 multi-Raft（实验性） |
| [pkg/builder](./pkg/builder/README.md) | 易于组装的组件 builder 模式 |

### 基础设施包

| 包 | 说明 |
|----|------|
| [pkg/cluster](./pkg/cluster/README.md) | 集群成员管理与节点管理 |
| [pkg/transport](./pkg/transport/README.md) | 网络 transport 层（TCP、HTTP、gRPC） |
| [pkg/session](./pkg/session/README.md) | 客户端 session 管理，确保幂等性 |
| [pkg/snapshot](./pkg/snapshot/README.md) | Snapshot 创建、存储与恢复 |
| [pkg/wal](./pkg/wal/README.md) | Write-ahead log 实现 |

## 配置

### 组件类型

| 组件 | 可用类型 |
|------|---------|
| **Session** | `noop`、`expire`、`lru` |
| **Transport** | `tcp`、`tcp-memory`、`http`、`grpc` |
| **WAL** | `babuza-wal`、`etcd-wal`、`badger-wal`、`badger-wal-memory`、`pebble-wal`、`pebble-wal-memory` |
| **Snapshot** | `durable`、`volatile`、`s3` |
| **Metrics** | `otel`、`prometheus` |


## 测试集群框架

Babuza 提供 [testcluster](./test/testcluster/README.md) 框架，用于测试分布式系统故障场景：

**支持的故障场景：**

| 场景 | 说明 |
|------|------|
| **节点断开** | 模拟单一节点网络故障 |
| **网络分区** | 将集群分割为隔离的分组 |
| **Leader 故障** | 停止／重启 leader 节点 |
| **法定人数丢失** | 断开多数节点的连接 |
| **节点重启** | 停止并重启，含 WAL／snapshot 恢复 |
| **灾难恢复** | 从丢失的集群中独立恢复 |



## 贡献

欢迎贡献！请确保：

1. 新功能包含测试
2. 根据需要更新文档

## 授权条款

Apache License 2.0，详见 [LICENSE](./LICENSE)。

Copyright 2025 Chen Chunchieh

