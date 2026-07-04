# IFP 工程实现规格说明书

> 基于 IFP_CORE.md v3.1 协议文档  
> 日期: 2026-05-27

---

## 目录

1. [系统架构总览](#1-系统架构总览)
2. [功能需求表](#2-功能需求表)
3. [工程实现需求表](#3-工程实现需求表)
4. [模块 API 文档](#4-模块-api-文档)
   - [4.1 控制面 API](#41-控制面-api)
   - [4.2 IFP SDK API](#42-ifp-sdk-api)
   - [4.3 外部进程通信协议](#43-外部进程通信协议)
   - [4.4 控制通道协议](#44-控制通道协议)
   - [4.5 Bootstrap 协议](#45-bootstrap-协议)
5. [状态机与流程规范](#5-状态机与流程规范)
   - [5.1 服务状态机](#51-服务状态机)
   - [5.2 消息状态机](#52-消息状态机)
   - [5.3 自演进闭环流程](#53-自演进闭环流程)

---

## 1. 系统架构总览

```
┌─────────────────────────────────────────────────────────────────────┐
│                    IFP Runtime（单一进程）                             │
│                                                                      │
│  ┌────────────────────────── 控制面 ──────────────────────────────┐  │
│  │                                                                  │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────┐  │  │
│  │  │ 服务组    │  │ 生命周期  │  │ 审计收集  │  │ Bootstrap      │  │  │
│  │  │ 管理器    │  │ 管理器    │  │ 器       │  │ Server         │  │  │
│  │  └──────────┘  └──────────┘  └──────────┘  └────────────────┘  │  │
│  │                                                                  │  │
│  │  对外接口: gRPC / HTTP                                            │  │
│  └──────────────────────────┬───────────────────────────────────────┘  │
│                             │                                         │
│              控制通道（进程内方法调用 / TCP / Unix socket）              │
│                             │                                         │
│  ┌────────────────────────── 数据面 ───────────────────────────────┐  │
│  │                                                                  │  │
│  │  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌──────────────┐   │  │
│  │  │ 服务 A   │   │ 服务 B   │   │ 服务 C   │   │ 外部进程 D    │   │  │
│  │  │(goroutine)│  │(goroutine)│  │(goroutine)│  │(stdin/stdout)│   │  │
│  │  │          │   │          │   │          │   │ JSON Lines    │   │  │
│  │  │ IFP SDK  │   │ IFP SDK  │   │ IFP SDK  │   │ Runtime pipe  │   │  │
│  │  └────┬─────┘   └────┬─────┘   └────┬─────┘   └──────┬───────┘   │  │
│  │       │               │               │               │           │  │
│  │       └─────── 消息路由器（Go channel / 可插拔传输）────┘           │  │
│  │                     (数据面 — 消息传递)                             │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

**架构要点：**

- IFP Runtime 是单一 Go 进程，内嵌控制面和数据面
- 服务默认为 goroutine，在 Runtime 进程内执行（`in_process` 隔离）
- 外部进程通过 `separate_process` 隔离模式启动，Runtime 通过 stdin/stdout 以 JSON Lines 协议与之通信，无需额外桥接进程
- 不存在 libifp.so 共享库注入、共享内存映射、特权辅助进程

---

## 2. 功能需求表

### 2.1 控制面 (Control Plane)

| ID | 功能 | 描述 | 优先级 |
|----|------|------|--------|
| CP-001 | 服务组加载与校验 | 解析 `.group.toml` 文件，校验服务定义、通道声明、身份配置的完整性和合法性 | P0 |
| CP-002 | spawn 服务实例 | 根据 ServiceSpec 创建服务（默认启动 goroutine，可选启动独立 OS 进程），注入环境变量和身份凭证 | P0 |
| CP-003 | terminate 服务实例 | 向指定服务发送 TERMINATE（优雅退出）或 KILL（强制终止）控制消息 | P0 |
| CP-004 | reap 服务实例 | 回收已终止服务的资源（通道绑定、goroutine/进程句柄），返回 ExitStatus | P0 |
| CP-005 | 通道静态建立 | 在服务启动前，根据服务组 channels 声明创建消息通道 | P0 |
| CP-006 | isolation 隔离 | 在 spawn 时根据 ServiceSpec.isolation 选择 `in_process`（goroutine）或 `separate_process`（独立 OS 进程） | P1 |
| CP-007 | identity 绑定与注入 | 创建 identity，通过环境变量注入到服务 | P0 |
| CP-008 | 审计事件收集 | 通过控制通道接收 SDK 上报的 AuditBatch，持久化存储 | P1 |
| CP-009 | 策略下发 (PolicyPush) | 增量更新指定服务的 access_check 规则 | P1 |
| CP-010 | 配置热更新 (ConfigReload) | 向指定服务发送新的配置段，触发运行时配置重载 | P1 |
| CP-011 | 服务组热更新 | 运行时增量修改服务组（新增/移除服务、修改配置、修改通道），不影响未变更服务 | P2 |
| CP-012 | 死信汇集 | 接收并存储进入死信的消息，保留原始消息体和失败原因 | P2 |
| CP-013 | 状态监控 | 通过控制通道接收 StatusReport，维护所有服务的实时状态视图 | P1 |
| CP-014 | Bootstrap Server | 监听 TCP/Unix socket，接收外部进程的绑定请求，执行身份验证，返回通道连接信息 | P1 |

### 2.2 IFP SDK (数据面)

IFP SDK 是 Go library，服务通过 `import "ifp/sdk"` 引入，直接调用 API。

| ID | 功能 | 描述 | 优先级 |
|----|------|------|--------|
| DA-001 | 初始化 | 从环境变量读取 service_id、通道映射；从凭证加载 identity；向 Runtime 注册并建立通道连接 | P0 |
| DA-002 | 消息发送 | 执行 access_check，Permit 时写入目标通道，Deny 时返回 ACCESS_DENIED | P0 |
| DA-003 | 消息接收 | 多路复用等待输入通道消息，派发给服务处理函数 | P0 |
| DA-004 | access_check 策略引擎 | 基于哈希表查找，O(1) 判定发送方→目标方→通道是否被允许；规则为空时默认拒绝 | P0 |
| DA-005 | privilege_transition | 不可逆地从本地能力掩码中移除指定权限，后续 access_check 基于降权后掩码判定 | P1 |
| DA-006 | signal_notify 处理 | 接收并处理 PAUSE/RESUME/RELOAD/CUSTOM 控制消息 | P0 |
| DA-007 | 背压检测与通知 | 监控输入通道缓冲使用率，超阈值时向上游发送 Backpressure 通知 | P2 |
| DA-008 | 审计事件生成 | 在 access_check Deny、privilege drop、channel error 等事件发生时生成审计事件 | P1 |
| DA-009 | 审计事件批量上报 | 通过控制通道批量上报审计事件（缓冲区满或定时触发） | P1 |
| DA-010 | 子请求管理 | 支持消息级别的 AWAITING_SUB 状态，管理 parent_trace 和 sequence，超时检测与子请求取消 | P1 |
| DA-011 | 流式消息 | 支持同一 message_id 的多条分片消息，按到达顺序拼接，最后一条标记 stream_final | P2 |
| DA-012 | 死信转发 | 将 TTL 过期、目标不可达的消息转发到死信汇集服务 | P2 |
| DA-013 | channel_create | 运行时动态创建通道，自动注册到白名单 | P2 |
| DA-014 | dynamic_spawn 发起 | 校验 content_hash，将请求转发给 Runtime 执行 | P2 |
| DA-015 | 控制通道客户端 | 建立并维护与控制面的连接，处理 PolicyPush/ConfigReload 下行消息，发送 AuditBatch/StatusReport 上行消息 | P0 |
| DA-016 | 优雅退出 | 收到 TERMINATE 后完成当前消息，发送最后一批审计事件并等待 ACK，关闭控制通道连接 | P0 |
| DA-017 | 故障隔离 | SDK 内部 panic 通过 recover 捕获，仅终止当前服务 goroutine，不影响其他服务 | P0 |

### 2.3 服务组管理

| ID | 功能 | 描述 | 优先级 |
|----|------|------|--------|
| SG-001 | TOML 解析 | 解析 `.group.toml` 文件中的 group 元信息、services 定义、channels 声明 | P0 |
| SG-002 | 配置校验 | 校验服务定义的完整性（type/config）、通道引用的服务是否已定义、身份配置合法性 | P0 |
| SG-003 | 服务类型注册 | 支持 llm / tool / router / ui / admin / registry / custom::name 类型 | P0 |
| SG-004 | 自演进增量更新 | 支持运行时新增/移除服务、修改配置（RELOAD）、修改通道 | P2 |
| SG-005 | 最大服务数限制 | admin 服务的 max_services 配置，防止无限扩张 | P2 |
| SG-006 | 自演进审批 | auto_approve 控制自动审批或人工审批 | P2 |

---

## 3. 工程实现需求表

### 3.1 技术栈建议

| 模块 | 语言 | 理由 |
|------|------|------|
| IFP Runtime（控制面 + 数据面） | Go | 高并发（goroutine）、单一二进制部署、跨平台编译、丰富的标准库 |
| IFP SDK | Go | 与 Runtime 同语言，直接 library 引入，零 FFI 开销 |

### 3.2 基础设施需求

| ID | 需求 | 描述 | 优先级 |
|----|------|------|--------|
| INF-001 | 消息通道传输 | 进程内：Go channel / lock-free queue；跨进程：可插拔传输接口（TCP、Unix socket、命名管道） | P0 |
| INF-002 | 控制通道传输 | 进程内：直接方法调用；跨进程：TCP/Unix socket，JSON 行协议 | P0 |
| INF-003 | 异步事件循环 | 基于 Go 的 goroutine + channel + select 多路复用，包含定时器（TTL/sub_request_timeout） | P0 |
| INF-004 | 凭证管理 | 安全的进程内凭证存储，spawn 时注入，服务退出后清理 | P0 |
| INF-005 | 审计事件持久化 | 控制面将审计事件写入持久化存储（如 SQLite/PostgreSQL），支持按时间/服务/事件类型查询 | P1 |
| INF-006 | 死信队列 | 持久化的死信消息存储，支持重放和人工检查 | P2 |

### 3.3 可靠性需求

| ID | 需求 | 描述 | 优先级 |
|----|------|------|--------|
| REL-001 | Runtime 高可用 | Runtime 进程崩溃后重启，通过状态恢复（重读服务组定义、重建通道）恢复系统 | P1 |
| REL-002 | 服务隔离 | 一个服务的 panic 不影响其他服务和 Runtime（goroutine 级别 recover） | P0 |
| REL-003 | 审计事件不丢服务 | 审计缓冲区满时丢弃事件但计数器递增（非阻塞），不因审计问题阻塞消息收发 | P1 |
| REL-004 | access_check 降级 | 策略引擎异常时降级为 Deny（默认拒绝所有），防止绕过安全检查 | P1 |
| REL-005 | 控制通道解耦 | 控制通道中断时数据面通道继续运行，仅暂停策略更新和审计上报 | P1 |
| REL-006 | 优雅终止 | 所有服务支持 TERMINATE 优雅退出（完成当前消息后退出），KILL 强制终止 | P0 |
| REL-007 | 资源泄漏防护 | spawn 服务的资源在 reap 后保证回收；bind 服务的资源在控制通道断开后触发回收 | P0 |

### 3.4 性能需求

| ID | 需求 | 描述 | 优先级 |
|----|------|------|--------|
| PERF-001 | 进程内消息延迟 < 1μs | Go channel 直接传递指针，无序列化开销（同进程内） | P0 |
| PERF-002 | SDK 初始化延迟 < 1ms | 轻量初始化：读取环境变量 + 注册到 Runtime | P1 |
| PERF-003 | access_check O(1) | 哈希表查找，不随规则数增长 | P1 |
| PERF-004 | SDK 内存 < 1MB/服务 | 通道引用 + identity 状态 + 审计缓冲区 | P1 |

### 3.5 安全需求

| ID | 需求 | 描述 | 优先级 |
|----|------|------|--------|
| SEC-001 | identity 不可篡改 | 身份在 spawn/bind 时一次性绑定，运行时不可更改 | P0 |
| SEC-002 | privilege 不可逆 | privilege_drop 执行后无法在本次生命周期内恢复 | P1 |
| SEC-003 | 默认拒绝 | access_check 规则为空时默认返回 Deny | P0 |
| SEC-004 | dynamic_spawn 哈希校验 | 可执行内容必须通过 SHA256 校验 | P1 |
| SEC-005 | Bootstrap 身份验证 | HMAC 挑战-响应（必须）→ mTLS（可选） | P1 |
| SEC-006 | 最小权限原则 | 服务以分配的能力掩码运行，不持额外权限 | P0 |
| SEC-007 | 控制通道认证 | 控制通道连接需验证对端身份（进程内：直接调用；跨进程：HMAC/mTLS） | P0 |

---

## 4. 模块 API 文档

### 4.1 控制面 API

控制面提供 gRPC 或 HTTP 接口，供外部系统（CLI、admin service、监控面板）调用。所有接口为同步请求-响应模型。

#### 4.1.1 服务组管理

##### LoadGroup

加载并校验服务组定义文件。

```
POST /v1/groups/load
```

**请求：**

```json
{
  "group_file": "/etc/ifp/groups/my-pipeline.group.toml",
  "auto_start": true
}
```

**响应：**

```json
{
  "group_id": "group-7f3a1b",
  "name": "my-pipeline",
  "version": "1.0",
  "services": [
    {"service_id": "agent-001", "status": "PENDING"},
    {"service_id": "tool-001", "status": "PENDING"}
  ],
  "channels": [
    {"source": "agent-001:tool_call", "targets": ["tool-001:in"]}
  ],
  "warnings": []
}
```

**错误码：**

| 错误码 | 含义 |
|--------|------|
| `PARSE_ERROR` | TOML 语法错误 |
| `VALIDATION_ERROR` | 服务/通道定义不合法（含校验详情列表） |
| `GROUP_ALREADY_LOADED` | 同名服务组已加载 |

---

##### ReloadGroup

运行时增量更新服务组。

```
PUT /v1/groups/{group_id}/reload
```

**请求：**

```json
{
  "diff": {
    "added_services": [ { /* ServiceSpec */ } ],
    "removed_services": ["tool-001"],
    "modified_configs": { "agent-001": { "temperature": 0.9 } },
    "modified_channels": { "agent:tool_call": ["tool_v2:in"] }
  },
  "force": false
}
```

**响应：**

```json
{
  "accepted": true,
  "affected_services": ["tool-001", "agent-001"],
  "unchanged_services": ["router-001"],
  "details": [
    {"action": "terminate", "service_id": "tool-001", "reason": "removed from group"},
    {"action": "spawn", "service_id": "tool_v2", "reason": "added to group"},
    {"action": "reload", "service_id": "agent-001", "reason": "config modified"}
  ]
}
```

**错误码：**

| 错误码 | 含义 |
|--------|------|
| `GROUP_NOT_FOUND` | 服务组不存在 |
| `MAX_SERVICES_EXCEEDED` | 超过 max_services 限制 |
| `CHANNEL_CONFLICT` | 新通道与现有通道冲突 |

---

##### UnloadGroup

卸载整个服务组（terminate 所有服务后回收）。

```
DELETE /v1/groups/{group_id}
```

**请求：** 空（可选 `force: bool` 强制终止）

**响应：**

```json
{
  "group_id": "group-7f3a1b",
  "terminated_services": 3,
  "errors": []
}
```

---

##### GetGroupStatus

获取服务组及所有服务的运行状态。

```
GET /v1/groups/{group_id}/status
```

**响应：**

```json
{
  "group_id": "group-7f3a1b",
  "name": "my-pipeline",
  "uptime_seconds": 3600,
  "services": [
    {
      "service_id": "agent-001",
      "type": "llm",
      "status": "RUNNING",
      "isolation": "in_process",
      "uptime_ms": 3598000,
      "channel_stats": {
        "in": {"msg_total": 150, "msg_rate": 0.04, "queue_depth": 2},
        "out": {"msg_total": 140, "msg_rate": 0.04, "queue_depth": 0}
      }
    }
  ],
  "dead_letter_count": 0
}
```

---

#### 4.1.2 服务生命周期

##### SpawnService

创建新服务实例。

```
POST /v1/services
```

**请求：**

```json
{
  "group_id": "group-7f3a1b",
  "service_spec": {
    "service_id": "custom-tool",
    "type": "tool",
    "command": "my_tool",
    "args": ["--mode", "production"],
    "env": { "TOOL_MODE": "production" },
    "isolation": {
      "mode": "in_process"
    },
    "identity": {
      "principal": "custom-tool-001",
      "attributes": { "role": "worker", "tier": "backend" }
    },
    "privileges": ["tool_exec", "channel_send"],
    "config": { "key": "value" }
  }
}
```

**响应：**

```json
{
  "service_id": "custom-tool",
  "status": "READY",
  "channel_bindings": {
    "in": "ch-in-7f3a1b",
    "out": "ch-out-7f3a1b"
  }
}
```

**错误码：**

| 错误码 | 含义 |
|--------|------|
| `SERVICE_ALREADY_EXISTS` | service_id 已存在 |
| `INVALID_SPEC` | ServiceSpec 定义不合法 |
| `SPAWN_FAILED` | 服务创建失败 |
| `INIT_TIMEOUT` | 服务在超时时间内未发送 READY |

---

##### TerminateService

停止服务实例。

```
POST /v1/services/{service_id}/terminate
```

**请求：**

```json
{
  "signal": "TERMINATE",
  "grace_period_ms": 5000
}
```

**响应：**

```json
{
  "service_id": "custom-tool",
  "previous_status": "RUNNING",
  "status": "STOPPED",
  "signal_sent": "TERMINATE",
  "exit_code": 0,
  "exit_reason": "normal"
}
```

**signal 取值：** `TERMINATE` | `KILL`

---

##### ReapService

回收已终止服务的资源。

```
DELETE /v1/services/{service_id}
```

**请求：** 空

**响应：**

```json
{
  "service_id": "custom-tool",
  "exit_status": {
    "exit_code": 0,
    "reason": "normal"
  },
  "freed_resources": {
    "channels_released": 2,
    "goroutine_stopped": true
  }
}
```

**错误码：**

| 错误码 | 含义 |
|--------|------|
| `SERVICE_NOT_STOPPED` | 服务未处于 STOPPED 状态，需先 terminate |
| `SERVICE_NOT_FOUND` | 服务不存在 |

---

##### SignalService

向服务发送流控信号。

```
POST /v1/services/{service_id}/signal
```

**请求：**

```json
{
  "signal": "PAUSE",
  "reason": "memory pressure"
}
```

**响应：**

```json
{
  "service_id": "custom-tool",
  "signal": "PAUSE",
  "current_status": "PAUSED"
}
```

**可用 signal：** `PAUSE` | `RESUME` | `RELOAD` | `CUSTOM`

---

#### 4.1.3 控制面操作

##### PolicyPush

向指定服务增量推送访问控制规则。

```
POST /v1/services/{service_id}/policy
```

**请求：**

```json
{
  "rules": [
    {
      "op": "add",
      "source": "agent-001",
      "channel": "out",
      "target": "tool-001",
      "permit": true
    },
    {
      "op": "remove",
      "source": "agent-001",
      "channel": "tool_call",
      "target": "deprecated-tool"
    }
  ]
}
```

**响应：**

```json
{
  "accepted": true,
  "rules_processed": 2,
  "current_rule_count": 15
}
```

---

##### ConfigReload

向指定服务发送配置热更新。

```
POST /v1/services/{service_id}/config
```

**请求：**

```json
{
  "config": {
    "temperature": 0.3,
    "max_tokens": 4096
  }
}
```

**响应：**

```json
{
  "acknowledged": true,
  "applied_at": "2026-05-27T10:30:00Z"
}
```

---

##### GetAuditEvents

查询审计事件。

```
GET /v1/audit?group_id={group_id}&from={timestamp}&to={timestamp}&event_type={type}&limit={n}
```

**查询参数：**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `group_id` | string | 否 | 按服务组过滤 |
| `from` | timestamp | 否 | 起始时间 |
| `to` | timestamp | 否 | 结束时间 |
| `event_type` | string | 否 | 事件类型过滤 |
| `limit` | int | 否 | 返回条数上限（默认 100，最大 1000） |
| `cursor` | string | 否 | 分页游标 |

**响应：**

```json
{
  "events": [
    {
      "event_id": "evt-a1b2c3",
      "event_type": "AccessViolation",
      "timestamp": "2026-05-27T10:15:00Z",
      "group_id": "group-7f3a1b",
      "service_id": "agent-001",
      "data": {
        "source": "agent-001",
        "target": "forbidden-tool",
        "channel": "tool_call",
        "deny_reason": "not in whitelist"
      }
    }
  ],
  "next_cursor": "evt-d4e5f6",
  "total_count": 42
}
```

---

##### ListDeadLetters

查询死信消息。

```
GET /v1/dead-letters?group_id={group_id}&reason={reason}&from={timestamp}&limit={n}
```

**响应：**

```json
{
  "messages": [
    {
      "dead_letter_id": "dl-001",
      "original_message": {
        "trace_id": "trace-xyz",
        "message_id": "msg-123",
        "content_type": "application/json",
        "priority": 128
      },
      "reason": "no_route",
      "timestamp": "2026-05-27T10:15:00Z",
      "context": {
        "source_service": "agent-001",
        "target_service": "deleted-tool",
        "channel": "tool_call"
      }
    }
  ],
  "next_cursor": "dl-050",
  "total_count": 5
}
```

---

#### 4.1.4 Bootstrap Server (由控制面内部暴露)

控制面监听 TCP/Unix socket 接收外部进程的绑定请求。协议细节见 [4.5 Bootstrap 协议](#45-bootstrap-协议)。

**监听地址：** 由 Runtime 配置指定（默认 `127.0.0.1:9740`）

---

### 4.2 IFP SDK API

IFP SDK 是 Go library，服务通过 `import "github.com/ifp/sdk"` 引入。

#### 4.2.1 初始化与销毁

##### NewClient

创建 IFP 客户端实例。

```go
/**
 * NewClient 创建 IFP SDK 客户端。
 *
 * 从环境变量读取 service_id、通道映射；
 * 向 Runtime 注册并建立通道连接；加载身份凭证。
 *
 * 环境变量：
 *   IFP_SERVICE_ID      服务标识符
 *   IFP_RUNTIME_ADDR    Runtime 地址（进程内为空）
 *   IFP_CHANNEL_MAP     通道映射 JSON
 *   IFP_CREDENTIAL      身份凭证（Base64 编码）
 */
func NewClient() (*Client, error)
```

**环境变量：**

| 变量名 | 说明 | 示例 |
|--------|------|------|
| `IFP_SERVICE_ID` | 服务标识符 | `agent-001` |
| `IFP_RUNTIME_ADDR` | Runtime 地址（进程内服务为空） | `127.0.0.1:9740` |
| `IFP_CHANNEL_MAP` | 通道映射 JSON | `{"in":"ch-in-7f3a","out":"ch-out-7f3a"}` |
| `IFP_CREDENTIAL` | 身份凭证（Base64） | `eyJwcmluY2lwYWwiOi...` |

---

##### Close

关闭客户端。

```go
/**
 * Close 销毁 IFP 客户端。
 *
 * 发送最后一批审计事件并等待 ACK；
 * 关闭控制通道连接；
 * 释放通道资源。
 */
func (c *Client) Close() error
```

---

#### 4.2.2 消息收发

##### Send

向指定输出通道发送消息。

```go
/**
 * Send 向输出通道发送消息。
 *
 * 发送前自动执行 access_check(source, target, channel)。
 * Permit → 写入目标通道；Deny → 返回 ErrAccessDenied。
 */
func (c *Client) Send(port string, content []byte, opts *SendOpts) error
```

**SendOpts 结构：**

```go
type SendOpts struct {
    ContentType     string        // 载荷 MIME 类型，默认 "application/octet-stream"
    Priority        uint8         // 优先级 0-255，默认 128
    TTL             time.Duration // TTL，0 表示使用默认值
    TraceID         string        // 追踪 ID，空则由 SDK 自动生成
    InResponseTo    string        // 被回复的消息 ID，空表示不是回复
    ParentMessageID string        // 父消息 ID（子请求），空表示顶层消息
    ParentSequence  uint32        // 子请求序号，从 1 开始
    Stream          bool          // 是否流式消息
    StreamFinal     bool          // 是否流式结束
}
```

**发送错误：**

| 错误 | 含义 |
|------|------|
| `ErrAccessDenied` | access_check 返回 Deny |
| `ErrChannelFull` | 目标通道缓冲已满 |
| `ErrChannelNotFound` | 指定端口不存在 |
| `ErrChannelBroken` | 通道已断开 |
| `ErrInvalidArg` | opts 参数非法 |

---

##### Recv

从输入通道接收消息（阻塞）。

```go
/**
 * Recv 从输入通道接收消息。
 *
 * ctx 用于超时和取消控制。
 */
func (c *Client) Recv(ctx context.Context) (*Message, error)
```

**Message 结构：**

```go
type Message struct {
    MessageID   string          // UUID 字符串
    TraceID     string
    Content     []byte          // 载荷
    ContentType string
    Priority    uint8
    TTL         time.Duration
    CreatedAt   time.Time
    InResponseTo string
    ParentTrace struct {
        ParentMessageID string
        Sequence        uint32
    }
    Stream      bool
    StreamFinal bool
    SourcePort  string          // 接收消息的端口名
}
```

---

##### RecvPoll

非阻塞轮询是否有可用消息。

```go
/**
 * RecvPoll 检查指定端口是否有消息可接收。
 *
 * portName 为空表示任意端口。
 * 返回 true 表示有消息等待。
 */
func (c *Client) RecvPoll(portName string) bool
```

---

#### 4.2.3 权限管理

##### PrivilegeDrop

不可逆地放弃指定权限。

```go
/**
 * PrivilegeDrop 放弃一项权限。不可逆。
 *
 * 执行后本地能力掩码更新，后续所有 access_check 基于降权后的掩码判定。
 */
func (c *Client) PrivilegeDrop(privilege string) error
```

**错误：**

| 错误 | 含义 |
|------|------|
| `ErrPrivNotHeld` | 当前未持有该权限（已放弃或从未授予） |
| `ErrInvalidPriv` | 权限名称不在合法集合中 |

---

##### PrivilegeHas

查询是否持有指定权限。

```go
/**
 * PrivilegeHas 检查当前是否持有指定权限。
 */
func (c *Client) PrivilegeHas(privilege string) bool
```

---

#### 4.2.4 通道管理

##### ChannelCreate

运行时动态创建通道。

```go
/**
 * ChannelCreate 动态创建一条新通道。
 *
 * 新通道自动注册到 access_check 白名单。
 * 默认缓冲 256 条消息。
 */
func (c *Client) ChannelCreate(sourcePort, targetService, targetPort string) (channelID string, err error)
```

---

##### ChannelStats

查询通道统计信息。

```go
/**
 * ChannelStats 查询指定端口的通道统计信息。
 */
func (c *Client) ChannelStats(portName string) (*ChannelStats, error)

type ChannelStats struct {
    MsgSent       uint64
    MsgReceived   uint64
    BytesSent     uint64
    BytesReceived uint64
    QueueDepth    uint32
    QueueCapacity uint32
    UsagePercent  float64   // QueueDepth / QueueCapacity * 100
}
```

---

##### Backpressure

数据面背压通知结构。当输入通道缓冲使用率超过阈值时，SDK 自动向上游发送方发出此通知。

```go
/**
 * Backpressure 背压通知结构。
 *
 * 由 SDK 自动生成并发送给上游服务。
 * 调用方可注册 OnBackpressure 回调自行处理。
 */
type Backpressure struct {
    SourceService     string // 发出背压通知的源服务 ID
    ChannelID         string // 触发背压的通道 ID
    Level             string // "low" | "medium" | "high" | "critical"
    QueueDepth        uint32 // 当前队列深度
    SuggestedPauseMs  uint32 // 建议暂停发送的毫秒数
}
```

**Level 语义：**

| level | 触发条件（usage%） | 行为 |
|-------|-------------------|------|
| `low` | > 50% | 通知上游，不阻塞发送 |
| `medium` | > 70% | 建议上游降速 |
| `high` | > 85% | 强烈建议暂停，`SuggestedPauseMs` 有效 |
| `critical` | 100% | 通道已满，新发送将收到 `ErrChannelFull` |

##### OnBackpressure

```go
/**
 * OnBackpressure 注册背压通知回调。
 *
 * 当收到上游发来的 Backpressure 通知时调用。
 * 若未设置，SDK 默认行为：critical 级别时暂停对应通道发送。
 */
func (c *Client) OnBackpressure(handler func(*Backpressure))
```

---

#### 4.2.5 子请求管理

##### SubRequestWait

等待所有已发起的子请求完成。

```go
/**
 * SubRequestWait 等待指定父消息的所有子请求完成。
 *
 * 会阻塞直到所有子请求返回或 TTL 超时。
 * TTL 超时后未完成的子请求被取消。
 */
func (c *Client) SubRequestWait(parentMessageID string, timeout time.Duration) error
```

**嵌套深度限制：** 子请求的最大嵌套深度由服务组定义中的 `max_sub_request_depth` 限制（默认 3）。每次创建子请求时，SDK 检查当前 `parent_trace.sequence` 的嵌套层级，超过限制时 `Send` 返回 `ErrMaxDepthExceeded`，子请求不被创建。此检查在 access_check 之后、消息入通道之前执行。

---

##### SubRequestCount

查询未完成的子请求数量。

```go
/**
 * SubRequestCount 查询指定父消息当前未完成的子请求数。
 */
func (c *Client) SubRequestCount(parentMessageID string) int
```

---

#### 4.2.6 dynamic_spawn

##### DynamicSpawn

运行时动态创建临时服务实例。

```go
/**
 * DynamicSpawn 动态创建临时服务实例。
 *
 * 调用方必须持有 dynamic_spawn 权限。
 * 实际执行由 Runtime 管理。
 */
func (c *Client) DynamicSpawn(spec *DynamicSpawnSpec) (serviceID string, err error)
```

**DynamicSpawnSpec 结构：**

```go
type DynamicSpawnSpec struct {
    ExecutableSource string            // "inline:base64" | "path"
    ExecutableData   []byte            // 可执行内容
    ContentHash      string            // SHA256 哈希（hex 编码），64 字符
    Args             []string          // 参数数组
    Env              map[string]string // 环境变量
    MaxRuntimeMs     uint32            // 最大运行时间
    OutputLimitBytes uint64            // 输出上限
    ReplyChannel     string            // 回复通道端口名
    Isolation        string            // "in_process" | "separate_process"
}
```

**错误：**

| 错误 | 含义 |
|------|------|
| `ErrUnsupported` | Runtime 不支持 dynamic_spawn |
| `ErrHashMismatch` | content_hash 校验失败 |
| `ErrPermissionDenied` | 调用方未持有 dynamic_spawn 权限 |
| `ErrMaxServices` | 已达到服务组 max_services 上限 |

---

#### 4.2.7 端口约定

以下端口名在 IFP 协议中具有预定义语义，服务应遵循此约定以确保互操作性。

| 端口名 | 语义 | 方向 | 说明 |
|--------|------|------|------|
| `in` | 默认输入端口 | 输入 | 服务的通用消息入口 |
| `out` | 默认输出端口 | 输出 | 服务的通用消息出口 |
| `tool_call` | LLM 工具调用端口 | 输出 | LLM 服务发起工具调用的端口 |
| `tool_result` | LLM 工具结果端口 | 输入 | LLM 服务接收工具调用结果的端口 |
| `dispatch` | 路由分发端口 | 输出 | router 服务分发消息的端口 |
| `register` | 注册请求端口 | 输出 | 工具注册请求的端口 |
| `response` | 回复端口 | 输出 | 明确的回复消息端口 |
| `error` | 错误输出端口 | 输出 | 错误信息输出端口 |

以上端口名是约定而非强制。服务可以定义任意名称的额外端口。未在服务组 channels 中声明的端口不会建立通道。

---

#### 4.2.8 回调注册

##### OnConfigReload

注册配置热更新回调。

```go
/**
 * OnConfigReload 注册配置热更新回调。
 *
 * 当控制面通过控制通道下发 ConfigReload 时，调用此回调。
 */
func (c *Client) OnConfigReload(handler func(configJSON string))
```

---

##### OnPolicyUpdate

注册策略更新回调。

```go
/**
 * OnPolicyUpdate 注册策略更新回调。
 *
 * 当控制面通过控制通道下发 PolicyPush 时，调用此回调。
 * 若未设置回调，SDK 默认自动更新策略表。
 */
func (c *Client) OnPolicyUpdate(handler func(update *PolicyUpdate))
```

---

### 4.3 外部进程通信协议

当服务以 `separate_process` 隔离模式启动时，Runtime 通过标准 I/O（stdin/stdout）与外部进程通信，采用 **JSON Lines** 格式（每行一个完整 JSON 对象，以 `\n` 分隔）。无需额外的桥接进程。

#### 4.3.1 外部进程 → Runtime (stdout)

外部进程通过 stdout 向 Runtime 发送消息。Runtime 读取后将其路由到对应输出通道。

```json
{"port":"out","payload":"SGVsbG8sIHdvcmxk","trace_id":"tr-001","stream":false}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|:--:|------|
| `port` | string | ✅ | 输出端口名（如 `out`） |
| `payload` | string | ✅ | Base64 编码的消息载荷 |
| `trace_id` | string | | 追踪 ID，不填则自动生成 |
| `stream` | bool | | 是否为流式消息，默认 false |
| `stream_final` | bool | | 是否流式结束，默认 false |

#### 4.3.2 Runtime → 外部进程 (stdin)

Runtime 将从输入通道接收到的消息通过 stdin 写入外部进程。

```json
{"port":"in","payload":"SGVsbG8sIHdvcmxk","trace_id":"tr-001","source_port":"in","stream":false,"stream_final":false}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|:--:|------|
| `port` | string | ✅ | 输入端口名（如 `in`） |
| `payload` | string | ✅ | Base64 编码的消息载荷 |
| `trace_id` | string | | 追踪 ID |
| `source_port` | string | ✅ | 消息来源端口 |
| `stream` | bool | | 是否流式 |
| `stream_final` | bool | | 是否流式结束 |

#### 4.3.3 控制信号

Runtime 通过 stdin 向外部进程发送控制信号（JSON Lines 格式）：

**终止信号：**
```json
{"type":"signal","signal":"terminate"}
```

**暂停信号：**
```json
{"type":"signal","signal":"pause"}
```

**恢复信号：**
```json
{"type":"signal","signal":"resume"}
```

#### 4.3.4 外部进程编写示例 (Python)

```python
import sys, json, base64

for line in sys.stdin:
    msg = json.loads(line)
    if msg.get("type") == "signal":
        if msg["signal"] == "terminate":
            break
        continue
    # 处理消息
    payload = base64.b64decode(msg["payload"])
    response = payload.upper()  # 示例：转为大写
    reply = {"port": "out", "payload": base64.b64encode(response).decode()}
    print(json.dumps(reply), flush=True)
```

---

### 4.4 控制通道协议

控制通道是控制面与 SDK 之间的协议边界。进程内服务通过直接方法调用通信；跨进程场景使用可插拔传输（TCP/Unix socket/命名管道），消息格式为 JSON 行协议。

#### 4.4.1 通用格式

每条控制消息是一个以换行符 `\n` 分隔的 JSON 对象。

```json
{"type": "<message_type>", ...}
```

**控制面 → SDK：** PolicyPush, ConfigReload
**SDK → 控制面：** AuditBatch, StatusReport

所有请求类消息（PolicyPush, ConfigReload）预期收到 ACK 响应。

#### 4.4.2 PolicyPush (下行)

```json
{
  "type": "PolicyPush",
  "seq": 1,
  "rules": [
    {
      "op": "add",
      "source": "agent-001",
      "channel": "out",
      "target": "tool-001",
      "permit": true
    },
    {
      "op": "remove",
      "source": "agent-001",
      "channel": "tool_call",
      "target": "deprecated-tool"
    }
  ]
}
```

**字段说明：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `type` | string | 固定为 "PolicyPush" |
| `seq` | uint | 单调递增序号，用于 ACK 匹配 |
| `rules[].op` | string | "add" 或 "remove" |
| `rules[].source` | string | 发送方 service_id |
| `rules[].channel` | string | 通道端口名 |
| `rules[].target` | string | 接收方 service_id |
| `rules[].permit` | bool | op=add 时有效，true=允许，false=显式拒绝 |

**ACK 响应（SDK → 控制面）：**

```json
{"type": "PolicyPushAck", "seq": 1, "accepted": true, "rules_processed": 2}
```

---

#### 4.4.3 ConfigReload (下行)

```json
{
  "type": "ConfigReload",
  "seq": 2,
  "config": {
    "temperature": 0.3,
    "max_tokens": 4096
  }
}
```

**ACK 响应：**

```json
{"type": "ConfigAck", "seq": 2, "accepted": true}
```

---

#### 4.4.4 AuditBatch (上行)

```json
{
  "type": "AuditBatch",
  "seq": 10,
  "events": [
    {
      "event_type": "AccessViolation",
      "timestamp_ms": 1716800100000,
      "data": {
        "source": "agent-001",
        "target": "forbidden-tool",
        "channel": "tool_call",
        "deny_reason": "not in whitelist"
      }
    },
    {
      "event_type": "PrivilegeDrop",
      "timestamp_ms": 1716800105000,
      "data": {
        "privilege": "tool_call",
        "previous_mask": "0b0111",
        "new_mask": "0b0011"
      }
    }
  ]
}
```

**审计事件类型与 data schema：**

**ServiceStart：**
```json
{
  "event_type": "ServiceStart",
  "timestamp_ms": 1716800000000,
  "data": {}
}
```

**ServiceExit：**
```json
{
  "event_type": "ServiceExit",
  "timestamp_ms": 1716800100000,
  "data": {
    "exit_code": 0,
    "reason": "normal"
  }
}
```

**AccessViolation：**
```json
{
  "event_type": "AccessViolation",
  "timestamp_ms": 1716800100000,
  "data": {
    "source": "agent-001",
    "target": "forbidden-tool",
    "channel": "tool_call",
    "deny_reason": "not in whitelist" | "privilege_not_held"
  }
}
```

**PrivilegeDrop：**
```json
{
  "event_type": "PrivilegeDrop",
  "timestamp_ms": 1716800100000,
  "data": {
    "privilege": "tool_call",
    "previous_mask": "0b0111",
    "new_mask": "0b0011"
  }
}
```

**DynamicSpawn：**
```json
{
  "event_type": "DynamicSpawn",
  "timestamp_ms": 1716800100000,
  "data": {
    "spawned_service_id": "dyn-7f3a",
    "content_hash": "e3b0...b855",
    "requested_by": "agent-001"
  }
}
```

**ChannelError：**
```json
{
  "event_type": "ChannelError",
  "timestamp_ms": 1716800100000,
  "data": {
    "channel_id": "ch-abc",
    "error": "channel_full" | "channel_broken" | "message_timeout",
    "source_port": "out",
    "target_service": "tool-001",
    "target_port": "in"
  }
}
```

**ACK（控制面 → SDK）：**

```json
{"type": "AuditAck", "seq": 10}
```

---

#### 4.4.5 StatusReport (上行)

```json
{
  "type": "StatusReport",
  "seq": 5,
  "state": "RUNNING",
  "uptime_ms": 3598000,
  "channel_stats": {
    "in": {"msg_received": 150, "queue_depth": 2, "queue_capacity": 256},
    "out": {"msg_sent": 140, "errors": 1}
  }
}
```

---

### 4.5 Bootstrap 协议

Bootstrap 协议用于外部进程通过 TCP/Unix socket 握手绑定到 IFP 网络。

#### 4.5.1 传输

- **传输层：** TCP 或 Unix socket（由 Runtime 配置决定）
- **地址：** 由 `IFP_BOOTSTRAP_ADDR` 环境变量指定（默认 `127.0.0.1:9740`）
- **消息格式：** 每行 JSON，以 `\n` 分隔

#### 4.5.2 消息序列

```
Client (外部进程)                      Server (Runtime)
     │                                       │
     │──── connect(bootstrap_addr) ─────────→│  [建立连接]
     │                                       │
     │←── Challenge ────────────────────────│  [2]
     │                                       │
     │──── Proof ──────────────────────────→│  [3]
     │                                       │  [身份验证]
     │                                       │
     │←── Bind ────────────────────────────│  [4]
     │                                       │
     │  [建立通道连接]                         │
     │  [连接控制通道]                         │
     │  [加载凭证]                            │
     │                                       │
     │──── Ready ──────────────────────────→│  [5]
     │                                       │
     │  —— 服务进入 READY 状态 ——             │
```

---

#### 4.5.3 消息定义

##### [2] Challenge (Runtime → 外部进程)

```json
{
  "type": "challenge",
  "version": "3.1",
  "nonce": "a1b2c3d4e5f6a7b8",
  "auth_methods": ["hmac"]
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `version` | string | 协议版本 |
| `nonce` | string | 16 字节随机数，hex 编码，一次性使用 |
| `auth_methods` | string[] | Runtime 支持的身份验证方法，按优先级排序 |

---

##### [3] Proof (外部进程 → Runtime)

```json
{
  "type": "proof",
  "method": "hmac",
  "principal": "agent-001",
  "proof_data": "3f7b9a2c..."
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `method` | string | 选择的验证方法，必须是 challenge.auth_methods 之一 |
| `principal` | string | 服务组中声明的身份主体 |
| `proof_data` | string | HMAC 方法：`HMAC-SHA256(nonce, key)` 的 hex 编码 |

---

##### [4] Bind (Runtime → 外部进程)

```json
{
  "type": "bind",
  "service_id": "agent-001",
  "control_addr": "127.0.0.1:9741",
  "channels": [
    {"name": "in",  "role": "input",  "addr": "127.0.0.1:9742"},
    {"name": "out", "role": "output", "addr": "127.0.0.1:9743"}
  ],
  "credential": "eyJwcmluY2lwYWwiOiJhZ2VudC0wMDEi..."
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `service_id` | string | 分配的服务标识符 |
| `control_addr` | string | 控制通道连接地址 |
| `channels` | array | 通道声明列表 |
| `channels[].name` | string | 端口名称 |
| `channels[].role` | string | "input" 或 "output" |
| `channels[].addr` | string | 通道连接地址（TCP/Unix socket） |
| `credential` | string? | 可选，Base64 编码的身份凭证 |

**客户端处理：**
1. 对每个 channels[i]，连接到 `channels[i].addr` 建立通道连接
2. 连接到 `control_addr` 建立控制通道
3. 如果存在 `credential`，解码并加载身份

---

##### [5] Ready (外部进程 → Runtime)

```json
{
  "type": "ready",
  "service_id": "agent-001"
}
```

---

##### Error (任一方发)

```json
{
  "type": "error",
  "code": "auth_failed",
  "message": "HMAC mismatch: expected 3f7b got 8a2c"
}
```

**错误码：**

| 错误码 | 含义 |
|--------|------|
| `auth_failed` | 身份验证不通过 |
| `principal_not_found` | principal 不在服务组中 |
| `bind_timeout` | Bind 消息后 30 秒内未收到 Ready |
| `version_mismatch` | 协议版本不匹配 |

---

## 5. 状态机与流程规范

### 5.1 服务状态机

服务状态由以下事件驱动：控制面的 `terminate`（TERMINATE/KILL → STOPPED）、流控信号（PAUSE → PAUSED、RESUME → RUNNING）、以及消息处理完成事件（RUNNING → READY）。

```
              spawn
                │
                v
   READY ──────────────► RUNNING ──────────────► STOPPED
     │                  │       │                   │
     │                  │       │ (PAUSE)           │
     │                  │       v                   │
     │                  │    PAUSED                 │
     │                  │       │                   │
     │                  │       │ (RESUME)          │
     │                  │       v                   │
     │                  │    RUNNING                │
     │                  │                           │
     │                  │ (terminate)               │
     │                  v                           │
     └────────────────► STOPPED ◄─────────────────┘
                              │
                              v
                           reaped
```

#### 5.1.1 状态定义

| 状态 | 含义 | 进入条件 |
|------|------|---------|
| `READY` | spawn 完成，等待首个消息或信号 | spawn 成功，SDK 初始化完毕 |
| `RUNNING` | 处理消息中 | 收到第一条消息；或从 READY 因 RESUME 进入 |
| `PAUSED` | 停止从输入通道接收新消息；已开始处理的消息（含等待中的子请求）不受影响 | 收到 PAUSE |
| `STOPPED` | terminate 完成，等待 reap | 收到 TERMINATE/KILL |
| `reaped` | 最终态，资源已回收 | 控制面调用 reap |

#### 5.1.2 转移规则

| 当前状态 | 触发事件 | 下一状态 | 说明 |
|---------|---------|---------|------|
| READY | 收到消息 | RUNNING | 首个消息到达任一输入通道 |
| RUNNING | 处理完成，发送结果 | READY | 当前消息处理完毕 |
| RUNNING | 收到 PAUSE | PAUSED | 暂停接收新消息 |
| RUNNING | 收到 TERMINATE / KILL | STOPPED | 终止信号 |
| PAUSED | 收到 RESUME | RUNNING | 恢复接收新消息 |
| PAUSED | 收到 TERMINATE / KILL | STOPPED | PAUSED 态可直接终止 |
| READY | 收到 TERMINATE / KILL | STOPPED | READY 态可直接终止 |
| STOPPED | 控制面调用 reap | reaped | 资源回收完成 |

#### 5.1.3 状态实现要求

| 状态 | 允许接收消息 | 允许处理消息 | 允许发送消息 | 允许发起子请求 | 允许接收子请求回复 |
|------|------------|------------|------------|-------------|-----------------|
| READY | 是 | — | — | — | — |
| RUNNING | 是 | 是 | 是 | 是 | 是 |
| PAUSED | 否 | 是(当前) | 是 | 否 | 是 |
| STOPPED | 否 | 否 | 否 | 否 | 否 |
| reaped | — | — | — | — | — |

**PAUSED 状态特殊说明：** PAUSED 状态不接收新消息，但：
- 已有 ACTIVE 的消息继续处理
- 已有 AWAITING_SUB 的消息可以继续等待子请求完成
- 子请求完成后，回复消息正常接收和处理
- 父消息的 COMPLETED 结果正常发送

---

### 5.2 消息状态机

子请求等待不改变服务状态，而是改变消息的处理状态。消息状态对控制面不可见，由服务内部的 SDK 管理。

```
         收到消息
            │
            v
         ACTIVE ──────────► COMPLETED ──► (结果发送)
            │
            │ (发起子请求)
            v
      AWAITING_SUB
            │
            │ (所有子请求完成)
            v
         ACTIVE ──────────► COMPLETED
            │
            │ (TTL 过期)
            v
       DEAD_LETTER
```

#### 5.2.1 状态定义

| 状态 | 含义 | 说明 |
|------|------|------|
| `ACTIVE` | 服务正在处理中 | 消息由 Recv 派发给服务后进入此状态 |
| `AWAITING_SUB` | 服务已发出子请求，等待结果 | 服务调用 Send 时设置 ParentMessageID 后自动进入。服务可同时处理其他消息 |
| `COMPLETED` | 处理完成，结果已发送 | 服务处理完毕后，SDK 自动标记 |
| `DEAD_LETTER` | TTL 过期或无法路由 | 消息被转发到死信汇集服务，保留原始消息体和失败原因 |

#### 5.2.2 状态转移

| 当前状态 | 触发事件 | 下一状态 | 说明 |
|---------|---------|---------|------|
| —（新消息） | 消息到达输入通道 | ACTIVE | Recv 派发 |
| ACTIVE | 服务完成处理，发送结果 | COMPLETED | Send 成功发送回复 |
| ACTIVE | 服务发起子请求 | AWAITING_SUB | Send 设置 parent_trace |
| AWAITING_SUB | 所有子请求回复到达 | ACTIVE | SDK 自动触发恢复 |
| ACTIVE | TTL 过期 | DEAD_LETTER | ttl_ms 递减至 0 |
| AWAITING_SUB | TTL 过期 | DEAD_LETTER | 子请求被取消 |

#### 5.2.3 消息状态与服务状态的关系

| 服务状态 | 可存在的消息状态 | 说明 |
|---------|----------------|------|
| RUNNING | ACTIVE, AWAITING_SUB | 可同时有不同状态的多个消息 |
| PAUSED | ACTIVE, AWAITING_SUB | 不接收新消息，但已有消息继续流转 |
| STOPPED | — | 所有消息被清理 |

- RUNNING 状态的服务可以有 ACTIVE 和 AWAITING_SUB 的消息同时存在
- PAUSED 状态的服务不接收新消息，但已有 AWAITING_SUB 的消息可以继续等待子请求完成
- 子请求的 TTL 过期仅影响该消息（进入 DEAD_LETTER），不影响服务状态

---

### 5.3 自演进闭环流程

LLM 服务在运行过程中发现能力不足时，通过自演进闭环自动扩展工具能力。

```
LLM 服务发现能力不足
    │
    ▼
LLM 生成工具提案（名称、输入/输出端口、实现方式）
    │
    ▼
提案通过 register 端口发送到 admin 服务
    │
    ▼
admin 校验提案 → 是否 auto_approve？
    │
    ├── auto_approve=true → 自动审批
    │
    └── auto_approve=false → 排队等待人工审批
    │
    ▼
admin 将新工具写入 .group.toml → 触发 ReloadGroup 增量更新
    │
    ▼
控制面 spawn 新工具服务
    │
    ▼
registry 服务更新能力目录（新工具的 channel 信息注册）
    │
    ▼
后续请求可通过 tool_call 端口路由到新工具
```

#### 5.3.1 提案格式

LLM 服务生成的工具提案通过 `register` 端口发送给 admin，消息 content_type 为 `application/json`：

```json
{
  "proposal_id": "prop-uuid",
  "proposed_by": "agent-001",
  "tool": {
    "service_id": "weather-api-tool",
    "type": "tool",
    "description": "查询天气信息的工具",
    "command": "weather_tool",
    "args": ["--mode", "server"],
    "isolation": {
      "mode": "in_process"
    },
    "privileges": ["tool_exec", "channel_send"],
    "channels": {
      "weather-api-tool:in": ["router:dispatch"],
      "weather-api-tool:out": ["agent:tool_result"]
    }
  },
  "reason": "当前无法回答天气相关查询，需要实时天气数据源"
}
```

#### 5.3.2 admin 校验规则

| 校验项 | 规则 | 不通过时行为 |
|--------|------|------------|
| service_id 唯一性 | 不与已有服务重名 | 拒绝，返回冲突原因 |
| isolation 合法性 | mode 必须是 `in_process` 或 `separate_process` | 拒绝，返回配置错误原因 |
| max_services | 服务组内服务总数不超过上限 | 拒绝，返回容量限制原因 |
| 可执行内容 | 若 executable_source=inline，需附带 SHA256 哈希 | 拒绝，返回校验原因 |
| 通道引用有效性 | channels 中引用的目标 service_id 必须已存在 | 拒绝，返回拓扑错误原因 |

#### 5.3.3 审批响应

admin 服务处理完成后，通过 `response` 端口返回结果：

```json
{
  "proposal_id": "prop-uuid",
  "status": "approved",
  "new_service_id": "weather-api-tool",
  "group_version": "1.1"
}
```

或拒绝：

```json
{
  "proposal_id": "prop-uuid",
  "status": "rejected",
  "reason": "已达到服务数上限 (100)"
}
```

---

## 附录 A：环境变量

| 变量名 | 使用者 | 说明 |
|--------|--------|------|
| `IFP_SERVICE_ID` | 服务进程 | 服务标识符 |
| `IFP_RUNTIME_ADDR` | 服务进程 | Runtime 地址（进程内服务为空） |
| `IFP_BOOTSTRAP_ADDR` | 外部进程 | Bootstrap 握手地址（默认 `127.0.0.1:9740`） |
| `IFP_CHANNEL_MAP` | 服务进程 | 通道映射 JSON |
| `IFP_CREDENTIAL` | 服务进程 | 身份凭证（Base64 编码） |
| `IFP_RELAY_TOKEN` | 外部进程/adapter | 中继认证预共享 token |

---

## 附录 B：文件路径约定

| 路径 | 说明 |
|------|------|
| `{config_dir}/groups/` | 服务组定义文件目录 |
| `{data_dir}/audit.db` | 审计事件持久化存储 |
| `{data_dir}/dead_letters/` | 死信消息持久化存储 |

路径中的 `{config_dir}` 和 `{data_dir}` 由 Runtime 配置决定，典型值：
- Linux: `config_dir=/etc/ifp`, `data_dir=/var/lib/ifp`
- macOS: `config_dir=~/Library/Application Support/ifp`, `data_dir=~/Library/Application Support/ifp`
- Windows: `config_dir=%ProgramData%/ifp`, `data_dir=%ProgramData%/ifp`

---

## 附录 C：错误码速查

### SDK 错误码

| 常量 | 含义 |
|------|------|
| `ErrAccessDenied` | access_check 拒绝 |
| `ErrChannelFull` | 通道缓冲已满 |
| `ErrChannelNotFound` | 端口不存在 |
| `ErrChannelBroken` | 通道已断开 |
| `ErrTimeout` | 操作超时 |
| `ErrPrivNotHeld` | 未持有该权限 |
| `ErrInvalidPriv` | 非法权限名称 |
| `ErrUnsupported` | 操作不被支持 |
| `ErrHashMismatch` | 哈希校验不匹配 |
| `ErrPermissionDenied` | 权限不足 |
| `ErrMaxServices` | 达到服务数上限 |
| `ErrInvalidArg` | 参数非法 |
| `ErrInternal` | 内部错误 |
| `ErrMaxDepthExceeded` | 超过 max_sub_request_depth 限制 |

### Bootstrap 协议错误码

| 错误码 | 含义 |
|--------|------|
| `auth_failed` | 身份验证不通过 |
| `principal_not_found` | principal 不在服务组定义中 |
| `bind_timeout` | 绑定超时（30 秒） |
| `version_mismatch` | 协议版本不匹配 |

### 控制面 API 错误码

| 错误码 | 含义 |
|--------|------|
| `PARSE_ERROR` | TOML 语法错误 |
| `VALIDATION_ERROR` | 服务/通道定义不合法 |
| `GROUP_ALREADY_LOADED` | 同名服务组已加载 |
| `GROUP_NOT_FOUND` | 服务组不存在 |
| `MAX_SERVICES_EXCEEDED` | 超过 max_services 限制 |
| `CHANNEL_CONFLICT` | 新通道与现有通道冲突 |
| `SERVICE_ALREADY_EXISTS` | service_id 已存在 |
| `SERVICE_NOT_FOUND` | 服务不存在 |
| `SERVICE_NOT_STOPPED` | 服务未处于 STOPPED 状态 |
| `INVALID_SPEC` | ServiceSpec 定义不合法 |
| `SPAWN_FAILED` | 服务创建失败 |
| `INIT_TIMEOUT` | 服务在超时时间内未发送 READY |
