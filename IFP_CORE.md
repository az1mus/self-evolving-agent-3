# Information Flow Protocol v3

> 版本: 3.1
> 日期: 2026-05-27

---

### 0. 定位

IFP 是面向多 Agent LLM 应用的**跨平台 Agent 运行时**。它借用 OS 进程模型（spawn/reap/signal/namespace）作为应用层 API 的设计隐喻，但不要求真实的内核级隔离——一个 IFP "服务"可以是运行在同一进程内的 goroutine/线程，也可以是一个独立的 OS 进程。

协议关注的是控制面与数据面的分离、身份驱动的访问控制、以及消息驱动的服务编排。IFP Runtime 作为单一进程运行，所有服务在其管理下执行，跨平台兼容，不依赖任何特定操作系统的内核特性。

---

## 1. 架构

### 1.1 控制面 / 数据面分离

IFP 系统划分为两个正交平面：

**控制面**负责编排与治理，不参与消息处理：
- 服务生命周期管理（启动、停止、重启、回收）
- 身份初始化与凭证注入
- 审计事件收集
- 策略下发与配置热更新

**数据面**负责消息的传输与处理：
- 服务间消息通信
- 运行时权限判定与策略拦截
- 通道建立与拆除
- 流控信号传递

控制面不介入数据面原语的执行。服务的启动、终止由控制面触发，但服务启动后的所有通信行为由数据面在 IFP Runtime 内直接完成。

控制面与 Runtime 之间的协议边界是一条独立的**控制通道**（见 1.4.5 节）。控制通道承载策略下发、配置热更新、审计批量上报等管理信令，不参与消息收发路径。

### 1.2 服务即节点

系统中的每一个处理单元是一个**服务**。服务的形态可以是：
- 运行在 IFP Runtime 进程内的 goroutine/线程（默认方式，零开销通信）
- 一个独立的 OS 进程（需要更强隔离时，通过 spawn 创建）
- 一个已存在的外部进程（通过第 11 节定义的绑定协议接入）

所有服务共享相同的通信原语和安全模型，形态差异仅影响其创建方式和底层传输。服务的代码通过 IFP SDK 与 Runtime 交互，不关心自身运行在进程内还是独立进程中。

### 1.3 通道即拓扑

服务之间通过**通道**通信。通道是单向的、有命名的消息路径。一个服务可以拥有多个输入通道和多个输出通道。通道在服务组定义中声明，由控制面在服务启动前建立；运行时也可通过 `channel_create` 原语由数据面动态创建。

消息沿通道传递，不携带路由信息。消息的去向由发送方选择的输出通道决定，不嵌入消息体。

### 1.4 IFP Runtime

IFP Runtime 是一个独立运行的后台进程，承载控制面和数据面的全部逻辑。它不是嵌入每个服务的 sidecar，而是一个中心化的消息路由和服务管理器——所有服务通过 IFP SDK 连接到它。

#### 1.4.1 架构分层

IFP Runtime 在逻辑上分为两层：

| 层 | 名称 | 运行方式 | 职责 |
|----|------|------|------|
| 控制面 | Controller | Runtime 进程内 | 服务生命周期管理、身份与凭证管理、审计收集、策略下发 |
| 数据面 | Router | Runtime 进程内 | 消息路由与派发、access_check 判定、信号传递、背压检测 |

服务代码通过 **IFP SDK**（一个轻量级客户端库）连接到 Runtime。SDK 提供 `ifp_send` / `ifp_recv` 等 API，内部通过本地连接与 Runtime 通信。

#### 1.4.2 服务生命周期

```
┌──────────────────────────────────────────────────┐
│                  初始化阶段                        │
│                                                    │
│  1. Runtime 根据服务组定义创建服务实例              │
│  2. 分配 service_id，建立通道                      │
│  3. 注入身份和初始权限                             │
│  4. 对于进程内服务：启动 goroutine/线程             │
│     对于独立进程服务：启动 OS 进程                  │
│  5. 服务调用 SDK 连接到 Runtime                   │
│  6. Runtime 确认连接，服务进入 READY 状态           │
└──────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────┐
│                  运行阶段                        │
│                                                    │
│  循环：                                             │
│  1. 服务等待输入通道的消息（通过 SDK 阻塞/非阻塞接收） │
│  2. 收到消息 → 调用服务处理函数                      │
│  3. 服务向输出通道发送消息 →                         │
│     a. Runtime 截获，执行 access_check               │
│     b. Permit → 消息路由到目标服务的输入通道          │
│     c. Deny → 返回 ACCESS_DENIED 给发送方           │
│  4. Runtime 监控通道缓冲使用率，需要时发出背压通知   │
│  5. privilege_transition 时更新服务的能力掩码         │
│  6. 审计事件批量上报到控制面（缓冲区满或定时触发）    │
└──────────────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────┐
│                  终止阶段                        │
│                                                    │
│  1. Runtime 向服务发送 TERMINATE 或服务主动退出     │
│  2. 服务完成当前消息（优雅退出）或立即停止（强制）   │
│  3. SDK 发送最后一批审计事件，等待 ACK              │
│  4. Runtime 关闭服务的所有通道                      │
│  5. 服务实例被回收                                  │
└──────────────────────────────────────────────────┘
```

#### 1.4.3 失败模型

| 失败场景 | 行为 | 对系统的影响 |
|----------|------|-------------|
| **Runtime 内部错误**（panic、断言失败） | Runtime 进程终止，所有服务停止 | 整个服务组不可用。需外部监控重启 Runtime |
| **单个服务崩溃**（goroutine panic / 进程退出） | Runtime 检测到退出，标记服务为 STOPPED | 该服务的通道变为断开状态，依赖它的上游收到 CHANNEL_BROKEN 错误，消息进入死信 |
| **审计事件缓冲区满** | 新事件被丢弃，计数器递增（无阻塞） | 控制面可能丢失部分审计事件，但系统可存活 |
| **access_check 策略引擎崩溃** | 降级为 Deny（默认拒绝），产生审计事件 | 该服务的所有通信被阻止，直到策略引擎恢复 |
| **SDK 断连** | Runtime 检测到连接断开，标记服务为 STOPPED | 等同于服务崩溃 |
| **初始化失败**（身份加载失败、通道映射失败） | 服务创建失败，调用方收到错误 | 服务未进入 READY 状态 |

原则：**单个服务的崩溃不应传播。** 一个服务的失败不会影响 Runtime 和其他运行中的服务。

#### 1.4.4 性能特征

| 指标 | 估算 |
|------|------|
| **进程内消息延迟** | ~100ns（Go channel / 无锁队列） |
| **跨进程消息延迟** | 取决于传输层（本地 TCP ~50μs） |
| **SDK 内存开销** | ~1MB/服务（通道 buffer + identity 状态 + 审计缓冲区） |
| **access_check** | O(1) 哈希表查找 |
| **启动延迟** | 进程内服务 ~1ms；独立进程服务取决于 OS |

#### 1.4.5 安全边界

IFP Runtime 以普通用户权限运行，不需要系统特权。服务的隔离程度取决于其运行形态：
- **进程内服务**：依赖编程语言的运行时隔离（goroutine/线程边界），access_check 和 privilege_transition 提供逻辑层面的安全控制
- **独立进程服务**：OS 进程边界提供内存隔离，可叠加平台相关的沙箱措施

#### 1.4.6 控制通道

控制通道是控制面与各服务 SDK 之间的协议边界。它是独立于数据面消息通道的信令通道。

**传输：** Runtime 与 SDK 之间的内部连接（进程内为函数调用/内存通道，跨进程为本地 socket 连接）。

**生命周期：** 绑定于服务实例。服务通过 SDK 连接 Runtime 时建立；服务退出时关闭。

**原则：**
- 控制通道不承载数据面消息
- 控制通道中断时，已有数据面通道继续运行，仅策略更新和审计上报暂停
- 控制消息采用请求-确认模型（request/ACK），发送方等待接收方确认，可配置超时

**控制面 → SDK（下行）：**

| 操作 | 格式 | 说明 |
|------|------|------|
| `PolicyPush` | `{type, rules: [{action, service, channel, permit?}]}` | 增量更新 access_check 规则 |
| `ConfigReload` | `{type, config: {key, value}}` | 热更新服务配置 |

**SDK → 控制面（上行）：**

| 操作 | 格式 | 说明 |
|------|------|------|
| `AuditBatch` | `{type, events: [{event_type, timestamp, data}]}` | 批量审计事件上报 |
| `StatusReport` | `{type, state, uptime_ms, channel_stats, msg_count}` | 状态快照 |

---

## 2. 消息格式

### 2.1 结构

```
Message:
├── trace_id:      string        // 追踪链 ID，贯穿一次完整请求的全部消息
├── message_id:    string        // 消息唯一标识，UUID
├── content:       bytes         // 载荷
├── content_type:  string        // 载荷 MIME 类型
├── priority:      uint8         // 优先级 0-255，默认 128
├── ttl_ms:        uint          // 生存时间（毫秒）
├── created_at:    timestamp     // 创建时间
├── in_response_to: string|null  // 被回复的消息 ID（非子请求场景下的直接回复）
└── parent_trace:  ParentTrace|null
    ├── parent_message_id: string
    └── sequence:          uint
```

### 2.2 设计原则

消息不携带服务拓扑信息。一条消息从服务 A 的某个输出通道发出，由通道另一端的服务 B 接收。B 不需要知道整条链路的全貌——它只知道自己从哪个通道收到消息、自己应该处理什么、以及完成后从哪个通道发出。

LLM 服务收到的消息中，上下文已经被压缩进提示词。消息本身只携带当前轮次的输入，不包含上游服务的完整输出历史。

### 2.3 载荷类型

`content_type` 采用 MIME 类型标注。常见值：

| content_type | 含义 |
|-------------|------|
| `application/json` | 结构化数据 |
| `text/plain` | 纯文本 |
| `text/markdown` | Markdown 文本 |
| `application/octet-stream` | 原始字节 |

未声明时默认为 `application/octet-stream`。

### 2.4 追踪

`trace_id` 在一次外部请求进入系统时创建，该请求触发的全部消息——包括子请求和间接调用——共享同一个 `trace_id`。

`message_id` 每条消息唯一。

`in_response_to` 用于直接的一对一回复场景：服务 A 向服务 B 发起询问，B 处理后回复，回复消息的 `in_response_to` 指向 A 的询问消息。与 `parent_trace` 的区别在于不涉及挂起/恢复语义。

---

## 3. 原语

系统提供 10 项原语。基础原语 6 项覆盖服务管理与通信，安全原语 3 项覆盖身份与权限，增强原语 1 项覆盖运行时动态生成。

### 3.1 spawn

创建新的服务实例。控制面操作。

```
spawn(spec: ServiceSpec) → service_id
```

`ServiceSpec` 包含：
- 可执行文件路径或服务入口函数
- 启动参数与环境变量
- 隔离级别声明
- 身份声明
- 通道声明

返回全局唯一的 `service_id`。服务创建后处于 `READY` 状态，等待第一个消息或信号。

### 3.2 terminate

停止服务实例。控制面操作。

```
terminate(service_id: string, signal: Signal) → void
```

`Signal` 取值：
- `TERMINATE` — 请求服务优雅退出，允许完成当前消息后停止
- `KILL` — 强制立即终止，不等待

终止后服务进入 `STOPPED` 状态。

### 3.3 reap

回收已终止服务的资源。控制面操作。

```
reap(service_id: string) → ExitStatus
```

返回服务的退出码和终止原因。reap 后系统释放该服务的通道绑定和资源。

### 3.4 isolation

为服务指定隔离级别。在 spawn 时作为 ServiceSpec 的一部分声明。

声明内容：
- `mode` — 隔离模式：
  - `in_process`（默认）：服务作为 Runtime 进程内的 goroutine/线程运行，零开销通信，逻辑隔离
  - `separate_process`：服务作为独立 OS 进程运行，OS 进程边界提供内存隔离
- `network` — 网络访问范围，仅 `separate_process` 模式下有效（`none` | `loopback` | `host`）
- `filesystem` — 文件系统访问范围，仅 `separate_process` 模式下有效（`readonly` | `isolated` | `host`）

隔离级别在 spawn 时一次性建立，运行时不可修改。`in_process` 模式是默认选择，适合大多数场景；`separate_process` 适合需要强边界隔离的场景（如执行不受信任的代码）。

### 3.5 channel

建立服务间的单向消息通道。可在 spawn 时静态声明，也可在运行时由数据面动态建立。

```
channel_create(source_service: string, source_port: string,
               target_service: string, target_port: string) → channel_id
```

- `source_port` 和 `target_port` 是服务内部的通道名称，由服务自主定义
- 通道是单向的。双向通信需要两条通道
- 通道有容量上限（默认缓冲 256 条消息）。缓冲满时，发送方收到 `CHANNEL_FULL` 错误

### 3.6 signal_notify

向服务发送或从服务接收流控和自定义信号。数据面操作。

```
signal_notify(target: string, signal: Signal) → void
```

可用信号：

| 信号 | 含义 |
|------|------|
| `PAUSE` | 暂停接收新消息，正在处理的消息不受影响 |
| `RESUME` | 恢复接收新消息 |
| `RELOAD` | 配置热更新（默认行为，服务可叠加自定义语义） |
| `CUSTOM` | 自定义信号，语义由服务定义 |

PAUSE/RESUME 可由控制面发送，也可在服务间传递。TERMINATE/KILL 仅由控制面的 `terminate` 原语发出，不在 `signal_notify` 的可用信号中。

### 3.7 identity

将身份绑定到服务实例。在 spawn 时声明，控制面执行。

```
identity_bind(service_id: string, identity: Identity) → void
```

`Identity` 包含：
- `principal` — 主体标识符，全局唯一
- `credentials` — 凭证集合（token、证书、密钥引用）
- `attributes` — 属性标签（如 `role=worker`, `tier=backend`）

身份绑定在服务启动时完成，运行时不可更改。身份信息通过 SDK 初始化参数注入，不由消息传递。

### 3.8 access_check

策略判定点。服务 A 向服务 B 的通道发送消息时，Runtime 在消息路由前执行。

```
access_check(source: Identity, target: Identity, channel: string) → Permit | Deny
```

判定依据：
- 服务组定义的通道白名单（静态允许的通信路径）
- 发送方身份属性与接收方要求的前置条件
- 运行时累积的权限上下文（发送方是否已完成不可逆降权）

`Deny` 结果时消息不进入目标通道，发送方收到 `ACCESS_DENIED` 错误。该错误不产生死信，由发送方自行处理。

### 3.9 privilege_transition

不可逆降权。服务在处理过程中声明放弃某项权限。数据面操作。

```
privilege_drop(service_id: string, privilege: string) → void
```

- 降权后，Runtime 更新该服务的能力掩码
- 后续 `access_check` 将基于降权后的能力判定
- 降权不可逆——一旦放弃，无法在本次服务生命周期内重新获取

典型场景：LLM 服务在开始推理前拥有 `tool_call` 权限，推理完成后主动 drop `tool_call`，此后即使提示词要求调用工具，access_check 也会拒绝。

### 3.10 dynamic_spawn

运行时动态创建服务实例。面向需要临时执行体的场景（LLM 生成代码后执行、按需启动工具进程）。Runtime 直接管理创建过程，不需要外部辅助进程。

```
dynamic_spawn(spec: DynamicSpawnSpec) → service_id
```

`DynamicSpawnSpec` 在 `ServiceSpec` 基础上增加：
- `executable_source` — 可执行内容的来源（`inline:base64` | `path`）
- `content_hash` — 内容的 SHA256 哈希，启动前必须校验通过
- `isolation` — 强制至少为 `separate_process` 模式
- `max_runtime_ms` — 最大运行时间，到期强制终止
- `output_limit_bytes` — 输出上限

`dynamic_spawn` 与服务组定义中的静态服务有同等地位——拥有独立的身份绑定和通道。区别在于它的生命周期更短（通常单次调用后 terminate），且创建时强制使用进程级隔离。

---

## 4. 安全模型

### 4.1 纵深防御

安全不依赖服务自觉遵守协议。防御分为两层：

**第一层：原语组合。** 服务创建时绑定最小身份（identity），通过 access_check 限制通信，通过 privilege_transition 在运行时收窄权限。

**第二层：隔离边界。** `in_process` 模式依赖语言运行时的内存安全保证和 access_check 逻辑隔离；`separate_process` 模式利用 OS 进程边界提供内存隔离，可叠加平台相关的沙箱措施。

### 4.2 身份流转

```
spawn 时创建 identity
    │
    ▼
identity 绑定到服务（不可更改）
    │
    ▼
每次通道发送前 → access_check(source, target, channel)
    │
    ├── Deny → 消息丢弃，发送方收到 ACCESS_DENIED
    │
    └── Permit → 消息进入通道
    │
    ▼
服务可主动调用 privilege_transition 放弃某项权限
    │
    ▼
后续 access_check 基于降权后的能力判定
```

### 4.3 审计事件

控制面通过以下事件获得系统可见性。事件由 SDK 产生，经控制通道批量上报。

| 事件 | 触发条件 |
|------|---------|
| `ServiceStart` | 服务进入 READY 状态 |
| `ServiceExit` | 服务退出（含退出码和终止原因） |
| `AccessViolation` | access_check 返回 Deny |
| `PrivilegeDrop` | privilege_transition 执行 |
| `DynamicSpawn` | dynamic_spawn 创建新服务 |
| `ChannelError` | 通道异常（满、断开、超时） |

---

## 5. 子请求

### 5.1 模型

一个服务在处理消息时可能需要等待其他服务的结果。此时当前消息不能继续处理，但也不能丢弃。

子请求的语义：

```
1. 服务 A 收到消息 M（trace_id = T）
2. A 的处理需要服务 B 的结果
3. A 创建子消息 M_sub，M_sub.parent_trace = { parent_message_id: M.message_id, sequence: 1 }
4. A 将 M 标记为 AWAITING_SUB（暂停 M 的处理，但服务继续处理其他消息），向 B 的输出通道发送 M_sub
5. B 处理 M_sub，将结果通过通道发回 A
6. A 收到对 M_sub 的回复后，恢复 M 的处理
```

子请求的挂起是**消息级别**的，不影响服务状态。服务在等待子请求结果的同时可以继续处理其他消息。

### 5.2 并发子请求

一个服务可以同时发起多个子请求。`parent_trace.sequence` 从 1 递增，区分不同的子请求。父消息必须等待所有已发起的子请求完成后才能恢复。

### 5.3 超时

父消息挂起期间，`ttl_ms` 持续递减。TTL 到期时，未完成的子请求被取消（向其目标服务发送 TERMINATE），父消息进入 DEAD_LETTER。

### 5.4 嵌套

子请求的处理过程中可以再次发起子请求（嵌套）。最大嵌套深度由服务组定义中的 `max_sub_request_depth` 字段限制（默认 3）。超过限制的子请求创建请求被拒绝。

---

## 6. 服务组

### 6.1 概念

服务组是一次完整应用的全部服务定义。它声明：
- 参与的服务及其配置
- 服务间的通道连接
- 安全策略

服务组文件采用 TOML 格式，后缀 `.group.toml`。

### 6.2 元信息

```toml
[group]
name = "my-pipeline"
version = "1.0"
max_sub_request_depth = 3
```

### 6.3 服务定义

```toml
[services.<service_id>]
type = "llm"              # 服务类型
command = "..."           # 可执行路径（tool 类型）
args = ["..."]            # 启动参数

# 隔离级别
[services.<service_id>.isolation]
mode = "in_process"        # "in_process" | "separate_process"

# 身份
[services.<service_id>.identity]
principal = "agent-001"
attributes = { role = "worker" }

# 初始权限集
[services.<service_id>.privileges]
initial = ["llm_inference", "channel_send"]

# 类型专属配置
[services.<service_id>.config]
model = "claude-4"
system_prompt = "..."
temperature = 0.7
```

服务类型：

| type | 说明 |
|------|------|
| `llm` | LLM 驱动服务 |
| `tool` | 确定性工具执行 |
| `router` | 消息路由分发 |
| `ui` | 用户交互边界 |
| `admin` | 服务组管理（自演进场景） |
| `registry` | 能力注册中心 |
| `custom::<name>` | 自定义类型 |

### 6.4 通道定义

```toml
[channels]
"ui:out"           = ["agent:in"]
"agent:tool_call"  = ["router:in"]
"router:dispatch"  = ["tool_a:in", "tool_b:in"]
"tool_a:out"       = ["agent:tool_result"]
"tool_b:out"       = ["agent:tool_result"]
"agent:out"        = ["ui:in"]
```

格式：`"<service_id>:<port>" = ["<target_service>:<port>", ...]`

- 左侧是源服务的输出端口
- 右侧是目标服务的输入端口列表（支持扇出）
- 每对源-目标构成一条独立通道
- 不在通道定义中的通信路径，access_check 默认拒绝（运行时通过 `channel_create` 创建的通道会自动注册到白名单，不受此限制）

### 6.5 端口约定

| 端口名 | 语义 |
|--------|------|
| `in` | 默认输入端口 |
| `out` | 默认输出端口 |
| `tool_call` | LLM 服务发起工具调用的输出端口 |
| `tool_result` | LLM 服务接收工具调用结果的输入端口 |
| `dispatch` | router 服务分发消息的输出端口 |
| `register` | 工具注册请求的输出端口 |
| `response` | 回复端口 |
| `error` | 错误输出端口 |

### 6.6 示例：代码审查

```toml
[group]
name = "code-review"
version = "1.0"

[services.reviewer]
type = "llm"
[services.reviewer.isolation]
mode = "in_process"
[services.reviewer.identity]
principal = "reviewer-001"
[services.reviewer.privileges]
initial = ["llm_inference", "channel_send", "tool_call"]
[services.reviewer.config]
model = "claude-4"
system_prompt = "你是代码审查者。收到代码后检查逻辑正确性和安全隐患。如需 ESLint 检查，通过 tool_call 端口调用。"

[services.eslint]
type = "tool"
command = "npx"
args = ["eslint", "--stdin"]
[services.eslint.isolation]
mode = "separate_process"
[services.eslint.identity]
principal = "eslint-001"
[services.eslint.privileges]
initial = ["tool_exec", "channel_send"]

[channels]
"reviewer:tool_call"  = ["eslint:in"]
"eslint:out"          = ["reviewer:tool_result"]
"reviewer:out"        = ["ui:in"]
```

---

## 7. 服务状态机

### 7.1 服务状态

服务状态由以下事件驱动：控制面的 `terminate`（TERMINATE/KILL → STOPPED）、任意平面的流控信号（PAUSE → PAUSED、RESUME → RUNNING）、以及消息处理完成事件（RUNNING → READY）。子请求等待不改变服务状态——服务的 AWAITING_SUB 消息不影响服务保持在 RUNNING 态。

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

| 状态 | 含义 |
|------|------|
| `READY` | spawn 完成，等待首个消息或信号 |
| `RUNNING` | 处理消息中。可同时处理多条消息（并发模型由服务自身决定），可发起子请求而不改变服务状态 |
| `PAUSED` | 控制面通过 PAUSE 要求暂停。服务停止从输入通道接收新消息，但已开始处理的消息（含等待中的子请求）不受影响 |
| `STOPPED` | terminate 完成，等待 reap |
| `reaped` | 最终态，资源已回收 |

### 7.2 转移规则

| 当前状态 | 触发 | 下一状态 |
|---------|------|---------|
| READY | 收到消息 | RUNNING |
| RUNNING | 处理完成，发送结果 | READY |
| RUNNING | 收到 PAUSE | PAUSED |
| RUNNING | 收到 TERMINATE / KILL | STOPPED |
| PAUSED | 收到 RESUME | RUNNING |
| PAUSED | 收到 TERMINATE / KILL | STOPPED |
| READY | 收到 TERMINATE / KILL | STOPPED |
| STOPPED | 控制面调用 reap | reaped |

### 7.3 消息状态（独立于服务状态）

子请求等待不改变服务状态，而是改变消息的处理状态。消息状态对控制面不可见，由 Runtime 管理：

| 状态 | 含义 |
|------|------|
| `ACTIVE` | 服务正在处理中 |
| `AWAITING_SUB` | 服务已发出子请求，等待结果。服务可同时处理其他消息 |
| `COMPLETED` | 处理完成，结果已发送 |
| `DEAD_LETTER` | TTL 过期或无法路由 |

消息状态与服务状态的关系：

- RUNNING 状态的服务可以有 ACTIVE 和 AWAITING_SUB 的消息同时存在
- PAUSED 状态的服务不接收新消息，但已有 AWAITING_SUB 的消息可以继续等待子请求完成
- 子请求的 TTL 过期仅影响该消息（进入 DEAD_LETTER），不影响服务状态

---

## 8. dynamic_spawn 规范

### 8.1 触发条件

dynamic_spawn 由服务在运行时通过 SDK 发起。调用方必须持有 `dynamic_spawn` 权限。

### 8.2 执行流程

```
1. 调用方通过 SDK 提交 DynamicSpawnSpec
2. Runtime 校验 content_hash（不匹配 → 拒绝）
3. Runtime 创建 separate_process 模式的服务实例
4. Runtime 注入可执行内容（inline 内容写入临时文件或通过 stdin 传递）
5. Runtime 启动子进程，应用隔离配置
6. 子进程 stdout/stderr 作为其输出通道
7. 超时或进程退出后，Runtime 回收服务实例
```

### 8.3 与静态服务的区别

| | 静态服务 (spawn) | 动态服务 (dynamic_spawn) |
|---|---|---|
| 创建时机 | 服务组加载时 | 运行时按需 |
| 隔离模式 | 可配置（默认 in_process） | 强制 separate_process |
| 哈希校验 | 无 | 强制 SHA256 |
| 生命周期 | 可长期运行 | 单次执行，完成后自动 terminate |
| 通道 | 声明式（在服务组中定义） | 自动绑定到调用方指定的回复通道 |
| 权限 | 按声明 | 初始仅有 tool_exec + channel_send |

---

## 9. 扩展

### 9.1 流式处理

消息头增加 `stream: bool` 字段。启用流式时，发送方可以对同一 `message_id` 发送多条消息，接收方按到达顺序拼接 `content`。最后一条消息标记 `stream_final: bool = true`。

流式消息的 `trace_id` 和 `in_response_to` 保持不变，接收方据此将分片归组。

### 9.2 背压

当服务的输入通道缓冲使用率超过阈值时，Runtime 向其上游发送方发出背压通知：

```
Backpressure:
├── source_service: string
├── channel_id:      string
├── level:           "low" | "medium" | "high" | "critical"
├── queue_depth:     uint
└── suggested_pause_ms: uint
```

`critical` 表示通道已满，新消息将被拒绝。发送方收到背压后可选择暂缓发送、降级处理或寻找替代服务。

### 9.3 死信处理

消息在以下情况下进入死信：
- 目标服务不存在且无法路由
- TTL 过期
- 通道永久断开（目标服务已终止且不在重启）

死信消息被转发到服务组中声明的 `dead_letter` 端口（如果配置了死信汇集服务）。死信消息保留完整的原始消息体和失败原因：

```
DeadLetter:
├── original_message: Message
├── reason:           "no_route" | "ttl_expired" | "channel_broken"
├── timestamp:        timestamp
└── context:          map<string, any>   // 失败时的补充信息
```

---

## 10. 自演进

### 10.1 服务组热更新

通过 admin 服务在运行时修改 `.group.toml` 文件，触发控制面增量更新：

| 操作 | 效果 |
|------|------|
| 新增 `[services.X]` | spawn 新服务，建立其通道 |
| 移除 `[services.X]` | terminate → reap 目标服务 |
| 修改 `[services.X].config` | 发送 RELOAD 给目标服务，携带新配置 |
| 修改 `[channels]` | 创建/删除通道，不影响运行中的消息 |

未变更的服务不受影响。

### 10.2 自学习闭环

```
LLM 服务发现能力不足
    │
    ▼
LLM 生成工具提案（名称、输入/输出端口、实现方式）
    │
    ▼
提案发送到 admin 服务
    │
    ▼
admin 校验 → 写入 .group.toml → 触发增量更新
    │
    ▼
新工具服务 spawn，registry 更新能力目录
    │
    ▼
后续请求可通过 tool_call 端口路由到新工具
```

### 10.3 自演进服务组示例

```toml
[group]
name = "self-evolving"
version = "1.0"

[services.agent]
type = "llm"
[services.agent.isolation]
mode = "in_process"
[services.agent.identity]
principal = "agent-001"
[services.agent.privileges]
initial = ["llm_inference", "channel_send", "tool_call", "dynamic_spawn"]
[services.agent.config]
model = "claude-4"
system_prompt = """
你是自演进助手。当无法完成任务时：
1. 分析需要什么工具
2. 生成工具提案（名称、描述、输入输出端口、实现代码）
3. 通过 register 端口发起注册请求
"""

[services.admin]
type = "admin"
[services.admin.identity]
principal = "admin-001"
[services.admin.config]
group_file = "./self-evolving.group.toml"
auto_approve = false
max_services = 100

[services.registry]
type = "registry"

[channels]
"agent:tool_call"  = ["router:in"]
"agent:register"   = ["admin:in"]
"admin:out"         = ["registry:in"]
"registry:out"      = ["agent:tool_result"]
"router:dispatch"  = []          # 初始为空，自演进后动态增加
```

---

## A. 与 v3.0 的核心差异

| v3.0 | v3.1 |
|------|------|
| Runtime Agent = libifp.so 注入到每个 OS 进程 | Runtime = 中心化进程，服务通过 SDK 连接 |
| 通道 = 共享内存 (shm_open/mmap) | 通道 = 进程内 channel（默认）或可插拔传输 |
| 服务 = OS 进程 | 服务 = goroutine/线程（默认）或 OS 进程（可选） |
| 信号 = Linux 信号 (SIGTERM/SIGKILL/SIGSTOP/SIGCONT/SIGUSR1/SIGUSR2) | 信号 = 应用层控制消息 (TERMINATE/KILL/PAUSE/RESUME/RELOAD/CUSTOM) |
| resource_bound = cgroups v2 资源限制 | 移除。应用层不做资源管理 |
| namespace = Linux namespace 隔离 | isolation = 可选的隔离级别配置 |
| ifp-helper = 特权辅助进程 | 移除。Runtime 直接管理 dynamic_spawn |
| sd_notify = systemd 集成 | 移除。使用 Runtime 内部状态管理 |
| 控制通道 = Unix SOCK_SEQPACKET | 控制通道 = 抽象传输（进程内函数调用或本地 socket） |
| 安全模型 = 三层（原语 + 内核 + LSM） | 安全模型 = 两层（原语 + 隔离边界） |
| 绑定协议 = SCM_RIGHTS + SO_PEERCRED + /proc 检查 | 绑定协议 = 跨平台握手 + HMAC 认证 |
| 仅 Linux | 跨平台（Linux / macOS / Windows） |

---

## 11. 外部进程绑定协议

### 11.1 概述

绑定协议允许已存在的进程在不经过 Runtime spawn 的情况下接入 IFP 网络。

它与 spawn 的关系是**对称操作**：

```
spawn: Runtime 创建服务 → 注入身份与通道 → SDK 连接 → READY
 bind: 进程已存在    → 网络握手 → SDK 连接 → READY
```

两者的终点相同——同一套消息收发、同一套 access_check、同一套背压和审计上报——区别仅在于 bootstrap 阶段：bind 用网络握手替换了 Runtime 主动创建。

### 11.2 传输

绑定协议使用 TCP 连接作为默认传输层，也可配置 Unix socket（Linux/macOS）或 named pipe（Windows）。Runtime 在已知地址监听，地址由环境变量 `IFP_BOOTSTRAP_ADDR` 指定。

如果外部进程需要通过 bind 接入，该环境变量必须在进程启动前设置到其环境中。

### 11.3 协议流程

```
进程（已运行）                    Runtime
     │                                │
     │──── connect(bootstrap_addr) ──→│     [1] 连接
     │←── Challenge                   │     [2] 挑战
     │──── Proof                      │     [3] 证明
     │                                │     [身份验证]
     │←── Bind                        │     [4] 绑定
     │                                │
     │  [建立 SDK 连接]                │
     │                                │
     │──── Ready                      │     [5] 就绪
```

**步骤说明：**

**[1] 连接：** 进程连接到 `IFP_BOOTSTRAP_ADDR`（格式：`tcp://host:port`、`unix:///path/to/sock` 或 `pipe://name`）。

**[2] 挑战：** Runtime 发送 Challenge 消息：

```json
{
  "type": "challenge",
  "version": "3.1",
  "nonce": "a1b2c3d4e5f6a7b8",
  "auth_methods": ["hmac", "token"]
}
```

- `nonce`：16 字节随机数，hex 编码，一次性使用
- `auth_methods`：Runtime 支持的身份验证方法，按优先级排序

**[3] 证明：** 进程选择一种方法回复 Proof 消息：

```json
{
  "type": "proof",
  "method": "hmac",
  "principal": "agent-001",
  "proof_data": "3f7b9a2c..."
}
```

- `principal`：服务组中声明的身份主体
- `proof_data`：方法特定的证明数据。HMAC 方法：`HMAC-SHA256(nonce, key)` 的 hex 编码

**[4] 绑定：** 身份验证通过后，Runtime 发送 Bind 消息：

```json
{
  "type": "bind",
  "service_id": "agent-001",
  "sdk_endpoint": "tcp://127.0.0.1:9801",
  "channels": [
    {"name": "in",  "role": "input"},
    {"name": "out", "role": "output"}
  ],
  "credential_ref": "agent-001"
}
```

- `sdk_endpoint`：SDK 连接地址，进程后续通过 SDK 连接此地址进行消息收发
- `channels`：通道声明列表
- `credential_ref`：凭证引用，进程据此加载身份

进程收到 Bind 后，通过 SDK 连接到 `sdk_endpoint`，完成通道注册。

**[5] 就绪：** 进程完成所有初始化后发送 Ready 消息：

```json
{
  "type": "ready",
  "service_id": "agent-001"
}
```

Runtime 收到 Ready 后，将该服务标记为 `READY` 状态，纳入运行时监控。

### 11.4 身份验证

两级验证：

**第一级：HMAC 挑战-响应（推荐）**

服务组定义中可声明 `bind_key`：

```toml
[services.external-process]
type = "custom::legacy"
identity.principal = "legacy-worker"

[services.external-process.bind]
key_ref = "ifp-bind-key"       # 引用 Runtime 的密钥存储
```

证明计算：`proof_data = HMAC-SHA256(nonce, key)`

Runtime 用相同的 key 和 nonce 重新计算，比对一致则通过。

**第二级：Token 验证（可选）**

用于简单场景：

```json
{
  "type": "proof",
  "method": "token",
  "principal": "agent-001",
  "proof_data": "${IFP_BIND_TOKEN}"
}
```

Runtime 比对预配置的 token，一致则通过。

### 11.5 与 spawn 的一致性

| 方面 | spawn | bind |
|------|-------|------|
| 服务创建 | Runtime 主动创建 | 外部进程自行启动后连接 |
| 身份注入 | SDK 初始化参数 | Bind 消息 |
| 通道建立 | Runtime 预建立 | 进程收到 Bind 后建立 |
| 消息收发 | SDK API，完全相同 | SDK API，完全相同 |
| 运行阶段行为 | 完全相同 | 完全相同 |
| 终止检测 | 服务退出 / SDK 断连 | SDK 断连 / 进程退出 |
| 资源回收 | reap 原语 | SDK 断连后触发隐式回收 |

服务进入运行阶段后，两种入口的服务行为完全一致——同一套消息收发、同一套 access_check、同一套背压和审计上报。

### 11.6 错误处理

| 场景 | 行为 |
|------|------|
| 身份验证不通过 | Runtime 关闭连接，记录 `AccessViolation` 审计事件 |
| principal 不在服务组中 | Runtime 拒绝绑定，记录审计事件 |
| 握手期间连接断开 | 绑定取消，Runtime 清理预留资源 |
| Bind 消息后 30 秒内未收到 Ready | 绑定超时，Runtime 关闭连接并清理资源 |

错误消息格式：

```json
{
  "type": "error",
  "code": "auth_failed",
  "message": "HMAC mismatch: expected 3f7b got 8a2c"
}
```

错误发生后，Runtime 关闭连接。进程必须重新连接以重试绑定。

### 11.7 Adapter 中继

当外部进程无法集成 IFP SDK 时，Runtime 可创建 `ifp-adapter` 辅助服务作为中继：

```
外部进程 ←── [RelayProtocol] ──→ ifp-adapter ←── [SDK] ──→ Runtime
```

Adapter 的生命周期：
1. Runtime 根据服务组定义中的 `[relay]` 配置创建 adapter
2. Adapter 通过 SDK 完成自身绑定，持有所有 IFP 通道
3. Adapter 通过 RelayProtocol 连接外部进程
4. Adapter 在 SDK 通道和 RelayProtocol 之间双向转换消息
5. 外部进程退出后，adapter 自动终止并触发 Runtime 隐式回收

**RelayProtocol 帧格式：**

```
| len: uint32 (大端) | flags: uint8 | channel_id: uint8 | payload ... |
```

- `len`：帧总长度（含头部），5 + payload 长度
- `flags` bit 0：0 = 数据面消息，1 = 控制信令（心跳、背压通告）
- `flags` bit 1：1 = 消息结束（流式场景的分片末尾标记）
- `channel_id`：通道序号，对应 Bind 消息中 channels 数组的索引
- `payload`：消息载荷

**Relay 传输方式声明：**

```toml
[services.legacy-tool]
type = "custom::legacy"
command = "/usr/bin/legacy-process"

[services.legacy-tool.identity]
principal = "legacy-worker"

[services.legacy-tool.relay]
transport = "unix"                   # "pipe" | "unix" | "tcp"
address = "/var/run/legacy.sock"     # 外部进程监听地址
auth = "token"                       # 外部进程对 adapter 的验证方式
auth_token = "${IFP_RELAY_TOKEN}"    # 预共享 token
```

### 11.8 与 service group 的集成

服务组定义中通过 `[services.X.bind]` 和 `[services.X.relay]` 声明绑定参数：

```toml
[services.db-proxy]
type = "custom::db"
identity.principal = "db-proxy-01"

# 方式 A：SDK 绑定
[services.db-proxy.bind]
key_ref = "db-proxy-binding-key"

# 方式 B：adapter 中继（可选，与 bind 互斥）
# [services.db-proxy.relay]
# transport = "unix"
# address = "/tmp/db-proxy.sock"
```

bind 和 relay 字段互斥。两者都不声明时，该服务被认为是 Runtime spawn 创建的（默认行为）。

通道定义不受影响——绑定服务和 spawn 服务在通道声明中完全一致：

```toml
[channels]
"app:query"   = ["db-proxy:in"]
"db-proxy:out" = ["app:result"]
```

拓扑视图不区分服务的创建方式，Runtime 也不区分——Bind 完成后，该服务与 spawn 服务在运行时上等价。
