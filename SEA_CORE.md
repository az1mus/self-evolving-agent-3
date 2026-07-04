# SEA CLI — 自演进对话式 CLI 架构设计（统一 Node 模型）

> 基于 IFP v3.1 实现  
> 日期: 2026-07-04

---

## 1. 概述

SEA（Self-Evolving Agent）CLI 是一个终端对话程序。整个系统由**一组节点（Node）**组成——每个节点是一个独立的处理单元，有**名字、功能描述、输入格式、输出格式**。节点之间通过 IFP 通道交换 JSON 消息。系统能够在运行过程中自动扩展自身的能力（自演进）。

**核心简化：不区分 Agent / Tool / Skill / Router / Admin / UI —— 从外部看，一切都只是 Node。** 节点的内部实现（LLM 推理、Python 子进程、内置逻辑、纯数据模板）对调用者完全透明。调用者只需要知道：这个节点叫什么、它做什么、它接受什么格式的输入、它产出什么格式的输出。

遵循 IFP 协议设计理念：**控制面/数据面分离、节点即一切、通道即拓扑**。

---

## 2. 进程数量与架构

### 2.1 核心结论

- **固定进程数量 = 1**：sea cli 二进制是 Rust 编译的单一可执行文件，运行后即为 IFP Runtime，是系统中唯一的常驻进程。
- **工具节点进程数量 = 3 ～ N**：所有需要独立隔离的节点（如文件读写、Shell 执行、网络请求），均作为独立的 **Python 子进程**运行。最少 3 个（内置），随自演进而增加。

核心节点（UI、推理、路由、管理、目录）全部作为 **tokio async task** 在 Runtime 进程内并发执行。需要 OS 隔离的节点用 Python 实现，通过 stdin/stdout JSON Lines 协议与 Runtime 桥接。

### 2.2 架构全景

```
┌────────────────────── SEA CLI（单一 Rust 进程）──────────────────────────┐
│                                                                          │
│  ┌─────────────────── IFP Runtime ──────────────────────────────────┐   │
│  │   控制面（tokio task）           数据面（tokio task）              │   │
│  │   ┌──────┬──────┬──────┐        ┌──────────────────────────┐     │   │
│  │   │ 组管理 │ 生命  │ 审计 │        │      通道交换机          │     │   │
│  │   │      │ 周期  │ 收集 │        │   (tokio::sync::mpsc)    │     │   │
│  │   └──────┴──────┴──────┘        └──────────────────────────┘     │   │
│  └───────────────────────────────┬──────────────────────────────────┘   │
│                                  │                                       │
│  ┌─── 核心节点池（tokio async task）─────────────────────────────┐      │
│  │                                                                │      │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐     │      │
│  │  │  Node  │ │  Node  │ │  Node  │ │  Node  │ │  Node  │     │      │
│  │  │   ui   │ │ agent  │ │ router │ │ admin  │ │registry│     │      │
│  │  │        │ │  core  │ │        │ │        │ │        │     │      │
│  │  │in_proc │ │in_proc │ │in_proc │ │in_proc │ │in_proc │     │      │
│  │  └────────┘ └────────┘ └────────┘ └────────┘ └────────┘     │      │
│  └────────────────────────────┬───────────────────────────────────┘      │
│                               │ mpsc channels                             │
│                               ▼                                           │
│  ┌──────────────── 桥接节点（Python 子进程 + bridge task）─────────────┐  │
│  │                                                                      │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐            │  │
│  │  │  Node    │  │  Node    │  │  Node    │  │  Node    │            │  │
│  │  │ file_rw  │  │shell_exec│  │web_fetch │  │ dynamic  │  ...       │  │
│  │  │sep_proc  │  │sep_proc  │  │sep_proc  │  │sep_proc  │            │  │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘            │  │
│  │       │             │             │             │                    │  │
│  │   stdin/stdout  stdin/stdout  stdin/stdout  stdin/stdout            │  │
│  │   JSON Lines     JSON Lines     JSON Lines     JSON Lines            │  │
│  └───────┼──────────────┼──────────────┼──────────────┼─────────────────┘  │
│          │              │              │              │                      │
│  ┌───────┴──────┐ ┌─────┴──────┐ ┌─────┴──────┐ ┌─────┴──────┐              │
│  │ Python 子进程 │ │ Python    │ │ Python     │ │ Python     │              │
│  │ file_rw.py  │ │shell_exec │ │web_fetch   │ │ dynamic…   │              │
│  └──────────────┘ └───────────┘ └────────────┘ └────────────┘              │
└──────────────────────────────────────────────────────────────────────────────┘
```

**桥接层说明：** 每个 `separate_process` 节点对应一个 Rust async task（bridge）。Bridge 负责：
- 从通道接收消息 → 序列化为 JSON Line → 写入 Python 子进程 stdin
- 读取 Python 子进程 stdout 的 JSON Line → 反序列化 → 写入输出通道

---

## 3. Node：统一的描述模型

### 3.1 外部视角（调用者看到的）

从外部看，每个 Node 只有三个属性：

| 属性 | 含义 | 示例 |
|------|------|------|
| **id** | 唯一标识符 | `agent-core`, `file_rw`, `security_auditor` |
| **description** | 功能描述（自然语言） | `"文件读写操作，限定工作区目录"` |
| **ports** | 输入/输出端口声明，每个端口附带 format | 见下方 |

**端口声明格式：**

```toml
[node.file_rw]
description = "文件读写操作，工作区限定"
inputs  = [{ port = "in",   format = "application/json" }]
outputs = [{ port = "out",  format = "application/json" }]
```

- `port` — 端口名，用于通道绑定
- `format` — 该端口接受的 MIME 类型（`text/plain`、`text/markdown`、`application/json` 等）

**外部视角不包含的信息：** 节点是 LLM 还是 Python 脚本还是纯数据模板、节点运行在哪、节点有哪些权限——这些全部是**内部实现细节**，对调用者不可见。

### 3.2 内部视角（仅有 Runtime 知道）

内部实现由 `runtime` 字段描述，仅在系统启动和演进时使用：

| runtime.kind | 含义 | 运行方式 |
|--------------|------|----------|
| `"builtin"` | Rust 内置逻辑 | tokio async task，进程内直接执行函数 |
| `"llm"` | LLM 推理 | tokio async task，通过 reqwest 调用 LLM API |
| `"python"` | Python 脚本 | 独立子进程，stdin/stdout JSON Lines |
| `"skill"` | 纯 prompt 模板 | 无运行时，仅作为数据的 prompt 片段 |

**完整节点定义示例：**

```toml
# ── LLM 推理节点 ──
[node.agent-core]
description = "核心推理引擎，理解用户意图，调度子节点，生成自演进提案"
inputs  = [
    { port = "in",     format = "text/plain"       },
    { port = "result", format = "application/json"  },
]
outputs = [
    { port = "out",      format = "text/markdown"   },
    { port = "call",     format = "application/json" },
    { port = "register", format = "application/json" },
]
[node.agent-core.runtime]
kind = "llm"
model = "${SEA_MODEL}"
system_prompt = """你是 SEA CLI 的核心助手（Supervisor）..."""

# ── Python 工具节点 ──
[node.file_rw]
description = "文件读写操作，工作区限定。支持 read/write/append/delete"
inputs  = [{ port = "in",  format = "application/json" }]
outputs = [{ port = "out", format = "application/json" }]
[node.file_rw.runtime]
kind = "python"
command = "python"
args = ["tools/file_rw.py"]
isolation = "separate_process"

# ── 内置节点（路由调度） ──
[node.router]
description = "消息调度中心，根据调用请求中的目标能力，匹配并路由到对应的子节点"
inputs  = [{ port = "in",    format = "application/json" }]
outputs = [{ port = "out",   format = "application/json" },
           { port = "error", format = "application/json" }]
[node.router.runtime]
kind = "builtin"
function = "router_dispatch"

# ── 纯数据节点（无运行时） ──
[node.code_review]
description = "代码审查模板，按逻辑正确性、边界条件、安全性、风格四步检查"
inputs  = [{ port = "in",  format = "application/json" }]
outputs = [{ port = "out", format = "text/plain"       }]
[node.code_review.runtime]
kind = "skill"
prompt = "你是代码审查专家。按以下步骤检查：\n1. 逻辑正确性\n2. 边界条件\n3. 安全性\n4. 代码风格"
```

---

## 4. 节点职责简介

### 4.1 节点 `ui`

| 项目 | 描述 |
|------|------|
| **职责** | 用户交互边界。抢占终端，循环读取 stdin，显示输出到 stdout。 |
| **ports** | `out`（用户输入，text/plain）→ agent-core；`in`（Agent 回复，text/markdown）← agent-core |
| **runtime** | `kind = "builtin"` — 终端 raw mode、历史记录、流式输出渲染 |

### 4.2 节点 `agent-core`

| 项目 | 描述 |
|------|------|
| **职责** | 系统主控 LLM 节点（Supervisor）。接收用户消息，自主决定：直接回复、调用其他节点、或提出自演进提案。 |
| **ports** | `in`（text/plain）← ui；`result`（application/json）← 子节点结果；`out`（text/markdown）→ ui；`call`（application/json）→ router；`register`（application/json）→ admin |
| **runtime** | `kind = "llm"` — 维护对话上下文，通过 reqwest 调用 LLM API（流式），system prompt 列出 Registry 提供的当前节点能力 |

### 4.3 节点 `router`

| 项目 | 描述 |
|------|------|
| **职责** | 无状态消息调度。接收调用请求，根据请求中的目标能力描述匹配节点，将消息路由到目标节点的 `in` 端口。 |
| **ports** | `in`（application/json）← agent-core:call；`out`（application/json）→ 各节点:in；`error`（application/json）→ agent-core:result |
| **runtime** | `kind = "builtin"` — 查 Registry 获取当前所有节点的端口信息，按能力匹配路由 |

### 4.4 节点 `admin`

| 项目 | 描述 |
|------|------|
| **职责** | 自演进控制阀门。接收演进提案，校验，审批，写入节点定义，触发增量更新。 |
| **ports** | `in`（application/json）← agent-core:register；`out`（application/json）→ registry:in |
| **runtime** | `kind = "builtin"` — 提案校验（id 唯一性、上限检查、合法性）→ 写入 .group.toml → ReloadGroup |

### 4.5 节点 `registry`

| 项目 | 描述 |
|------|------|
| **职责** | 节点目录。存储所有节点的 id、description、ports 信息（不含 runtime）。 |
| **ports** | `in`（application/json）← admin:out；`query`（application/json）← 任意查询方；`out`（application/json）→ 查询结果 |
| **runtime** | `kind = "builtin"` — `HashMap<String, NodeInfo>`，`RwLock` 保护 |

### 4.6 Python 桥接节点（file_rw / shell_exec / web_fetch）

| 项目 | 描述 |
|------|------|
| **职责** | 可被调用的功能单元，全部用 Python 编写，独立子进程运行。 |
| **ports** | 每个节点声明 `in`（application/json）和 `out`（application/json） |
| **runtime** | `kind = "python"` — Bridge task 负责 mpsc ↔ stdin/stdout JSON Lines 双向转换 |

---

## 5. 通道拓扑

### 5.1 核心思想：端口绑定，不关心节点类型

通道拓扑是**纯端口绑定**——声明"哪个节点的哪个端口连接到哪个节点的哪个端口"。不涉及节点内部实现。

```toml
[node.ui]
description = "终端交互"
inputs  = [{ port = "in",  format = "text/markdown"   }]
outputs = [{ port = "out", format = "text/plain"       }]

[node.agent-core]
description = "核心推理引擎"
inputs  = [{ port = "in",     format = "text/plain"       },
           { port = "result", format = "application/json"  }]
outputs = [{ port = "out",      format = "text/markdown"   },
           { port = "call",     format = "application/json" },
           { port = "register", format = "application/json" }]

[node.router]
description = "消息调度中心"
inputs  = [{ port = "in",    format = "application/json" }]
outputs = [{ port = "out",   format = "application/json" },
           { port = "error", format = "application/json" }]

[node.file_rw]
description = "文件读写"
inputs  = [{ port = "in",  format = "application/json" }]
outputs = [{ port = "out", format = "application/json" }]

# ── 通道：纯端口绑定 ──
[channels]
# 对话主通道
"ui:out"                   = ["agent-core:in"]
"agent-core:out"           = ["ui:in"]

# 节点调用
"agent-core:call"          = ["router:in"]
"router:out"               = ["file_rw:in", "shell_exec:in", "web_fetch:in"]

# 结果返回（所有被调用节点的输出汇入同一端口）
"file_rw:out"              = ["agent-core:result"]
"shell_exec:out"           = ["agent-core:result"]
"web_fetch:out"            = ["agent-core:result"]
"router:error"             = ["agent-core:result"]

# 自演进闭环
"agent-core:register"      = ["admin:in"]
"admin:out"                = ["registry:in"]
"registry:out"             = ["agent-core:result"]
```

### 5.2 Router 的匹配逻辑

Router 不关心目标是"工具"还是"Agent"还是"Skill"。它只做一件事：**根据调用请求中的能力描述，匹配 Registry 中可用的节点。**

```
Supervisor 发出调用请求
    {
      "target": "file_rw",           // 直接指定节点 id
      "payload": { "action": "read", "path": "/workspace/foo.txt" }
    }
        │
        ▼
Router 查 Registry → 找到 node "file_rw" → 获取其 "in" 端口 → 路由消息
```

```
Supervisor 发出调用请求
    {
      "capability": "安全审计",      // 按能力描述匹配
      "payload": { "code": "..." }
    }
        │
        ▼
Router 查 Registry → 匹配 description 含 "安全" 的节点 → security_auditor → 路由消息
```

**匹配策略（按优先级）：**
1. `target` 字段直接指定 `node_id` → 精确匹配
2. `capability` 字段模糊匹配 `description` → 取最相关节点
3. 都未指定 → 返回 error

### 5.3 通道语义：统一的结果返回

所有被调用的节点，无论其 `runtime.kind` 是什么，结果都发往 `agent-core:result` 端口。Supervisor 不区分结果是来自 Python 工具、LLM 子节点、还是纯 Skill 模板——它只看到一个 JSON 结果。

```
agent-core:result  ←── file_rw:out         (Python 工具)
                   ←── shell_exec:out      (Python 工具)
                   ←── web_fetch:out       (Python 工具)
                   ←── security_auditor:out (LLM 子节点)
                   ←── router:error        (路由失败)
                   ←── registry:out        (自演进通知)
```

---

## 6. 启动流程

```
用户执行 sea
    │
    ▼
[1] 解析内嵌的 .group.toml → 获取所有 [node.*] 定义 + [channels] 绑定
    │
    ▼
[2] 控制面 + 数据面启动（tokio task）
    │
    ▼
[3] 遍历所有节点，按 runtime.kind 初始化：
    │
    ├── kind = "builtin"  → spawn tokio async task，传入 function 指针
    ├── kind = "llm"      → spawn tokio async task，传入 model + system_prompt
    ├── kind = "python"   → spawn Python 子进程 + bridge task
    └── kind = "skill"    → 仅写入 Registry 目录，无运行时
    │
    ▼
[4] 根据 [channels] 建立端口间 mpsc 通道
    │
    ▼
[5] 所有节点进入 READY → UI 打印欢迎信息
```

---

## 7. 典型对话流程

```
用户: "帮我用 Python 写一个查看 CPU 温度的脚本"

[1] ui:out → agent-core:in
    {"content": "帮我用 Python 写一个查看 CPU 温度的脚本",
     "content_type": "text/plain",
     "trace_id": "tr-001"}

[2] agent-core 推理 → 决定需要调用两个节点：shell_exec + file_rw
    ● 发送调用请求到 agent-core:call → router:in

[3] Router 收到 {"target": "shell_exec", "payload": {...}}
    → 查 Registry → 找到 node "shell_exec" → 路由到其 "in" 端口
    → shell_exec 执行 → 结果发回 agent-core:result

[4] Router 收到 {"target": "file_rw", "payload": {...}}
    → 同上流程 → file_rw 创建脚本文件 → 结果发回 agent-core:result

[5] agent-core 收集所有结果 → 生成最终回复

[6] agent-core:out → ui:in
    {"content": "已创建脚本 save_cpu_temp.py，运行方法如下...",
     "content_type": "text/markdown"}
    → UI 打印到终端
```

**关键对比：** 旧模型中，Router 需要区分 `tool_call` vs `agent_dispatch` 两种端口。新模型中，Router 只有一个 `out` 端口——它根据 Registry 查到的目标节点端口名直接路由，不区分调用目标是什么种类。

---

## 8. 自演进机制（节点的增删改查）

自演进不仅是"新增节点"，而是对系统节点池的完整生命周期管理——**查、增、改、删**四种操作都通过统一的提案→校验→落盘→生效闭环执行。

### 8.1 四种操作总览

| 操作 | 触发场景 | 副作用 |
|------|----------|--------|
| **查 (query)** | agent-core 定期/按需拉取可用节点列表 | 无，纯读取 |
| **增 (add)** | 发现能力缺口，需要新工具/Agent/Skill | 新增通道 + 启动运行时 |
| **改 (update)** | 节点行为需要调整（prompt 更新、代码优化、描述修正） | 热更新或重建运行时 |
| **删 (remove)** | 节点不再适用或被更好的替代 | 回收运行时 + 清理通道 |

### 8.2 查询：节点发现

agent-core 通过 Registry 查询当前所有可用节点的基本信息（id、description、ports），无需知道 runtime 细节。

```
agent-core 需要感知当前能力
    │
    ▼
发送 query 消息 → registry:query
    │
    ▼
Registry 返回节点目录（仅含外部三要素）
    │
    ▼
agent-core 将节点列表注入 system prompt，下次推理自动感知
```

**查询时机：** 每次对话开始时、自演进完成后、收到 Registry 变更通知时。

### 8.3 新增：创建节点

```
agent-core 发现能力不足 / 用户要求新能力
    │
    ▼
生成提案 JSON → agent-core:register → admin:in
    │
    ▼
Admin 校验（id 唯一性、上限检查、格式合法性）
    │
    ▼
auto_approve? → 是 → 自动通过
              → 否 → 等待人工确认
    │
    ▼
Admin 写入节点定义 → ReloadGroup
    │
    ├── runtime.kind = "skill"   → Registry 新增记录（无运行时）
    ├── runtime.kind = "llm"     → spawn 新 tokio task + 建立通道
    ├── runtime.kind = "python"  → spawn 新 Python 子进程 + bridge task
    └── runtime.kind = "builtin" → 拒绝（核心节点不允许动态创建）
    │
    ▼
Registry 更新节点目录 → agent-core 下次查询自动感知新节点
```

**新增提案格式：**

```json
{
  "operation": "add",
  "proposal_id": "prop-001",
  "proposed_by": "agent-core",
  "node": {
    "id": "db_query",
    "description": "数据库查询工具，支持 SQL 查询",
    "inputs":  [{ "port": "in",  "format": "application/json" }],
    "outputs": [{ "port": "out", "format": "application/json" }],
    "runtime": {
      "kind": "python",
      "code": "import sys, json, sqlite3\n...",
      "code_hash": "sha256:abc123..."
    },
    "channels": {
      "db_query:out": ["agent-core:result"]
    }
  },
  "reason": "用户频繁需要查询数据库，现有节点无此能力"
}
```

### 8.4 更新：修改已有节点

更新操作针对已有节点的属性进行增量修改。不同的 `target` 字段决定了变更范围和生效方式。

```
agent-core 发现某节点行为需要调整
    │
    ▼
生成更新提案 → agent-core:register → admin:in
    │
    ▼
Admin 校验（节点存在性、字段合法性、权限兼容性）
    │
    ▼
Admin 写入变更 → ReloadGroup → 按 target 字段执行
    │
    ├── target = "description"       → Registry 更新描述即可
    ├── target = "prompt"            → 控制面发送 RELOAD 信号热更新 LLM 节点的 system_prompt
    ├── target = "code"              → 终止旧 Python 子进程，spawn 新子进程 + bridge
    ├── target = "ports"             → 可能需要调整通道拓扑
    └── target = "channels"          → 数据面增量建立/拆除通道
    │
    ▼
Registry 同步变更 → agent-core 下次查询感知更新
```

**更新提案格式：**

```json
{
  "operation": "update",
  "proposal_id": "prop-002",
  "proposed_by": "agent-core",
  "node_id": "agent-core",
  "changes": {
    "target": "prompt",
    "new_system_prompt": "你是 SEA CLI 的核心助手。优先使用子节点处理复杂任务..."
  },
  "reason": "当前策略偏向直接回复，需要加强任务委托意识"
}
```

```json
{
  "operation": "update",
  "proposal_id": "prop-003",
  "proposed_by": "agent-core",
  "node_id": "web_fetch",
  "changes": {
    "target": "code",
    "new_code": "import sys, json, httpx\n...",
    "new_code_hash": "sha256:def456..."
  },
  "reason": "替换 requests 为 httpx，支持异步请求"
}
```

**更新安全约束：**
- 只能更新动态创建或内置 Python 节点的 `code`；核心 `builtin` 节点的逻辑不可动态修改
- Prompt 更新仅适用于 `runtime.kind = "llm"` 的节点
- Ports 变更需确保不破坏现有通道的格式兼容性
- 所有更新保留审计记录（旧值 → 新值）

### 8.5 删除：移除节点

```
agent-core 判断某节点不再需要 / 用户要求清理
    │
    ▼
生成删除提案 → agent-core:register → admin:in
    │
    ▼
Admin 校验（节点存在、非核心节点、无其他节点依赖）
    │
    ▼
确认后执行：
    │
    ├── runtime.kind = "skill"   → Registry 删除记录
    ├── runtime.kind = "llm"     → terminate tokio task + reap + 拆除通道
    └── runtime.kind = "python"  → terminate 子进程 + reap + 拆除通道 + 回收 bridge task
    │
    ▼
Registry 移除记录 → 清理 .group.toml 中的节点定义和通道声明
```

**删除提案格式：**

```json
{
  "operation": "remove",
  "proposal_id": "prop-004",
  "proposed_by": "agent-core",
  "node_id": "legacy_parser",
  "reason": "legacy_parser 已被 new_parser 完全替代，不再被任何节点引用"
}
```

**删除安全约束：**
- `builtin` 核心节点（ui、agent-core、router、admin、registry）**不可删除**
- 删除前检查依赖：如果有其他节点的通道引用该节点的端口，拒绝删除
- 删除 `python` 节点时确保子进程优雅终止（SIGTERM → 超时 → SIGKILL）

### 8.6 全局安全边界

| 约束 | 值 | 说明 |
|------|-----|------|
| `max_nodes` | 64 | 节点总数上限 |
| `max_llm_nodes` | 10 | LLM 节点上限（最昂贵） |
| `max_python_nodes` | 50 | Python 节点上限 |
| 动态 Python 节点 | 强制 `separate_process` | SHA256 校验 |
| 动态 LLM 节点 | 仅 `channel_send` 权限 | 无 `tool_call` 和 `dynamic_spawn` |
| 核心节点保护 | `builtin` 节点不可增/删 | 仅允许 prompt 更新 |
| 依赖检查 | 删除前验证引用链 | 防止悬空通道 |
| 审计溯源 | 所有操作记录 `proposed_by` + 前后值 | 可回溯 |

---

## 9. 进程间通信

### 9.1 进程内通信（tokio async task — mpsc channel）

节点全部运行在同一个 Rust 进程中，消息通过 `tokio::sync::mpsc` 通道传递。每个通道是有界 mpsc channel（容量 256），`send` 非阻塞，`recv` async 等待。Rust 所有权系统保证消息转移时零拷贝。

### 9.2 跨进程通信（Python 节点）

每个 `runtime.kind = "python"` 的节点通过 stdin/stdout JSON Lines 协议与 Runtime 的 bridge task 通信：

```
Runtime bridge task (Rust)        Python 子进程
         │                              │
         │── mpsc recv ← Router ──      │
         │──── JSON Line → stdin ────→  │  {"payload": "..."}
         │←── JSON Line ← stdout ────  │  {"payload": "..."}
         │── mpsc send → agent-core ──  │
```

### 9.3 消息格式

统一 JSON 信封：

```json
{
  "trace_id": "tr-001",
  "content_type": "application/json",
  "content": { "tool_name": "file_rw", "action": "read", "path": "/workspace/foo.txt" }
}
```

---

## 10. 设计原则总结

| 原则 | 实现 |
|------|------|
| **节点不区分类型** | 外部只看到 id、description、ports。runtime 是实现细节 |
| **Router 不关心种类** | 按能力匹配路由，不区分"工具"和"Agent" |
| **结果统一返回** | 所有节点的输出汇入同一个 result 端口 |
| **通道即拓扑** | 端口绑定描述整个系统的连接关系 |
| **自演进即节点 CRUD** | 增删改查四种操作统一为提案→校验→落盘→生效闭环 |
| **描述驱动发现** | Supervisor 通过节点描述理解可用能力，不做类型推断 |

---

## 11. 核心约束：永远简单的三样东西

| 东西 | 约束 |
|------|------|
| **Node** | 一个节点 = id + description + ports + runtime。外部视角只暴露前三者 |
| **Message** | 一个消息 = 一段 JSON。content_type 标注格式，其余全是载荷 |
| **Channel** | 一条通道 = source_port → target_port 的绑定。与节点内部实现无关 |

---

## 12. 与 IFP 规范的对齐

| IFP 原语 | SEA CLI 中的使用 |
|----------|------------------|
| `spawn` | 启动时创建所有节点；自演进时创建新节点 |
| `terminate` | 用户退出时优雅关闭所有节点 |
| `reap` | 退出时回收资源；Python 子进程退出后回收 |
| `isolation` | `builtin`/`llm` 节点 `in_process`；`python` 节点 `separate_process` |
| `channel` | 所有通信通过端口绑定声明 |
| `signal_notify` | PAUSE/RESUME 流控，RELOAD 配置热更新 |
| `identity` | 每个节点绑定唯一的 principal |
| `access_check` | 每次通道发送前 Runtime 自动执行 |
| `privilege_transition` | 推理完成后 drop 调用权限 |
| `dynamic_spawn` | 自演进场景创建新 Python 子进程 |
