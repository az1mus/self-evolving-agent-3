# SEA CLI — 关键子程序伪代码设计

> 基于 SEA_CORE.md (统一 Node 模型) + IFP_CORE v3.1  
> 日期: 2026-07-04

---

## 目录

1. [IFP Runtime 主程序](#1-ifp-runtime-主程序)
2. [节点生命周期管理器 (Node Manager)](#2-节点生命周期管理器-node-manager)
3. [Registry 节点目录服务](#3-registry-节点目录服务)
4. [Router 消息调度分发](#4-router-消息调度分发)
5. [Channel Switch 通道交换机](#5-channel-switch-通道交换机)
6. [Admin 自演进管理](#6-admin-自演进管理)
7. [Bridge Task Python 子进程桥接](#7-bridge-task-python-子进程桥接)
8. [UI 终端交互循环](#8-ui-终端交互循环)
9. [Reload Group 增量热更新](#9-reload-group-增量热更新)
10. [Access Check 策略判定](#10-access-check-策略判定)
11. [信号处理与流控](#11-信号处理与流控)
12. [自演进闭环 (Self-Evolution)](#12-自演进闭环-self-evolution)
13. [项目结构与模块规划](#13-项目结构与模块规划)

---

## 1. IFP Runtime 主程序

### 职责

系统唯一 Rust 进程入口。解析 `.group.toml`，初始化控制面与数据面，启动所有节点，等待退出。

### 伪代码

```
FUNCTION main():
    // ── 1. 解析内置 .group.toml ──
    group_config = parse_group_toml(EMBEDDED_GROUP_TOML)
    IF group_config.is_error():
        PRINT("FATAL: 无法解析 .group.toml: " + group_config.error)
        EXIT(1)
    group_config = group_config.unwrap()
    
    // ── 2. 初始化 IFP Runtime ──
    runtime = IFPRuntime.new()
    
    // ── 3. 启动控制面 ──
    control_plane = ControlPlane.new(runtime)
    control_plane.start()  // tokio::spawn
    
    // ── 4. 启动数据面 ──
    data_plane = DataPlane.new(runtime, channel_capacity=256)
    data_plane.start()     // tokio::spawn
    
    // ── 5. 初始化节点管理器 ──
    node_manager = NodeManager.new(control_plane, data_plane)
    
    // ── 6. 先建立通道拓扑（节点启动前通道就绪） ──
    FOR EACH (source, targets) IN group_config.channels:
        FOR EACH target IN targets:
            data_plane.channel_create(source, target)
    
    // ── 7. 再初始化所有节点（通道已就绪） ──
    FOR EACH node_def IN group_config.nodes:
        node_manager.spawn(node_def)
    
    // ── 8. 控制面发送启动审计 ──
    control_plane.audit_event("RuntimeStart", {
        "node_count": len(group_config.nodes),
        "channel_count": len(group_config.channels)
    })
    
    // ── 9. 等待终止信号 ──
    signal = wait_for_shutdown_signal()  // Ctrl+C / SIGTERM
    
    // ── 10. 优雅关闭 ──
    node_manager.shutdown_all()
    data_plane.shutdown()
    control_plane.shutdown()
    runtime.shutdown()
    
    PRINT("sea: goodbye")
    EXIT(0)


CLASS IFPRuntime:
    // 单例，系统核心状态
    FIELD config: GroupConfig
    FIELD node_manager: NodeManager
    FIELD control_plane: ControlPlane
    FIELD data_plane: DataPlane
    
    METHOD new():
        self.config = null
        self.node_manager = null
        self.control_plane = null
        self.data_plane = null
    
    METHOD shutdown():
        // 释放所有资源
        PASS
```

---

## 2. 节点生命周期管理器 (Node Manager)

### 职责

统一管理所有节点的 spawn / terminate / reap 操作。根据 runtime.kind 决定创建方式，维护节点状态机。

### 伪代码

```
CLASS NodeManager:
    FIELD nodes: HashMap<String, NodeInstance>
    FIELD control_plane: ControlPlane
    FIELD data_plane: DataPlane
    FIELD audit_logger: AuditLogger
    
    METHOD new(cp, dp):
        self.nodes = empty HashMap
        self.control_plane = cp
        self.data_plane = dp
        self.audit_logger = AuditLogger.new()
    
    
    // ── spawn: 根据 runtime.kind 分支创建 ──
    METHOD spawn(node_def: NodeDef) -> Result<NodeId, Error>:
        IF self.nodes.contains(node_def.id):
            RETURN Error("节点已存在: " + node_def.id)
        
        IF self.nodes.len() >= MAX_NODES:
            RETURN Error("节点总数超限 (max=64)")
        
        instance = NodeInstance {
            id: node_def.id,
            state: CREATING,
            runtime_kind: node_def.runtime.kind,
            identity: Identity.new(node_def.id),
            handle: null,
            created_at: now()
        }
        
        SWITCH node_def.runtime.kind:
            CASE "builtin":
                function_ptr = lookup_builtin(node_def.runtime.function)
                handle = tokio::spawn(async_task(function_ptr))
                instance.handle = InProcessHandle(handle)
                
            CASE "llm":
                handle = tokio::spawn(llm_task(node_def.runtime))
                instance.handle = InProcessHandle(handle)
                
            CASE "python":
                // 启动 Python 子进程 + Bridge task
                child = spawn_python_process(
                    command=node_def.runtime.command,
                    args=node_def.runtime.args
                )
                bridge = BridgeTask.new(
                    child, 
                    node_def.id,
                    data_plane
                )
                bridge.start()
                instance.handle = SeparateProcessHandle(child, bridge)
                
            CASE "skill":
                // 无运行时，仅注册到 Registry
                instance.handle = null  // 纯数据，不需要运行时
                
            CASE ELSE:
                RETURN Error("未知 runtime.kind: " + node_def.runtime.kind)
        
        // ── 身份绑定 ──
        self.control_plane.identity_bind(instance.id, instance.identity)
        
        instance.state = RUNNING
        self.nodes.insert(node_def.id, instance)
        
        // ── 审计 ──
        self.audit_logger.log("ServiceStart", {
            "node_id": node_def.id,
            "kind": node_def.runtime.kind
        })
        
        RETURN Ok(node_def.id)
    
    
    // ── terminate: 停止节点 ──
    METHOD terminate(node_id: String, signal: Signal) -> Result<Void, Error>:
        instance = self.nodes.get(node_id)
        IF instance IS None:
            RETURN Error("节点不存在: " + node_id)
        
        IF instance.state == STOPPED:
            RETURN Ok()  // 已经停止
        
        instance.state = STOPPING
        
        SWITCH instance.handle.type:
            CASE InProcessHandle:
                IF signal == KILL:
                    instance.handle.task.abort()
                ELSE:
                    instance.handle.task.send_signal(TERMINATE)
                
            CASE SeparateProcessHandle:
                IF signal == KILL:
                    instance.handle.child.kill()
                ELSE:
                    instance.handle.child.send_signal("TERMINATE")
                    // 等待超时后强制 kill
                    timeout(5000ms, lambda:
                        IF instance.handle.child.is_alive():
                            instance.handle.child.kill()
                    )
                
            CASE Null:
                PASS  // skill 类型，无运行时
        
        instance.state = STOPPED
        self.audit_logger.log("ServiceExit", {
            "node_id": node_id,
            "signal": signal
        })
        
        RETURN Ok()
    
    
    // ── reap: 回收已停止节点的资源 ──
    METHOD reap(node_id: String) -> Result<ExitStatus, Error>:
        instance = self.nodes.get(node_id)
        IF instance IS None:
            RETURN Error("节点不存在: " + node_id)
        
        IF instance.state != STOPPED:
            RETURN Error("节点未停止，不能 reap")
        
        exit_status = null
        
        SWITCH instance.handle.type:
            CASE InProcessHandle:
                exit_status = instance.handle.task.await()
                instance.handle.task = null
                
            CASE SeparateProcessHandle:
                exit_code = instance.handle.child.wait()
                instance.handle.bridge.shutdown()
                exit_status = ExitStatus(exit_code)
                
            CASE Null:
                exit_status = ExitStatus(0)
        
        // 拆除通道
        self.data_plane.remove_all_channels_for(node_id)
        
        // 从 Registry 移除
        self.control_plane.registry_remove(node_id)
        
        // 从节点管理器移除
        self.nodes.remove(node_id)
        
        RETURN Ok(exit_status)
    
    
    // ── shutdown_all: 优雅关闭所有节点 ──
    METHOD shutdown_all():
        // 先终止所有 Python 子进程（可能最慢）
        FOR EACH (id, instance) IN self.nodes:
            IF instance.handle.type == SeparateProcessHandle:
                self.terminate(id, TERMINATE)
        
        WAIT 3000ms  // 给子进程时间优雅退出
        
        // 再终止所有 in_process 节点
        FOR EACH (id, instance) IN self.nodes:
            IF instance.handle.type == InProcessHandle:
                self.terminate(id, TERMINATE)
        
        // 全部 reap：先收集 keys 避免迭代中修改集合
        all_ids = self.nodes.keys().collect()
        FOR EACH id IN all_ids:
            self.reap(id)
    
    
    // ── dynamic_spawn: 运行时动态创建节点 ──
    METHOD dynamic_spawn(spec: DynamicSpawnSpec) -> Result<NodeId, Error>:
        // 校验哈希
        actual_hash = sha256(spec.executable_source.content)
        IF actual_hash != spec.content_hash:
            RETURN Error("内容哈希不匹配")
        
        // 强制 separate_process
        IF spec.isolation != "separate_process":
            RETURN Error("动态节点必须使用 separate_process 隔离模式")
        
        // 构造临时节点定义
        temp_def = NodeDef {
            id: generate_unique_id("dyn_"),
            description: spec.description,
            runtime: RuntimeDef {
                kind: "python",
                code: spec.executable_source.content,
                isolation: "separate_process",
                max_runtime_ms: spec.max_runtime_ms,
                output_limit_bytes: spec.output_limit_bytes
            },
            channels: {
                temp_def.id + ":out": [spec.reply_channel]
            }
        }
        
        id = self.spawn(temp_def)
        
        // 注册自动超时终止
        tokio::spawn_after(spec.max_runtime_ms, lambda:
            IF self.nodes.contains(id) AND self.nodes[id].state == RUNNING:
                self.terminate(id, KILL)
                self.audit_logger.log("DynamicSpawnTimeout", {
                    "node_id": id,
                    "max_runtime_ms": spec.max_runtime_ms
                })
        )
        
        RETURN Ok(id)
```

---

## 3. Registry 节点目录服务

### 职责

存储所有节点的 **外部三要素**（id, description, ports），不保存 runtime 细节。支持查询、注册、注销。

### 伪代码

```
CLASS Registry:
    // 核心存储：仅外部三要素
    FIELD catalog: RwLock<HashMap<NodeId, NodeInfo>>
    
    // NodeInfo = 外部视角的节点描述
    STRUCT NodeInfo:
        id: String
        description: String
        inputs: List<PortDecl>
        outputs: List<PortDecl>
        created_at: Timestamp
    
    STRUCT PortDecl:
        port: String
        format: String  // MIME type
    
    
    METHOD new():
        self.catalog = RwLock::new(empty HashMap)
    
    
    // ── 注册节点 ──
    METHOD register(node_def: NodeDef) -> Result<Void, Error>:
        info = NodeInfo {
            id: node_def.id,
            description: node_def.description,
            inputs: node_def.inputs,
            outputs: node_def.outputs,
            created_at: now()
        }
        
        self.catalog.write().insert(node_def.id, info)
        RETURN Ok()
    
    
    // ── 注销节点 ──
    METHOD unregister(node_id: String) -> Result<Void, Error>:
        self.catalog.write().remove(node_id)
        RETURN Ok()
    
    
    // ── 查：按 id 精确查询 ──
    METHOD query_by_id(node_id: String) -> Result<NodeInfo, Error>:
        info = self.catalog.read().get(node_id)
        IF info IS None:
            RETURN Error("节点未注册: " + node_id)
        RETURN Ok(info)
    
    
    // ── 查：按能力描述模糊匹配 ──
    METHOD query_by_capability(capability: String) -> Result<List<NodeInfo>, Error>:
        results = empty List
        FOR EACH info IN self.catalog.read().values():
            // 简单关键词匹配，后续可升级为语义匹配
            IF info.description.contains(capability):
                results.push(info)
        
        // 按匹配度排序：关键字在描述开头 > 描述越短匹配度越高
        results.sort_by(lambda a, b:
            a.description.find(capability) <= b.description.find(capability)
        )
        results.sort_by(lambda a, b:
            len(a.description) - len(b.description)
        )
        
        RETURN Ok(results)
    
    
    // ── 查：列出全部节点（注入 Supervisor prompt 用） ──
    METHOD list_all() -> List<NodeInfo>:
        RETURN self.catalog.read().values().copy()
    
    
    // ── 查：生成供 LLM 注入的文本摘要 ──
    METHOD generate_summary() -> String:
        summary_lines = []
        FOR EACH info IN self.catalog.read().values().sorted_by(id):
            inputs_desc = info.inputs.map(p => p.port+"("+p.format+")").join(", ")
            outputs_desc = info.outputs.map(p => p.port+"("+p.format+")").join(", ")
            line = "- `" + info.id + "`: " + info.description
                + " [in: " + inputs_desc + "] [out: " + outputs_desc + "]"
            summary_lines.push(line)
        
        RETURN "## 可用节点\n\n" + summary_lines.join("\n")
```

---

## 4. Router 消息调度分发

### 职责

无状态消息调度。接收 agent-core 发起的调用请求，按 `target` 或 `capability` 匹配目标节点，路由消息到目标节点的 in 端口。

### 伪代码

```
CLASS Router:
    FIELD registry: Registry
    FIELD data_plane: DataPlane
    
    // 输入端口：接收 agent-core 的调用请求
    PORT in:  channel "agent-core:call" -> "router:in"
    // 输出端口：路由到目标节点（目标由路由逻辑动态决定）
    PORT out: channel "router:out" -> "router:out"  // 动态路由，不绑具体节点
    // 错误端口：返回无法路由的错误
    PORT error: channel "router:error" -> "agent-core:result"
    
    
    METHOD new(registry, data_plane):
        self.registry = registry
        self.data_plane = data_plane
    
    
    // ── 主循环：等待调用请求并分发 ──
    ASYNC METHOD run():
        LOOP:
            message = WAIT self.in.recv()
            
            result = self.dispatch(message)
            
            IF result.is_error():
                self.error.send({
                    "trace_id": message.trace_id,
                    "in_response_to": message.message_id,
                    "content": {
                        "status": "error",
                        "error": result.error,
                        "original_payload": message.content
                    },
                    "content_type": "application/json"
                })
    
    
    // ── 调度逻辑 ──
    METHOD dispatch(message: Message) -> Result<Void, Error>:
        payload = message.content  // JSON object
        
        // 优先级 1: 精确 target
        IF payload contains "target":
            target_id = payload.target
            node_info = self.registry.query_by_id(target_id)
            
            IF node_info.is_error():
                RETURN Error("目标节点不存在: " + target_id)
            
            // 路由到目标节点的 in 端口
            self.route(target_id, "in", message)
            RETURN Ok()
        
        // 优先级 2: 按能力描述
        IF payload contains "capability":
            matches = self.registry.query_by_capability(payload.capability)
            
            IF matches.is_empty():
                RETURN Error("没有节点能处理能力: " + payload.capability)
            
            // 取最匹配（排序后第一个）
            best_match = matches[0]
            self.route(best_match.id, "in", message)
            RETURN Ok()
        
        // 都未指定
        RETURN Error("调用请求必须包含 target 或 capability 字段")
    
    
    // ── 路由执行 ──
    METHOD route(target_id: String, port: String, message: Message):
        channel_key = target_id + ":" + port
        
        // 构造路由后的消息（保留 trace_id）
        routed_msg = Message {
            trace_id: message.trace_id,
            message_id: generate_uuid(),
            in_response_to: message.message_id,
            content: message.content.payload,  // 只传递 payload
            content_type: message.content_type,
            created_at: now()
        }
        
        // 通过数据面的通道发送
        self.data_plane.send(channel_key, routed_msg)
    
    
    // ── 获取目标节点端口 ──
    METHOD resolve_target_port(node_id: String) -> String:
        // 默认路由到 "in" 端口
        // 后续可扩展：根据消息 content_type 匹配端口 format
        RETURN "in"
```

---

## 5. Channel Switch 通道交换机

### 职责

数据面核心组件。管理所有端口间的 mpsc 通道，执行消息转发、背压检测、死信处理。

### 伪代码

```
CLASS ChannelSwitch:
    // 拓扑表: "source_node:port" -> List<Channel>
    FIELD topology: RwLock<HashMap<String, List<Channel>>>
    
    // 所有活跃通道
    FIELD channels: HashMap<ChannelId, Channel>
    
    // 默认容量
    CONST DEFAULT_CAPACITY = 256
    
    STRUCT Channel:
        id: ChannelId
        source: String      // "node_id:port"
        target: String      // "node_id:port"
        tx: mpsc::Sender
        rx: mpsc::Receiver  // (只在目标节点侧持有)
        buffer_usage: Arc<AtomicUint>  // 发送/接收两侧共享
        created_at: Timestamp
    
    STRUCT ChannelId:
        source: String
        target: String  // 全局唯一: "source->target"
    
    
    METHOD new():
        self.topology = RwLock::new(empty HashMap)
        self.channels = empty HashMap
    
    
    // ── 建立通道 ──
    METHOD create(source: String, target: String) -> Result<ChannelId, Error>:
        channel_id = ChannelId(source, target)
        
        IF self.channels.contains(channel_id):
            RETURN Error("通道已存在: " + channel_id)
        
        // 创建有界 mpsc channel
        (tx, rx) = mpsc::channel(DEFAULT_CAPACITY)
        
        // 共享的 buffer_usage 计数器（发送/接收两侧均可访问）
        shared_buffer_usage = Arc::new(AtomicUint::new(0))
        
        channel = Channel {
            id: channel_id,
            source: source,
            target: target,
            tx: tx,
            rx: rx,
            buffer_usage: shared_buffer_usage.clone(),
            created_at: now()
        }
        
        // 写入拓扑表
        self.topology.write()
            .entry(source)
            .or_insert(empty List)
            .push(channel)
        
        self.channels.insert(channel_id, channel)
        
        // 将 rx 注册到目标节点的消息循环，同时传入 buffer_usage 用于 decrement
        self.bind_receiver(target, rx, shared_buffer_usage)
        
        RETURN Ok(channel_id)
    
    
    // ── 拆除通道 ──
    METHOD remove(channel_id: ChannelId) -> Result<Void, Error>:
        channel = self.channels.remove(channel_id)
        IF channel IS None:
            RETURN Error("通道不存在")
        
        // 从拓扑表移除
        source_channels = self.topology.write().get(channel.source)
        IF source_channels IS NOT None:
            source_channels.remove_all(c => c.id == channel_id)
            IF source_channels.is_empty():
                self.topology.write().remove(channel.source)
        
        // 关闭 sender（receiver 会收到 None）
        channel.tx.close()
        
        // 审计
        self.audit("ChannelRemoved", {
            "source": channel.source,
            "target": channel.target
        })
        
        RETURN Ok()
    
    
    // ── 发送消息 ──
    METHOD send(source: String, message: Message) -> Result<Void, Error>:
        channels = self.topology.read().get(source)
        
        IF channels IS None OR channels.is_empty():
            // 无通道 = 没有接收方
            RETURN Error("没有从 " + source + " 出发的通道")
        
        // 扇出：一个源端口可连接多个目标
        FOR EACH channel IN channels:
            // 异步非阻塞发送
            result = channel.tx.try_send(message.clone())
            
            SWITCH result:
                CASE Ok(()):
                    channel.buffer_usage.fetch_add(1)
                    
                    // 背压检测
                    usage = channel.buffer_usage.load()
                    IF usage > DEFAULT_CAPACITY * 0.8:
                        self.emit_backpressure(
                            channel, usage, "high"
                        )
                    ELSE IF usage > DEFAULT_CAPACITY * 0.5:
                        self.emit_backpressure(
                            channel, usage, "medium"
                        )
                
                CASE Err(TrySendError::Full(msg)):
                    // 通道满，进入死信
                    self.dead_letter(msg, "channel_full")
                    
                    self.emit_backpressure(
                        channel, DEFAULT_CAPACITY, "critical"
                    )
                    
                    self.audit("ChannelError", {
                        "channel_id": channel.id,
                        "error": "channel_full"
                    })
                
                CASE Err(TrySendError::Closed(msg)):
                    // 通道已关闭（目标节点已停止）
                    self.dead_letter(msg, "channel_broken")
                    
                    self.audit("ChannelError", {
                        "channel_id": channel.id,
                        "error": "channel_broken"
                    })
        
        RETURN Ok()
    
    
    // ── 移除与特定节点相关的所有通道 ──
    METHOD remove_all_channels_for(node_id: String):
        // 先收集 keys 避免迭代中修改集合
        all_ids = self.channels.keys().collect()
        FOR EACH channel_id IN all_ids:
            source_node = channel_id.source.split(":")[0]
            target_node = channel_id.target.split(":")[0]
            
            IF source_node == node_id OR target_node == node_id:
                self.remove(channel_id)
    
    
    // ── 背压通知 ──
    METHOD emit_backpressure(channel: Channel, depth: Uint, level: String):
        backpressure_msg = Backpressure {
            source_service: channel.source,
            channel_id: channel.id,
            level: level,
            queue_depth: depth,
            suggested_pause_ms: SWITCH level:
                CASE "low":    0
                CASE "medium": 100
                CASE "high":   500
                CASE "critical": 2000
        }
        
        // 通过信号通道发送给源节点
        self.signal_channel.send(channel.source, backpressure_msg)
    
    
    // ── 死信处理 ──
    METHOD dead_letter(message: Message, reason: String):
        dead_letter_entry = DeadLetter {
            original_message: message,
            reason: reason,
            timestamp: now(),
            context: {}
        }
        
        // 如果配置了死信汇集端口，转发到那里（try_send 避免阻塞）
        IF self.dead_letter_channel IS NOT None:
            result = self.dead_letter_channel.try_send(dead_letter_entry)
            IF result.is_error():
                // 死信通道也满时静默丢弃（防止级联阻塞）
                self.audit("DeadLetterDropped", {
                    "message_id": message.message_id,
                    "trace_id": message.trace_id,
                    "reason": "dead_letter_channel_full"
                })
        
        // 记录审计
        self.audit("DeadLetter", {
            "message_id": message.message_id,
            "trace_id": message.trace_id,
            "reason": reason
        })
```

---

## 6. Admin 自演进管理

### 职责

接收自演进提案，校验合法性，落盘到 `.group.toml`，触发增量生效（ReloadGroup）。

### 伪代码

```
CLASS Admin:
    FIELD config_path: String        // .group.toml 路径
    FIELD node_manager: NodeManager
    FIELD registry: Registry
    FIELD data_plane: DataPlane      // 用于 channel_create
    FIELD auto_approve: Bool         // 是否自动审批
    
    // kind 计数缓存（避免每次都读磁盘）
    FIELD kind_cache: RwLock<HashMap<String, Int>>
    
    // 安全约束常量
    CONST MAX_NODES = 64
    CONST MAX_LLM_NODES = 10
    CONST MAX_PYTHON_NODES = 50
    CONST PROTECTED_NODES = ["ui", "agent-core", "router", "admin", "registry"]
    
    PORT in:  channel "agent-core:register" -> "admin:in"
    PORT out: channel "admin:out" -> "registry:in"
    
    
    METHOD new(config_path, node_manager, registry, data_plane, auto_approve):
        self.config_path = config_path
        self.node_manager = node_manager
        self.registry = registry
        self.data_plane = data_plane
        self.auto_approve = auto_approve
        self.kind_cache = RwLock::new(empty HashMap)
        // 启动时从完整节点初始化 kind_cache
        self.rebuild_kind_cache()
    
    
    // ── 主循环 ──
    ASYNC METHOD run():
        LOOP:
            message = WAIT self.in.recv()
            proposal = message.content  // 提案 JSON
            
            result = self.process_proposal(proposal)
            
            // 回复结果
            self.out.send({
                "trace_id": message.trace_id,
                "in_response_to": message.message_id,
                "content": result,
                "content_type": "application/json"
            })
    
    
    // ── 提案处理入口 ──
    METHOD process_proposal(proposal: Proposal) -> ProposalResult:
        // 校验 proposed_by：只接受已知节点的合法请求
        IF NOT self.is_valid_proposer(proposal.proposed_by):
            RETURN ProposalError("非法提案者: " + proposal.proposed_by)
        
        SWITCH proposal.operation:
            CASE "add":
                RETURN self.proposal_add(proposal)
            CASE "update":
                RETURN self.proposal_update(proposal)
            CASE "remove":
                RETURN self.proposal_remove(proposal)
            CASE ELSE:
                RETURN ProposalError("未知操作类型: " + proposal.operation)
    
    
    // ── 新增提案 ──
    METHOD proposal_add(proposal: Proposal) -> ProposalResult:
        node = proposal.node
        
        // === 校验 ===
        // 1) id 唯一性
        IF self.registry.query_by_id(node.id).is_ok():
            RETURN ProposalError("节点 id 已存在: " + node.id)
        
        // 2) 节点总数上限
        total_count = self.registry.list_all().len()
        IF total_count >= MAX_NODES:
            RETURN ProposalError("节点总数已达上限 (" + MAX_NODES + ")")
        
        // 3) 按 kind 检查独立上限
        SWITCH node.runtime.kind:
            CASE "llm":
                llm_count = self.count_by_kind("llm")
                IF llm_count >= MAX_LLM_NODES:
                    RETURN ProposalError("LLM 节点已达上限 (" + MAX_LLM_NODES + ")")
            CASE "python":
                py_count = self.count_by_kind("python")
                IF py_count >= MAX_PYTHON_NODES:
                    RETURN ProposalError("Python 节点已达上限 (" + MAX_PYTHON_NODES + ")")
            CASE "builtin":
                RETURN ProposalError("不允许动态创建 builtin 核心节点")
        
        // 4) 格式合法性
        IF node.inputs.is_empty() AND node.runtime.kind != "skill":
            RETURN ProposalError("非 skill 节点至少需要一个输入端口")
        
        IF node.outputs.is_empty():
            RETURN ProposalError("节点至少需要一个输出端口")
        
        // === 审批 ===
        IF NOT self.auto_approve:
            approved = WAIT self.request_human_approval(proposal)
            IF NOT approved:
                RETURN ProposalRejected("人工审批未通过")
        
        // === 执行（先 spawn 再写文件，确保运行时成功才持久化） ===
        // 1) 调用 NodeManager.spawn
        spawn_result = self.node_manager.spawn(node)
        IF spawn_result.is_error():
            RETURN ProposalError("节点启动失败: " + spawn_result.error)
        
        // 2) 建立通道
        IF node.channels IS NOT None:
            FOR EACH (source, targets) IN node.channels:
                FOR EACH target IN targets:
                    self.data_plane.channel_create(source, target)
        
        // 3) 注册到 Registry
        self.registry.register(node)
        
        // 4) 将节点定义写入 .group.toml（spawn 成功后持久化）
        self.append_to_group_toml(node)
        
        // 5) 更新 kind 缓存
        self.kind_cache.write().insert(node.id, node.runtime.kind)
        
        RETURN ProposalAccepted {
            "node_id": node.id,
            "proposal_id": proposal.proposal_id
        }
    
    
    // ── 更新提案 ──
    METHOD proposal_update(proposal: Proposal) -> ProposalResult:
        node_id = proposal.node_id
        
        // 1) 检查节点存在
        node_info = self.registry.query_by_id(node_id)
        IF node_info.is_error():
            RETURN ProposalError("节点不存在: " + node_id)
        
        // 2) 获取当前节点定义（需要从 group config 中读取）
        current_def = self.get_node_def(node_id)
        
        SWITCH proposal.changes.target:
            CASE "description":
                // 仅更新 Registry
                self.registry.update_description(node_id, proposal.changes.new_description)
                self.update_in_group_toml(node_id, "description", proposal.changes.new_description)
                
            CASE "prompt":
                // 仅对 LLM 节点有效
                IF current_def.runtime.kind != "llm":
                    RETURN ProposalError("prompt 更新仅适用于 LLM 节点")
                
                // 发送 RELOAD 信号
                self.node_manager.send_signal(node_id, RELOAD, {
                    "new_system_prompt": proposal.changes.new_system_prompt
                })
                self.update_in_group_toml(node_id, "runtime.system_prompt", 
                    proposal.changes.new_system_prompt)
                
            CASE "code":
                // 仅对 Python 节点有效
                
                // 记录旧值用于审计
                old_code = current_def.runtime.code
                
                // 终止旧进程
                self.node_manager.terminate(node_id, TERMINATE)
                self.node_manager.reap(node_id)
                
                // 更新代码
                current_def.runtime.code = proposal.changes.new_code
                if proposal.changes contains "new_code_hash":
                    current_def.runtime.code_hash = proposal.changes.new_code_hash
                
                // 重新 spawn
                self.node_manager.spawn(current_def)
                
                // 记录审计
                self.audit("CodeUpdate", {
                    "node_id": node_id,
                    "old_hash": sha256(old_code),
                    "new_hash": sha256(proposal.changes.new_code)
                })
                
            CASE "ports":
                // 检查格式兼容性（防止破坏通道）
                NEW_SETS = validator.check_port_compatibility(
                    current_def, proposal.changes
                )
                IF NOT NEW_SETS.is_ok():
                    RETURN ProposalError("端口变更不兼容: " + NEW_SETS.error)
                
                // 更新定义
                self.update_in_group_toml(node_id, "inputs", proposal.changes.new_inputs)
                self.update_in_group_toml(node_id, "outputs", proposal.changes.new_outputs)
                self.registry.update_ports(node_id, 
                    proposal.changes.new_inputs, 
                    proposal.changes.new_outputs)
                
            CASE "channels":
                // 增量拆除旧通道
                FOR EACH (source, target) IN proposal.changes.remove_channels:
                    self.data_plane.channel_remove(source, target)
                
                // 增量建立新通道
                FOR EACH (source, targets) IN proposal.changes.add_channels:
                    FOR EACH target IN targets:
                        self.data_plane.channel_create(source, target)
                
                // 更新 group.toml 中的 channels 段
                self.update_channels_in_group_toml(
                    proposal.changes.add_channels,
                    proposal.changes.remove_channels
                )
        
        RETURN ProposalAccepted {
            "node_id": node_id,
            "proposal_id": proposal.proposal_id
        }
    
    
    // ── 删除提案 ──
    METHOD proposal_remove(proposal: Proposal) -> ProposalResult:
        node_id = proposal.node_id
        
        // 1) 检查节点存在
        node_info = self.registry.query_by_id(node_id)
        IF node_info.is_error():
            RETURN ProposalError("节点不存在: " + node_id)
        
        // 2) 检查是否为受保护的核心节点
        IF node_id IN PROTECTED_NODES:
            RETURN ProposalError("核心节点不可删除: " + node_id)
        
        // 3) 检查依赖：是否有其他节点的通道引用此节点
        dependencies = self.check_dependencies(node_id)
        IF NOT dependencies.is_empty():
            RETURN ProposalError("以下节点依赖 " + node_id + "，请先删除依赖: " 
                + dependencies.join(", "))
        
        // 4) 获取节点定义（用于审计）
        current_def = self.get_node_def(node_id)
        
        // 5) 执行删除
        self.node_manager.terminate(node_id, TERMINATE)
        self.node_manager.reap(node_id)
        
        // 6) 从 .group.toml 移除
        self.remove_from_group_toml(node_id)
        
        // 7) 清理通道
        self.data_plane.remove_all_channels_for(node_id)
        
        // 8) Registry 注销
        self.registry.unregister(node_id)
        
        // 9) 更新 kind 缓存
        self.kind_cache.write().remove(node_id)
        
        RETURN ProposalAccepted {
            "node_id": node_id,
            "proposal_id": proposal.proposal_id
        }
    
    
    // ── 依赖检查 ──
    METHOD check_dependencies(node_id: String) -> List<String>:
        dependents = empty List
        all_nodes = self.registry.list_all()
        
        FOR EACH node IN all_nodes:
            // 从 .group.toml 的 channels 声明中查找
            channels = self.get_channels_for(node.id)
            IF channels.contains_ref_to(node_id):
                dependents.push(node.id)
        
        RETURN dependents
    
    
    // ── 按 runtime.kind 计数（使用缓存，避免重复读磁盘） ──
    METHOD count_by_kind(kind: String) -> Int:
        count = 0
        FOR EACH (node_id, node_kind) IN self.kind_cache.read():
            IF node_kind == kind:
                count++
        RETURN count
    
    // ── 重建 kind 缓存（启动时或 Reload 后调用） ──
    METHOD rebuild_kind_cache():
        new_cache = empty HashMap
        FOR EACH node IN self.registry.list_all():
            node_def = self.get_node_def(node.id)
            new_cache.insert(node.id, node_def.runtime.kind)
        self.kind_cache = RwLock::new(new_cache)
    
    // ── 校验提案者身份 ──
    METHOD is_valid_proposer(proposed_by: String) -> Bool:
        // 只允许已知节点和人工操作发起提案
        RETURN self.registry.query_by_id(proposed_by).is_ok()
            OR proposed_by == "human"
```

---

## 7. Bridge Task Python 子进程桥接

### 职责

每个 `runtime.kind = "python"` 的节点对应一个 Bridge task。负责 Rust mpsc channel ↔ Python stdin/stdout JSON Lines 的双向转换。

### 伪代码

```
CLASS BridgeTask:
    FIELD node_id: String
    FIELD child: ChildProcess          // Python 子进程
    FIELD data_plane: DataPlane
    FIELD input_channel: mpsc::Receiver  // 从 Router 来的消息
    FIELD output_channel: mpsc::Sender   // 发往 agent-core:result
    
    FIELD running: AtomicBool
    FIELD metrics: BridgeMetrics
    
    // 输出缓冲区安全上限（源自节点定义的 output_limit_bytes）
    FIELD output_limit: Uint64
    FIELD current_output_bytes: Uint64
    
    STRUCT BridgeMetrics:
        messages_in: Uint64
        messages_out: Uint64
        bytes_in: Uint64
        bytes_out: Uint64
        errors: Uint64
        started_at: Timestamp
    
    // JSON Lines 协议信封
    STRUCT BridgeMessage:
        trace_id: String
        in_response_to: String?
        payload: JsonValue    // 实际载荷
        content_type: String
    
    
    METHOD new(node_id, child, input_rx, output_tx, data_plane, output_limit_bytes):
        self.node_id = node_id
        self.child = child
        self.input_channel = input_rx
        self.output_channel = output_tx
        self.data_plane = data_plane
        self.running = False
        self.metrics = BridgeMetrics.new()
        self.output_limit = output_limit_bytes
        self.current_output_bytes = 0
    
    
    // ── 启动：两个并发循环 ──
    ASYNC METHOD start():
        self.running = True
        
        // 循环 A: Rust → Python（读取 mpsc，写入 stdin）
        tokio::spawn(lambda: self.loop_rust_to_python())
        
        // 循环 B: Python → Rust（读取 stdout，写入 mpsc）
        tokio::spawn(lambda: self.loop_python_to_rust())
        
        // 循环 C: 监控子进程退出
        tokio::spawn(lambda: self.watch_process_exit())
        
        PRINT("[bridge:" + self.node_id + "] 桥接已启动")
    
    
    // ── 循环 A: Rust → Python ──
    ASYNC METHOD loop_rust_to_python():
        stdin_writer = self.child.stdin()
        
        WHILE self.running:
            // 从 mpsc 接收消息
            message = WAIT self.input_channel.recv()
            
            IF message IS None:
                BREAK  // 通道关闭
            
            // 序列化为 JSON Line
            bridge_msg = BridgeMessage {
                trace_id: message.trace_id,
                in_response_to: message.in_response_to,
                payload: message.content,
                content_type: message.content_type
            }
            json_line = json_serialize(bridge_msg) + "\n"
            
            // 写入 Python 子进程 stdin（write_all 保证全部写入）
            result = stdin_writer.write_all(json_line)
            
            IF result.is_error():
                self.handle_error("stdin_write_error", result.error)
                BREAK
            
            stdin_writer.flush()
            
            self.metrics.messages_in++
            self.metrics.bytes_in += len(json_line)
    
    
    // ── 循环 B: Python → Rust ──
    ASYNC METHOD loop_python_to_rust():
        stdout_reader = self.child.stdout()
        buffer = ""
        
        WHILE self.running:
            // 读取一行（JSON Line）
            line = WAIT stdout_reader.read_line()
            
            IF line IS None OR line.is_empty():
                BREAK  // 子进程关闭 stdout
            
            buffer += line
            
            // 安全上限检查：防止恶意子进程耗尽内存
            IF buffer.len() > self.output_limit AND self.output_limit > 0:
                self.handle_error("output_limit_exceeded",
                    "Python 子进程输出超出上限: " + self.output_limit)
                self.child.kill()
                BREAK
            
            // 可能有多行（流式场景）
            WHILE buffer.contains("\n"):
                json_line, buffer = buffer.split_at_first("\n")
                json_line = json_line.trim()
                
                IF json_line.is_empty():
                    CONTINUE
                
                // 反序列化
                bridge_msg = json_deserialize(json_line)
                
                // 构造返回消息
                response = Message {
                    trace_id: bridge_msg.trace_id,
                    message_id: generate_uuid(),
                    in_response_to: bridge_msg.in_response_to,
                    content: bridge_msg.payload,
                    content_type: bridge_msg.content_type,
                    created_at: now()
                }
                
                // 发送到 data_plane → agent-core:result
                self.data_plane.send(
                    self.node_id + ":out",
                    response
                )
                
                self.metrics.messages_out++
                self.metrics.bytes_out += len(json_line)
    
    
    // ── 循环 C: 监控子进程 ──
    ASYNC METHOD watch_process_exit():
        exit_status = WAIT self.child.wait()
        
        self.running = False
        
        // 如果非预期退出，生成审计事件
        IF NOT exit_status.success:
            self.metrics.errors++
            
            stderr_output = self.child.read_stderr()
            
            self.data_plane.audit("ProcessExited", {
                "node_id": self.node_id,
                "exit_code": exit_status.code,
                "stderr": stderr_output,
                "metrics": self.metrics
            })
        
        PRINT("[bridge:" + self.node_id + "] 子进程退出, code=" 
            + exit_status.code)
    
    
    // ── 优雅关闭 ──
    METHOD shutdown():
        self.running = False
        
        // 关闭 stdin（Python 子进程将收到 EOF）
        self.child.stdin().close()
        
        // 等待子进程退出（最多 5 秒）
        result = WAIT timeout(5000ms, self.child.wait())
        IF result.is_timeout():
            self.child.kill()
            WAIT self.child.wait()
        
        PRINT("[bridge:" + self.node_id + "] 桥接已关闭, 统计: " 
            + json_serialize(self.metrics))
```

---

## 8. UI 终端交互循环

### 职责

用户交互边界。抢占终端，循环读取 stdin，显示 markdown 渲染输出到 stdout。

### 伪代码

```
CLASS UI:
    // 内置节点，无 Python 子进程
    FIELD data_plane: DataPlane
    
    // 端口
    PORT out: channel "ui:out" -> "agent-core:in"     // 用户输入
    PORT in:  channel "agent-core:out" -> "ui:in"     // Agent 回复
    
    FIELD history: List<ChatEntry>
    FIELD running: AtomicBool
    
    CONST PROMPT = "sea> "
    
    
    METHOD new(data_plane):
        self.data_plane = data_plane
        self.running = False
        self.history = empty List
    
    
    // ── 主循环 ──
    ASYNC METHOD run():
        self.running = True
        
        // 启用终端 raw mode（允许 tab 补全、历史导航等）
        // (Windows: SetConsoleMode ENABLE_VIRTUAL_TERMINAL_PROCESSING)
        terminal = Terminal::enable_raw_mode()
        
        // 打印欢迎信息
        terminal.print_welcome()
        terminal.print(self.PROMPT)
        
        // 并行等待：用户输入 或 Agent 推送
        LOOP WHILE self.running:
            // 使用 tokio::select 多路等待
            result = SELECT:
                CASE line = self.read_user_input():
                    // 先检查是否是特殊命令
                    IF self.handle_special_command(line):
                        // 特殊命令已处理，跳过发送给 agent-core
                        PASS
                    ELSE:
                        // 普通用户消息
                        user_msg = Message {
                            trace_id: generate_trace_id(),
                            message_id: generate_uuid(),
                            content: {"content": line, "content_type": "text/plain"},
                            content_type: "application/json",
                            created_at: now()
                        }
                        
                        self.history.push({
                            "role": "user",
                            "content": line,
                            "trace_id": user_msg.trace_id
                        })
                        
                        // 发送给 agent-core
                        self.data_plane.send("ui:out", user_msg)
                    
                CASE agent_msg = self.in.recv():
                    // 收到 Agent 回复
                    self.handle_agent_response(agent_msg)
                    
                CASE signal = self.signal_channel.recv():
                    // 收到流控信号
                    self.handle_signal(signal)
            
            // 打印下一个 prompt
            terminal.print(self.PROMPT)
        
        // 恢复终端设置
        terminal.restore()
    
    
    // ── 读取用户输入 ──
    METHOD read_user_input() -> String:
        // 支持:
        // - 行编辑（退格、左右键）
        // - Tab 补全（节点名、命令）
        // - 上下键历史导航
        // - Ctrl+C 中断（触发退出流程）
        // - Ctrl+D EOF（触发退出流程）
        
        LOOP:
            line = terminal.readline()
            
            IF line IS None:  // Ctrl+D
                self.running = False
                RETURN "/exit"
            
            line = line.trim()
            
            IF line == "":
                CONTINUE  // 继续读取，防止栈溢出
            ELSE:
                RETURN line
    
    
    // ── 处理 Agent 回复 ──
    METHOD handle_agent_response(msg: Message):
        // 支持流式渲染（stream: true）
        IF msg.stream:
            // 真正的流式渲染：增量接收 LLM SSE 分片后实时输出
            // msg.content 已由 agent-core 在流式传输中逐片推送
            // 这里直接渲染，不再模拟
            rendered = render_markdown(msg.content)
            terminal.print_inline(rendered)
        ELSE:
            // 一次性输出
            rendered = render_markdown(msg.content)
            terminal.print(rendered)
        
        self.history.push({
            "role": "assistant",
            "content": msg.content,
            "trace_id": msg.trace_id
        })
    
    
    // ── 特殊命令处理 ──
    METHOD handle_special_command(cmd: String) -> Bool:
        SWITCH cmd:
            CASE "/exit", "/quit":
                self.running = False
                RETURN True
            
            CASE "/help":
                terminal.print("可用命令:")
                terminal.print("  /exit     退出")
                terminal.print("  /history  显示历史")
                terminal.print("  /nodes    查看可用节点")
                terminal.print("  /clear    清屏")
                terminal.print("  Ctrl+C    中断")
                RETURN True
            
            CASE "/history":
                FOR EACH entry IN self.history:
                    terminal.print("[" + entry.role + "] " + entry.content)
                RETURN True
            
            CASE "/nodes":
                // 通过 Registry 查询
                // (发送查询消息并等待响应)
                nodes = QUERY self.data_plane.query_registry()
                FOR EACH node IN nodes:
                    terminal.print("- " + node.id + ": " + node.description)
                RETURN True
            
            CASE "/clear":
                terminal.clear_screen()
                RETURN True
        
        RETURN False  // 不是特殊命令
```

---

## 9. Reload Group 增量热更新

### 职责

当 Admin 修改 `.group.toml` 后，控制面执行增量变更：对比新旧配置，自动增/删/改节点和通道，不影响未变更的服务。

### 伪代码

```
FUNCTION reload_group(group_toml_path: String, 
                       node_manager: NodeManager,
                       data_plane: DataPlane,
                       registry: Registry,
                       audit_logger: AuditLogger):
    
    // 1. 解析新配置
    new_config = parse_group_toml(load_file(group_toml_path))
    
    // 2. 获取当前状态（从 Registry 中读取快照）
    current_nodes = registry.list_all()
    current_node_set = Set(current_nodes.map(n => n.id))
    
    // 3. 新配置的节点集
    new_node_set = Set(new_config.nodes.keys())
    
    // 4. 计算 diff
    to_add    = new_node_set - current_node_set     // 新增
    to_remove = current_node_set - new_node_set     // 移除
    to_update = intersection(new_node_set, current_node_set)  // 可能的更新
    
    // === 删除（先删，避免依赖冲突） ===
    FOR EACH node_id IN to_remove:
        IF node_id IN PROTECTED_NODES:
            PRINT("[reload] 跳过受保护节点: " + node_id)
            CONTINUE
        
        PRINT("[reload] 移除节点: " + node_id)
        
        node_manager.terminate(node_id, TERMINATE)
        node_manager.reap(node_id)
        registry.unregister(node_id)
        
        audit_logger.log("GroupReloadRemove", {
            "node_id": node_id
        })
    
    // === 更新（需要检测变化） ===
    FOR EACH node_id IN to_update:
        old_def = get_current_node_def(node_id)
        new_def = new_config.nodes[node_id]
        
        IF old_def == new_def:
            CONTINUE  // 未变化
        
        PRINT("[reload] 更新节点: " + node_id)
        
        runtime_changed = (old_def.runtime != new_def.runtime)
        ports_changed = (old_def.inputs != new_def.inputs 
                      OR old_def.outputs != new_def.outputs)
        config_changed = (old_def.config != new_def.config)
        
        IF runtime_changed:
            // 运行时变更需要重启
            node_manager.terminate(node_id, TERMINATE)
            // 记录旧值
            audit_logger.log("ReloadRestart", {
                "node_id": node_id,
                "old_runtime": old_def.runtime.kind,
                "new_runtime": new_def.runtime.kind
            })
            node_manager.spawn(new_def)
            
        ELSE IF ports_changed:
            // 端口变更可能需要重建通道
            // 实际开发中需要更细粒度的差异比较
            audit_logger.log("ReloadPortsChanged", {
                "node_id": node_id
            })
            // 通知下游重建通道
            data_plane.rebuild_channels_for(node_id, new_def)
            
        ELSE IF config_changed:
            // 发送 RELOAD 信号
            node_manager.send_signal(node_id, RELOAD, new_def.config)
            audit_logger.log("ReloadConfig", {
                "node_id": node_id
            })
    
    // === 新增 ===
    FOR EACH node_id IN to_add:
        node_def = new_config.nodes[node_id]
        
        PRINT("[reload] 新增节点: " + node_id)
        
        result = node_manager.spawn(node_def)
        IF result.is_ok():
            registry.register(node_def)
            
            audit_logger.log("GroupReloadAdd", {
                "node_id": node_id,
                "kind": node_def.runtime.kind
            })
        ELSE:
            PRINT("[reload] 新增节点失败: " + node_id 
                  + " - " + result.error)
    
    // === 通道增量更新 ===
    old_channels = get_current_channels()
    new_channels = new_config.channels
    
    FOR EACH (source, new_targets) IN new_channels:
        old_targets = old_channels.get(source, [])
        
        targets_to_add = new_targets - Set(old_targets)
        targets_to_remove = Set(old_targets) - new_targets
        
        FOR EACH target IN targets_to_remove:
            data_plane.channel_remove(source, target)
        
        FOR EACH target IN targets_to_add:
            data_plane.channel_create(source, target)
    
    PRINT("[reload] 热更新完成: added=" + len(to_add)
          + " removed=" + len(to_remove)
          + " updated=" + len(to_update.filter(is_changed)))
    
    
    RETURN ReloadResult {
        added: to_add,
        removed: to_remove,
        updated: to_update.filter(is_changed)
    }
```

---

## 10. Access Check 策略判定

### 职责

在消息路由前执行权限检查。同一进程内节点信任逻辑隔离，跨进程节点强制执行白名单通信。

### 伪代码

```
CLASS AccessChecker:
    // 静态白名单：由 group config 声明
    FIELD whitelist: Set<String>       // 格式: "source_node:port->target_node:port"
    
    // 动态创建通道的运行时白名单
    FIELD runtime_whitelist: RwLock<Set<String>>
    
    // 节点身份表
    FIELD identities: HashMap<NodeId, Identity>
    
    // 特权掩码表（用于 privilege_transition）
    FIELD privilege_masks: RwLock<HashMap<NodeId, Set<String>>>
    
    CONST IN_PROCESS_TRUSTED = True   // 进程内节点相互信任
    CONST SEPARATE_PROCESS_CHECK = True  // 跨进程强制执行
    
    
    METHOD new():
        self.whitelist = empty Set
        self.runtime_whitelist = RwLock::new(empty Set)
        self.identities = empty HashMap
        self.privilege_masks = RwLock::new(empty HashMap)
    
    
    // ── 加载静态白名单 ──
    METHOD load_from_group_config(config: GroupConfig):
        FOR EACH (source, targets) IN config.channels:
            FOR EACH target IN targets:
                self.whitelist.insert(source + "->" + target)
    
    // ── 运行时注册通道 ──
    METHOD register_channel(source: String, target: String):
        self.runtime_whitelist.write().insert(source + "->" + target)
    
    
    // ── 权限判定 ──
    METHOD check(source_node: String, target_node: String, 
                 port: String, trace_id: String) -> Permit | Deny:
        
        source_identity = self.identities.get(source_node)
        target_identity = self.identities.get(target_node)
        
        // 0) 进程内互信：如果两个节点都是 in_process，且 IN_PROCESS_TRUSTED 启
        IF IN_PROCESS_TRUSTED:
            source_type = self.get_node_type(source_node)
            target_type = self.get_node_type(target_node)
            // 跨进程节点仍然走完整检查
            IF source_type != "separate_process" AND target_type != "separate_process":
                RETURN Permit  // 进程内节点互信
        
        // 1) 检查特权掩码：发送方是否有 channel_send 权限
        masks = self.privilege_masks.read()
        IF source_node IN masks:
            source_privileges = masks[source_node]
            IF "channel_send" NOT IN source_privileges:
                self.audit_violation(source_node, target_node, 
                    "缺少 channel_send 权限 (已降权)")
                RETURN Deny("channel_send privilege dropped")
        
        // 2) 检查源节点是否在目标节点依赖的白名单中
        channel_key = source_node + ":" + port + "->" + target_node + ":in"
        
        is_in_whitelist = (channel_key IN self.whitelist)
                       OR (channel_key IN self.runtime_whitelist.read())
        
        IF NOT is_in_whitelist:
            self.audit_violation(source_node, target_node, 
                "通道不在白名单中")
            RETURN Deny("Access denied: channel not in whitelist")
        
        // 3) 如果源是 LLM 节点且已发 tool_call，检查是否已降权
        source_type = self.get_node_type(source_node)
        IF source_type == "llm":
            IF "tool_call" NOT IN masks.get(source_node, empty Set):
                // LLM 已经完成了 tool_call 降权
                // 但后续的普通 channel_send 仍允许
                PASS  // 这是正常流程
        
        RETURN Permit
    
    
    // ── 审计违规 ──
    METHOD audit_violation(source, target, reason):
        audit_logger.log("AccessViolation", {
            "source": source,
            "target": target,
            "reason": reason,
            "timestamp": now()
        })
    
    
    // ── privilege_transition: 不可逆降权 ──
    METHOD drop_privilege(node_id: String, privilege: String):
        self.privilege_masks.write()
            .entry(node_id)
            .or_insert(all_privileges)
            .remove(privilege)
        
        audit_logger.log("PrivilegeDrop", {
            "node_id": node_id,
            "dropped_privilege": privilege,
            "remaining": self.privilege_masks.read()[node_id]
        })
```

---

## 11. 信号处理与流控

### 职责

实现 PAUSE/RESUME/RELOAD 流控信号及背压响应。每个节点可响应信号，调整自己的消息消费行为。

### 伪代码

```
// ── 信号类型（应用层，非 OS 信号） ──
ENUM Signal:
    PAUSE     // 暂停接收新消息
    RESUME    // 恢复接收新消息
    RELOAD    // 配置热更新
    TERMINATE // 优雅终止
    KILL      // 强制终止
    CUSTOM    // 自定义（具体语义由节点定义）
    
// ── sentinel 值：标记当前消息为"完成当前任务后退出" ──
CONST COMPLETE_AND_EXIT = "<complete_and_exit>"
    
    
// ── 信号通道 ──
CLASS SignalChannel:
    FIELD signals: HashMap<NodeId, mpsc::Sender<Signal>>
    
    METHOD register(node_id: String, rx: mpsc::Receiver<Signal>):
        PASS
    
    METHOD send(node_id: String, signal: Signal):
        PASS
    
    METHOD broadcast(signal: Signal):
        // 向所有节点广播信号
        FOR EACH (node_id, sender) IN self.signals:
            sender.send(signal)
    

// ── 信号响应处理（在每个节点的消息循环中嵌入） ──
ASYNC METHOD node_message_loop(self, node_id: String,
                                 input_rx: mpsc::Receiver<Message>,
                                 signal_rx: mpsc::Receiver<Signal>,
                                 buffer_usage: Arc<AtomicUint>):
    
    state = RUNNING
    
    LOOP:
        SELECT:
            CASE signal = signal_rx.recv():
                SWITCH signal:
                    CASE PAUSE:
                        state = PAUSED
                        PRINT("[node:" + node_id + "] pause")
                    
                    CASE RESUME:
                        state = RUNNING
                        PRINT("[node:" + node_id + "] resume")
                    
                    CASE RELOAD:
                        // 热更新配置
                        new_config = signal.data
                        self.apply_config(new_config)
                        PRINT("[node:" + node_id + "] reload")
                    
                    CASE TERMINATE:
                        self.current_msg = COMPLETE_AND_EXIT
                        PRINT("[node:" + node_id + "] terminating...")
                        EXIT LOOP
                    
                    CASE KILL:
                        PRINT("[node:" + node_id + "] killed")
                        EXIT LOOP
            
            CASE message = input_rx.recv() IF state == RUNNING:
                IF message IS None:
                    // 通道已关闭（目标节点已停止或通道被拆除）
                    PRINT("[node:" + node_id + "] 输入通道已关闭")
                    EXIT LOOP
                // 处理消息后递减 buffer_usage
                self.process_message(message)
                buffer_usage.fetch_sub(1)
            
            CASE sleep(100ms) IF state == PAUSED:
                // PAUSED 状态时仍然处理信号，但不接收新消息
                // 已有消息继续处理
                IF self.current_msg IS NOT None:
                    self.process_message(self.current_msg)
                    self.current_msg = None
```

---

## 12. 自演进闭环 (Self-Evolution)

### 职责

描述 agent-core（Supervisor LLM）如何发现能力缺口、生成提案、验证效果，形成一个完整的自演进闭环。

### 伪代码

```
// ── 自演进贯穿 SEA CLI 的完整流程 ──
//
// 触发点：agent-core 在每次对话开始时注入当前节点目录
// 决策点：agent-core 的 system prompt 中内置自演进策略
// 闭环：  发现缺口 → 生成提案 → 校验生效 → 感知新能力


// ── agent-core 的 System Prompt 模板（核心） ──
FUNCTION build_system_prompt(registry_summary: String) -> String:
    RETURN """
你是 SEA CLI 的核心助手（Supervisor）。你运行在一个节点化架构中。

## 可用节点
""" + registry_summary + """

## 你的能力
1. 直接回复用户消息
2. 调用节点完成子任务（通过发出 JSON 调用请求）
3. 当现有节点无法满足需求时，**提出自演进提案**

## 调用节点
发送 JSON 到 call 端口：
{
  "target": "节点 id",
  "payload": { ... }
}
或者
{
  "capability": "能力描述",
  "payload": { ... }
}

## 自演进策略
当出现以下情况时，你应该提出自演进提案：
- 用户请求的能力当前没有任何节点能提供
- 现有节点执行任务效率低、频繁出错
- 用户明确要求"添加一个能做 XXX 的工具"
- 你发现可以通过组合新节点大幅提升系统能力

## 自演进提案
发送 JSON 到 register 端口：
{
  "operation": "add" | "update" | "remove",
  "proposal_id": "prop-xxx",
  "node": { ... },
  "reason": "为什么需要这个变更"
}

## 安全约束
- 不要尝试修改或删除核心节点（ui, agent-core, router, admin, registry）
- 新增 Python 节点代码必须校验完整性
- 确保新节点有明确的输入/输出端口声明
"""
    
    
// ── agent-core 主推理循环（含自演进决策） ──
CONST MAX_HISTORY = 50              // 对话历史最大轮数
ASYNC METHOD agent_core_run(llm_client: LLMClient,
                              registry: Registry,
                              data_plane: DataPlane,
                              history: List<Message>):
    
    // 首次启动时查询 Registry
    summary = registry.generate_summary()
    system_prompt = build_system_prompt(summary)
    
    LOOP:
        // 等待用户输入
        user_msg = WAIT data_plane.recv("agent-core:in")
        
        // 将用户输入追加到对话历史
        history.push(user_msg)
        
        // 构造 LLM 请求
        llm_request = {
            "system": system_prompt,
            "messages": history,
            "tools": []     // 不使用 function calling，让 LLM 在文本中决策
        }
        
        // 调用 LLM（流式）
        llm_response = llm_client.chat_completion(llm_request)
        
        // 解析 LLM 回复中的动作
        actions = parse_actions(llm_response.content)
        
        FOR EACH action IN actions:
            SWITCH action.type:
                CASE "reply":
                    // 直接回复用户
                    data_plane.send("agent-core:out", {
                        "content": action.content,
                        "content_type": "text/markdown"
                    })
                    
                    history.push({
                        "role": "assistant",
                        "content": action.content
                    })
                
                CASE "call":
                    // 调用子节点
                    call_msg = {
                        "target": action.target,
                        "capability": action.capability?,  // 可选
                        "payload": action.payload
                    }
                    
                    // 发送调用请求
                    trace_id = generate_trace_id()
                    data_plane.send("agent-core:call", {
                        "trace_id": trace_id,
                        "content": call_msg,
                        "content_type": "application/json"
                    })
                    
                    // 等待结果（超时机制）
                    result = WAIT timeout(30000ms, 
                        data_plane.recv("agent-core:result"))
                    
                    IF result.is_timeout():
                        history.push({
                            "role": "system",
                            "content": "【调用超时】节点 " + action.target + " 无响应"
                        })
                    ELSE:
                        // 将结果追加到对话（作为 system 消息）
                        history.push({
                            "role": "system",
                            "content": "【调用结果】\n" 
                                + json_serialize(result.content)
                        })
                
                CASE "register":
                    // 自演进提案
                    proposal = action.proposal
                    
                    // 发给 admin
                    data_plane.send("agent-core:register", {
                        "trace_id": generate_trace_id(),
                        "content": proposal,
                        "content_type": "application/json"
                    })
                    
                    // 等待 admin 回复
                    result = WAIT data_plane.recv("agent-core:result")
                    
                    IF result.content.status == "accepted":
                        // 提案生效，重新查询 Registry 更新系统提示
                        new_summary = registry.generate_summary()
                        system_prompt = build_system_prompt(new_summary)
                        
                        history.push({
                            "role": "system",
                            "content": "【自演进成功】新节点 " 
                                + proposal.node.id + " 已就绪"
                        })
                    ELSE:
                        history.push({
                            "role": "system",
                            "content": "【自演进失败】" + result.content.error
                        })
        
        // 如果 LLM 请求了子节点调用，再次调用 LLM 生成最终回复
        IF has_pending_results(actions):
            final_request = {
                "system": system_prompt,
                "messages": history,
            }
            final_response = llm_client.chat_completion(final_request)
            
            data_plane.send("agent-core:out", {
                "content": final_response.content,
                "content_type": "text/markdown"
            })
            
            history.push({
                "role": "assistant",
                "content": final_response.content
            })
        
        // 历史窗口管理（只保留最近 N 轮）
        IF len(history) > MAX_HISTORY:
            history = trim_history(history, MAX_HISTORY)


// ── parse_actions: 从 LLM 的回复中解析出动作 ──
FUNCTION parse_actions(llm_content: String) -> List<Action>:
    actions = empty List
    
    // 主解析策略：XML <action> 标签
    action_blocks = extract_xml_tags(llm_content, "action")
    
    // 备选策略：从 markdown 代码块中提取 JSON（LLM 输出格式不稳定时的 fallback）
    IF action_blocks.is_empty():
        json_blocks = extract_markdown_json(llm_content)
        FOR EACH block IN json_blocks:
            action = try_parse_action_from_json(block)
            IF action IS NOT None:
                actions.push(action)
    
    // 第三策略：直接全文正则搜索 JSON 动作模式（兜底）
    IF actions.is_empty():
        regex_found = extract_json_by_regex(llm_content,
            r'\{"operation":\s*"(?:add|update|remove)".*?\}')
        FOR EACH match IN regex_found:
            action = try_parse_action_from_json(match)
            IF action IS NOT None:
                actions.push(action)
    
    // 仍为空 → 记录解析失败用于诊断（不阻塞普通回复）
    IF actions.is_empty():
        audit_logger.log("ActionParseFailure", {
            "content_preview": llm_content.truncate(200)
        })
    
    FOR EACH block IN action_blocks:
        block_type = block.attributes.type
        block_json = json_parse(block.inner_text)
        
        actions.push(Action {
            type: block_type,
            target: block_json.target?,
            capability: block_json.capability?,
            payload: block_json.payload?,
            proposal: block_json,  // register 提案完整内容
            content: block_json.content?  // reply 内容
        })
    
    RETURN actions


// ── 自演进全流程时序 ──
//
// 用户: "帮我查一下 MySQL 的用户表"
// 
// agent-core:
//   1. 查 Registry → 没有 mysql 相关节点
//   2. 决定自演进：创建 mysql_query 节点
//   3. 生成提案 JSON 发于 admin
// 
// admin:
//   1. 校验通过
//   2. 写入 .group.toml
//   3. spawn Python 子进程 + bridge
//   4. 注册到 Registry
//   5. 回复 accepted
// 
// agent-core:
//   6. 重新查询 Registry → 现在有 mysql_query 节点
//   7. 更新 system prompt
//   8. 调用 mysql_query 节点
//   9. 得到结果，回复用户
// 
// 结果: 系统无需重启，能力自动扩展
```

---

## 13. 项目结构与模块规划

### 13.1 目录树总览

```
sea/
├── Cargo.toml              # workspace root, [workspace] + [profile.*]
├── Cargo.lock
├── rust-toolchain.toml     # channel = "stable", components = ["clippy", "rustfmt"]
├── .cargo/
│   └── config.toml         # 构建配置（linker, target-specific）
├── .env.example            # 环境变量模板 (SEA_MODEL, API 密钥等)
├── .gitignore
├── README.md
├── LICENSE
│
├── .group.toml             # 系统内置节点定义 + 通道拓扑（编译时 embed!）
│
├── crates/                 # ── workspace 子 crate ──
│   │
│   ├── sea-bin/            # 二进制入口 crate
│   │   ├── Cargo.toml      #   name = "sea", [[bin]] name = "sea"
│   │   └── src/
│   │       └── main.rs     #   入口：解析参数，启动 IFP Runtime
│   │
│   ├── sea-runtime/        # IFP Runtime 核心库（控制面 + 数据面）
│   │   ├── Cargo.toml      #   name = "sea-runtime"
│   │   └── src/
│   │       ├── lib.rs          # crate 根，聚合各模块
│   │       ├── runtime.rs      # IFPRuntime 单例 — main() 函数伪代码 §1
│   │       ├── control_plane.rs # 控制面：组管理 / 生命周期 / 审计
│   │       ├── data_plane.rs   # 数据面：ChannelSwitch 包装入口
│   │       │
│   │       ├── node/               # 节点子系统
│   │       │   ├── mod.rs
│   │       │   ├── manager.rs      # NodeManager — 伪代码 §2
│   │       │   ├── instance.rs     # NodeInstance, Handle 枚举
│   │       │   ├── identity.rs     # Identity 绑定（principal）
│   │       │   └── signal.rs       # 应用层信号定义 + SignalChannel — 伪代码 §11
│   │       │
│   │       ├── registry/           # Registry — 伪代码 §3
│   │       │   ├── mod.rs
│   │       │   └── catalog.rs      # RwLock<HashMap<NodeId, NodeInfo>>
│   │       │
│   │       ├── router/             # Router — 伪代码 §4
│   │       │   ├── mod.rs
│   │       │   └── dispatch.rs     # 无状态匹配 + 路由
│   │       │
│   │       ├── channel/            # ChannelSwitch — 伪代码 §5
│   │       │   ├── mod.rs
│   │       │   ├── switch.rs       # 拓扑表 / 通道建立 / 拆除 / 扇出
│   │       │   ├── backpressure.rs # 背压检测 + 通知
│   │       │   └── dead_letter.rs  # 死信收集 + 审计
│   │       │
│   │       ├── admin/              # Admin — 伪代码 §6
│   │       │   ├── mod.rs
│   │       │   ├── proposal.rs     # 提案接收 + 分发（add/update/remove）
│   │       │   ├── validation.rs   # 校验链（id、上限、格式、依赖）
│   │       │   └── store.rs        # .group.toml 读写 + ReloadGroup §9
│   │       │
│   │       ├── bridge/             # BridgeTask — 伪代码 §7
│   │       │   ├── mod.rs
│   │       │   ├── task.rs         # 桥接逻辑：mpsc ↔ stdin/stdout JSON Lines
│   │       │   ├── protocol.rs     # BridgeMessage 信封定义 + 序列化
│   │       │   └── metrics.rs      # BridgeMetrics 统计
│   │       │
│   │       ├── access/             # AccessChecker — 伪代码 §10
│   │       │   ├── mod.rs
│   │       │   └── checker.rs      # 权限判定 + 白名单 + privilege_transition
│   │       │
│   │       └── audit/              # 审计日志
│   │           ├── mod.rs
│   │           └── logger.rs       # AuditLogger 统一接口
│   │
│   ├── sea-ui/              # UI 终端交互 crate
│   │   ├── Cargo.toml      #   name = "sea-ui", deps = [crossterm, ratatui, syntect]
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── terminal.rs      # Terminal raw mode 管理 — 伪代码 §8
│   │       ├── input.rs         # 行编辑 / 历史 / Tab 补全
│   │       ├── render.rs        # Markdown → 终端 ANSI 渲染
│   │       ├── chat_view.rs     # 对话气泡布局
│   │       ├── history.rs       # 历史持久化（~/.sea/history.jsonl）
│   │       └── commands.rs      # 特殊命令处理（/help, /nodes, /clear...）
│   │
│   ├── sea-llm/             # LLM 客户端 crate
│   │   ├── Cargo.toml      #   name = "sea-llm", deps = [reqwest, tokio, serde_json]
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── client.rs        # LLM API 调用（chat_completion, stream）
│   │       ├── prompts.rs       # system prompt 模板构建 — 伪代码 §12
│   │       ├── parser.rs        # parse_actions: XML → JSON → regex fallback
│   │       └── context.rs       # 对话历史窗口管理
│   │
│   └── sea-common/          # 共享数据类型 crate（零外部依赖）
│       ├── Cargo.toml      #   name = "sea-common"
│       └── src/
│           ├── lib.rs
│           ├── node.rs          # NodeDef, NodeInfo, PortDef, RuntimeDef 等
│           ├── message.rs       # Message, Priority, 封装信封
│           ├── channel.rs       # Channel, ChannelId, 拓扑类型
│           ├── proposal.rs      # Proposal, ProposalResult 等自演进类型
│           ├── identity.rs      # Identity, Principal
│           ├── error.rs         # 统一 Error 类型 + Result 别名
│           └── config.rs        # GroupConfig 解析（serde + toml）
│
├── tools/                  # ── Python 工具节点（内置）──
│   ├── requirements.txt    #   Python 依赖（如有）
│   ├── file_rw.py          #   文件读写节点
│   ├── shell_exec.py       #   Shell 执行节点
│   ├── web_fetch.py        #   网络请求节点
│   └── _bridge.py          #   json_lines 协议辅助库（stdin/stdout 编解码）
│
├── tests/                  # ── 集成测试 ──
│   ├── integration/
│   │   ├── lifecycle.rs        # spawn → terminate → reap 完整链路
│   │   ├── routing.rs          # Router 匹配 + 扇出验证
│   │   ├── channels.rs         # 通道建立/拆除/背压
│   │   ├── admin.rs            # 自演进提案校验（add/update/remove）
│   │   ├── bridge.rs           # Python 子进程桥接端到端
│   │   ├── access.rs           # 权限白名单 + privilege_transition
│   │   └── reload.rs           # ReloadGroup 增量热更新
│   ├── fixtures/
│   │   ├── basic.group.toml    # 最小节点集（2 核心 + 1 工具）
│   │   ├── full.group.toml     # 全部内置节点
│   │   └── malformed.group.toml # 故意破坏的配置（错误注入测试）
│   ├── python/
│   │   ├── test_echo.py        # 最简单的 Python 节点（echo 协议）
│   │   ├── test_slow.py        # 模拟慢节点（超时测试）
│   │   └── test_oom.py         # 模拟大量输出（buffer 上限测试）
│   └── common/
│       └── mod.rs              # test helpers: spawn_runtime(), send_msg(), etc.
│
├── scripts/                # ── 开发辅助脚本 ──
│   ├── embed_toml.sh       # 编译时将 .group.toml embed 到二进制
│   ├── audit_log.sh        # 审计日志查询工具
│   └── gen_test_keys.sh    # 测试用 identity 生成
│
└── docs/
    ├── SEA_CORE.md         # 架构设计文档（已存在）
    ├── IFP_CORE.md         # IFP 协议规范（已存在）
    ├── IFP_ENGINEERING_SPEC.md
    └── sea_instruct.md     # 本文档（伪代码 + 项目结构）
```

---

### 13.2 Cargo 依赖图

```
sea-common ───────────────────────────────────────────────────┐（零外部依赖）
    ↑                        ↑            ↑            ↑      │
sea-runtime            sea-ui（crossterm） sea-llm（reqwest）  │
    ↑                                                    ↑    │
    ├────────────────────────────────────────────────────┘    │
    ↑                                                         │
sea-bin（入口 crate，组装一切）                                │
```

依赖方向是单向的：`sea-bin → sea-runtime → sea-common`，`sea-llm → sea-common`，`sea-ui → sea-common`。没有循环依赖。

---

### 13.3 Cargo.toml 关键配置

**根 `Cargo.toml`：**

```toml
[workspace]
members = [
    "crates/sea-bin",
    "crates/sea-runtime",
    "crates/sea-ui",
    "crates/sea-llm",
    "crates/sea-common",
]
resolver = "2"

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "MIT"

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"
parking_lot = "0.12"       # RwLock 替代 std::sync::RwLock
tracing = "0.1"            # 替代 println 的结构化日志
tracing-subscriber = "0.3"

[workspace.metadata.cargo-udeps.ignore]
normal = []
```

**`crates/sea-bin/Cargo.toml`：**

```toml
[package]
name = "sea"
version.workspace = true
edition.workspace = true

[[bin]]
name = "sea"
path = "src/main.rs"

[dependencies]
sea-runtime = { path = "../sea-runtime" }
sea-ui = { path = "../sea-ui" }
sea-llm = { path = "../sea-llm" }
sea-common = { path = "../sea-common" }
tokio.workspace = true
tracing-subscriber.workspace = true
clap = { version = "4", features = ["derive"] }  # CLI 参数解析
```

**`crates/sea-common/Cargo.toml`：**

```toml
[package]
name = "sea-common"
version.workspace = true
edition.workspace = true

# 零外部依赖，仅 std
[dependencies]
serde.workspace = true
serde_json.workspace = true
toml.workspace = true
```

---

### 13.4 模块职责矩阵

| 源文件 | 伪代码 § | 核心类型 | 职责 |
|--------|----------|----------|------|
| `main.rs` | §1 | — | 命令行参数解析 → 日志初始化 → `IFPRuntime::boot()` |
| `runtime.rs` | §1 | `IFPRuntime` | 解析 .group.toml → 创建控制面+数据面 → 启动节点 → 等待退出 |
| `control_plane.rs` | §1,§9 | `ControlPlane` | 组管理 + identity_bind + 审计事件发布 |
| `data_plane.rs` | §1,§5 | `DataPlane` | 包装 ChannelSwitch 为对外统一接口 |
| `node/manager.rs` | §2 | `NodeManager` | spawn / terminate / reap / shutdown_all / dynamic_spawn |
| `node/instance.rs` | §2 | `NodeInstance`, `Handle` | 节点状态机 + Handle 枚举（InProcess / SeparateProcess / None） |
| `node/identity.rs` | §2 | `Identity`, `Principal` | 节点身份绑定与查询 |
| `node/signal.rs` | §11 | `Signal`, `SignalChannel` | 应用层信号定义 + 通道注册/广播 |
| `registry/catalog.rs` | §3 | `Registry`, `NodeInfo`, `PortDecl` | 节点目录 CRUD + query_by_id / query_by_capability / generate_summary |
| `router/dispatch.rs` | §4 | `Router` | target/capability 匹配 → route 到目标节点 in 端口 |
| `channel/switch.rs` | §5 | `ChannelSwitch`, `Channel`, `ChannelId` | 拓扑表 + create / remove / send（扇出） |
| `channel/backpressure.rs` | §5 | `Backpressure` | 背压检测 → 通知源节点 |
| `channel/dead_letter.rs` | §5 | `DeadLetter` | 死信收集 + try_send 策略 |
| `admin/proposal.rs` | §6 | `Admin` | 提案分发（add / update / remove） |
| `admin/validation.rs` | §6 | `Validator` | 校验链：id 唯一性 / 数量上限 / 格式 / 依赖 |
| `admin/store.rs` | §6,§9 | `GroupTomlStore` | .group.toml 读写 + ReloadGroup 增量变更 |
| `bridge/task.rs` | §7 | `BridgeTask` | Rust mpsc ↔ Python stdin/stdout 双向桥接 |
| `bridge/protocol.rs` | §7 | `BridgeMessage` | JSON Lines 信封编解码 |
| `bridge/metrics.rs` | §7 | `BridgeMetrics` | 消息/字节计数统计 |
| `access/checker.rs` | §10 | `AccessChecker` | 白名单加载 + check + privilege_transition |
| `audit/logger.rs` | — | `AuditLogger` | 结构化审计事件输出（tracing event） |
| `sea-ui/*` | §8 | `UI` | 终端 raw mode / 行编辑 / Markdown 渲染 / 流式输出 |
| `sea-llm/client.rs` | §12 | `LLMClient` | LLM API 调用 + 流式 SSE 消费 |
| `sea-llm/prompts.rs` | §12 | — | system_prompt 模板构建 + Registry 摘要注入 |
| `sea-llm/parser.rs` | §12 | — | parse_actions（XML → markdown JSON → regex fallback） |
| `sea-llm/context.rs` | §12 | — | 对话历史窗口管理（trim_history） |

---

### 13.5 编译时嵌入 .group.toml

使用 Rust 的 `include_str!()` 宏在编译时将默认配置嵌入二进制：

```rust
// crates/sea-runtime/src/runtime.rs

const EMBEDDED_GROUP_TOML: &str = include_str!("../../../.group.toml");

impl IFPRuntime {
    fn boot() -> Result<Self, Error> {
        let group_config: GroupConfig = toml::from_str(EMBEDDED_GROUP_TOML)
            .map_err(|e| Error::ConfigParse(e.to_string()))?;
        // ...
    }
}
```

当前工作区下的 `.group.toml` 被编译进二进制。用户可以通过 `sea --config /path/to/custom.group.toml` 覆盖。

---

### 13.6 关键文件规模预估

| 文件 | 预估行数 | 说明 |
|------|----------|------|
| `node/manager.rs` | ~350 | spawn / terminate / reap 状态机 + 4 种 runtime.kind |
| `channel/switch.rs` | ~300 | 拓扑表管理 + create/remove/send + 扇出逻辑 |
| `channel/backpressure.rs` | ~100 | 背压阈值检测 + 通知构建 |
| `channel/dead_letter.rs` | ~80 | 死信入口 + try_send 策略 |
| `admin/proposal.rs` | ~200 | 提案接收 + 三种操作分发 |
| `admin/validation.rs` | ~200 | 校验链 + 权限检查 |
| `admin/store.rs` | ~300 | .group.toml 增量读写 + ReloadGroup diff |
| `bridge/task.rs` | ~250 | 三个 tokio::spawn 循环 |
| `bridge/protocol.rs` | ~80 | JSON Line 编解码 |
| `access/checker.rs` | ~200 | 白名单 + in_process_trusted + privilege_transition |
| `router/dispatch.rs` | ~150 | target/capability 匹配 + 路由 |
| `registry/catalog.rs` | ~150 | HashMap CRUD + query_by_capability 排序 |
| `sea-llm/parser.rs` | ~180 | 三阶段策略解析 |
| `sea-ui/terminal.rs` | ~200 | raw mode + 输入输出流管理 |
| `sea-common/node.rs` | ~100 | 数据结构（零逻辑） |
| `sea-common/message.rs` | ~80 | 数据结构 |
| **核心总计** | **~3000** | 不含测试 |

测试代码预期为核心代码的 1.5~2 倍规模（集成测试较重）。

---

### 13.7 开发阶段建议

| 阶段 | 目标 | 涉及的 crate | 产出 |
|------|------|-------------|------|
| **P0** 骨架 | 编译通过的空壳，Cargo workspace 就绪 | sea-common, sea-bin | `cargo build` 通过 |
| **P1** 通道层 | ChannelSwitch + 消息类型 | sea-common, sea-runtime/channel | 通道 canary 测试通过 |
| **P2** 节点生命周期 | NodeManager + Registry | sea-runtime/node, sea-runtime/registry | spawn / reap 链路测试 |
| **P3** 路由 | Router + 端到端消息流 | sea-runtime/router | 消息从源节点经过 Router 到达目标 |
| **P4** 桥接 | BridgeTask + Python 子进程 | sea-runtime/bridge | Python echo 节点往返成功 |
| **P5** UI | 终端交互 | sea-ui, sea-llm | 用户输入 → agent-core → 终端输出 |
| **P6** 自演进 | Admin + parse_actions + Reload | sea-runtime/admin, sea-llm/parser | LLM 提案 → 自动创建 Python 节点 |
| **P7** 加固 | 权限、审计、背压、错误处理 | sea-runtime/access, sea-runtime/audit | 生产级质量 |

---

## 附录: 关键数据结构速查

```rust
// ── 节点定义（完整，含 runtime） ──
struct NodeDef {
    id: String,
    description: String,
    inputs: Vec<PortDef>,
    outputs: Vec<PortDef>,
    runtime: RuntimeDef,
    channels: Option<HashMap<String, Vec<String>>>,
}

struct PortDef {
    port: String,       // e.g. "in", "out", "call"
    format: String,     // e.g. "application/json", "text/plain"
}

struct RuntimeDef {
    kind: String,        // "builtin" | "llm" | "python" | "skill"
    function: Option<String>,    // builtin
    model: Option<String>,       // llm
    system_prompt: Option<String>, // llm
    command: Option<String>,     // python
    args: Option<Vec<String>>,   // python
    code: Option<String>,        // python (dynamic_spawn)
    code_hash: Option<String>,   // python (dynamic_spawn)
    isolation: Option<String>,   // "separate_process"
    max_runtime_ms: Option<u64>,
}

// ── 节点外部视角（Registry 存储） ──
struct NodeInfo {
    id: String,
    description: String,
    inputs: Vec<PortDef>,
    outputs: Vec<PortDef>,
}

// ── 消息 ──
struct Message {
    trace_id: String,
    message_id: String,
    content: JsonValue,
    content_type: String,
    priority: u8,            // 0-255, default 128
    ttl_ms: u64,
    created_at: Timestamp,
    in_response_to: Option<String>,
    stream: Option<bool>,
    stream_final: Option<bool>,
}

// ── 通道声明 ──
// channels: HashMap<String, Vec<String>>
// 键:   "source_node:port"
// 值:   ["target1:port", "target2:port"]

// ── 自演进提案 ──
struct Proposal {
    operation: String,       // "add" | "update" | "remove"
    proposal_id: String,
    proposed_by: String,
    node: Option<NodeDef>,       // add
    node_id: Option<String>,     // update / remove
    changes: Option<Changes>,    // update
    reason: String,
}

struct Changes {
    target: String,          // "description" | "prompt" | "code" | "ports" | "channels"
    new_system_prompt: Option<String>,
    new_code: Option<String>,
    new_code_hash: Option<String>,
    new_inputs: Option<Vec<PortDef>>,
    new_outputs: Option<Vec<PortDef>>,
    add_channels: Option<HashMap<String, Vec<String>>>,
    remove_channels: Option<Vec<(String, String)>>,
}

// ── 服务状态 ──
enum ServiceState {
    CREATING,
    RUNNING,
    PAUSED,
    STOPPING,
    STOPPED,
}

// ── 信号 ──
enum Signal {
    PAUSE,
    RESUME,
    RELOAD,
    TERMINATE,
    KILL,
    CUSTOM(String),
}
```

---

> 本文档覆盖 12 个关键子程序的伪代码设计，完整覆盖 SEA CLI 从启动、运行、通信、自演进的整个生命周期。实现时建议按"Runtime → 通道 → 核心节点 → 桥接 → Admin"的顺序推进。
