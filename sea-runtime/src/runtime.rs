use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use sea_common::GroupConfig;

use crate::control_plane::ControlPlane;
use crate::data_plane::DataPlane;
use crate::llm_node::{AgentCore, default_llm_config};
use crate::node_manager::NodeManager;
use crate::registry::Registry;

/// IFPRuntime 引用（Arc<RwLock<>> 包装）。
pub type IFPRuntimeRef = Arc<RwLock<IFPRuntime>>;

/// IFP Runtime —— 系统核心状态单例。
///
/// 遵循 IFP v3.1 规范，控制面/数据面分离，控制面不介入数据面原语的执行。
pub struct IFPRuntime {
    pub config: Option<GroupConfig>,
    pub node_manager: Option<NodeManager>,
    pub control_plane: Option<ControlPlane>,
    pub data_plane: Option<Arc<DataPlane>>,
    pub registry: Option<Arc<Registry>>,
    pub running: bool,
}

impl IFPRuntime {
    pub fn new() -> Self {
        Self {
            config: None,
            node_manager: None,
            control_plane: None,
            data_plane: None,
            registry: None,
            running: false,
        }
    }

    pub fn into_shared(self) -> IFPRuntimeRef {
        Arc::new(RwLock::new(self))
    }

    pub fn data_plane(&self) -> sea_common::SeaResult<Arc<DataPlane>> {
        self.data_plane
            .clone()
            .ok_or_else(|| sea_common::SeaError::Internal("DataPlane 未初始化".to_string()))
    }

    pub async fn shutdown(&mut self) {
        self.running = false;
        tracing::info!("[runtime] IFP Runtime 正在关闭...");
    }
}

impl Default for IFPRuntime {
    fn default() -> Self {
        Self::new()
    }
}

/// 创建并初始化完整的 IFP Runtime。
pub async fn bootstrap_runtime(config: GroupConfig) -> sea_common::SeaResult<IFPRuntimeRef> {
    let data_plane = Arc::new(DataPlane::new(256));

    // 1. 构建 Runtime（先不放入 RwLock）
    let mut runtime = IFPRuntime::new();
    runtime.config = Some(config.clone());
    runtime.data_plane = Some(data_plane.clone());
    runtime.running = true;
    let runtime_ref = runtime.into_shared();

    // 2. 启动数据面
    data_plane.start().await;

    // 3. 启动控制面
    let mut control_plane = ControlPlane::new(runtime_ref.clone());
    control_plane.start().await;
    {
        let mut rt = runtime_ref.write().await;
        rt.control_plane = Some(control_plane);
    }

    // 4. 初始化 Registry（节点启动前就绪）
    let registry = Arc::new(Registry::new());
    {
        let mut rt = runtime_ref.write().await;
        rt.registry = Some(registry.clone());
    }

    // 5. 识别 LLM 节点（agent-core），预创建 AgentCore 并注册消息处理器
    let node_defs = config.to_node_defs();
    let mut _agent_core_handle: Option<tokio::task::JoinHandle<()>> = None;
    let mut llm_config_used = false;

    for node_def in &node_defs {
        if node_def.runtime.kind == sea_common::RuntimeKind::Llm {
            // 从节点定义或环境变量获取 LLM 配置
            let llm_config = match default_llm_config() {
                Some(cfg) => cfg,
                None => {
                    tracing::warn!(
                        "[bootstrap] 跳过 agent-core: LLM 配置不可用（请设置 SEA_API_KEY 环境变量）"
                    );
                    continue;
                }
            };

            // 创建 AgentCore 的输入通道
            let (tx_in, rx_in) = mpsc::channel(256);
            let (tx_result, rx_result) = mpsc::channel(256);

            // 注册到数据面的通道交换机
            // 所有发送到 "agent-core:in" 和 "agent-core:result" 的消息会转发到这些 Sender
            data_plane
                .register_target_handler("agent-core:in", tx_in)
                .await?;
            data_plane
                .register_target_handler("agent-core:result", tx_result)
                .await?;

            // 创建 AgentCore 实例
            let mut agent = AgentCore::new(
                &node_def.id,
                data_plane.clone(),
                registry.clone(),
                llm_config,
            );
            agent.bind_input_rx(rx_in);
            agent.bind_result_rx(rx_result);

            // 启动 AgentCore 主循环
            _agent_core_handle = Some(tokio::spawn(async move {
                agent.run().await;
            }));

            llm_config_used = true;
            tracing::info!(
                "[bootstrap] LLM 节点已就绪: {} (model={})",
                node_def.id,
                std::env::var("SEA_MODEL").unwrap_or_else(|_| "default".to_string())
            );
        }
    }

    // 6. 建立通道拓扑（先于普通节点创建，但晚于 AgentCore 注册处理器）
    //    当通道目标为 "agent-core:in" 或 "agent-core:result" 时，
    //    通道交换机将使用已注册的处理器发送端。
    for (source, targets) in &config.channels {
        for target in targets {
            if let Err(e) = data_plane.channel_create(source, target).await {
                tracing::warn!("[bootstrap] 通道创建失败: {source} -> {target}: {e}");
            }
        }
    }

    // 7. 初始化 NodeManager 并启动非 LLM 节点（LLM 节点已在上方启动）
    let node_manager = {
        let nm = NodeManager::new(data_plane.clone());
        for node_def in &node_defs {
            // LLM 节点已在步骤 5 中由 AgentCore 接管
            if node_def.runtime.kind == sea_common::RuntimeKind::Llm {
                // 在 NodeManager 中注册但不启动新 task
                if let Err(e) = nm.register_llm_node(node_def.clone()).await {
                    tracing::error!("[bootstrap] LLM 节点注册失败: {e}");
                }
                continue;
            }
            if let Err(e) = nm.spawn(node_def.clone()).await {
                tracing::error!("[bootstrap] 节点启动失败: {e}");
            }
        }
        nm
    };
    {
        let mut rt = runtime_ref.write().await;
        rt.node_manager = Some(node_manager);
    }

    // 7b. 绑定所有节点身份到控制面（供访问控制和审计使用）
    {
        let rt = runtime_ref.read().await;
        if let Some(cp) = &rt.control_plane {
            for node_def in &node_defs {
                let identity = sea_common::Identity::new(&node_def.id);
                let _ = cp.identity_bind(&node_def.id, &identity).await;
            }
        }
    }

    // 8. 注册所有节点到 Registry
    for node_def in &node_defs {
        let node_info = sea_common::NodeInfo::with_ports(
            &node_def.id,
            &node_def.description,
            node_def.inputs.clone(),
            node_def.outputs.clone(),
        );
        let _ = registry.register(node_info).await;
    }

    // 9. 发送启动审计
    {
        let rt = runtime_ref.read().await;
        if let Some(cp) = &rt.control_plane {
            let mut data = std::collections::HashMap::new();
            data.insert("node_count".to_string(), config.node.len().to_string());
            data.insert("channel_count".to_string(), config.channels.len().to_string());
            data.insert("llm_enabled".to_string(), llm_config_used.to_string());
            if let Ok(key_set) = std::env::var("SEA_API_KEY") {
                data.insert("api_configured".to_string(), (!key_set.is_empty()).to_string());
            }
            cp.audit_event(sea_common::AuditEventType::RuntimeStart, data)
                .await;
        }
    }

    tracing::info!(
        "[bootstrap] Runtime 启动完成: {} 节点, {} 通道",
        config.node.len(),
        config.channels.len()
    );

    Ok(runtime_ref)
}
