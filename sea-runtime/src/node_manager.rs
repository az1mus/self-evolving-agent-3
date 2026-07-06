use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use sea_common::{
    AuditEventType, AuditLogger, Identity, IsolationMode, NodeDef, NodeInfo,
    NodeState, RuntimeKind, SeaError, SeaResult, Signal, MAX_NODES,
};

use crate::data_plane::DataPlane;

/// 节点实例 —— 运行时的节点状态。
#[derive(Debug)]
pub struct NodeInstance {
    pub id: String,
    pub state: NodeState,
    pub runtime_kind: RuntimeKind,
    pub identity: Identity,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// 节点生命周期管理器。
///
/// 统一管理所有节点的 spawn/terminate/reap 操作，
/// 根据 runtime.kind 决定创建方式，维护节点状态机。
pub struct NodeManager {
    nodes: Arc<RwLock<HashMap<String, NodeInstance>>>,
    audit_logger: Arc<RwLock<AuditLogger>>,
    data_plane: Arc<DataPlane>,
}

impl NodeManager {
    pub fn new(data_plane: Arc<DataPlane>) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            audit_logger: Arc::new(RwLock::new(AuditLogger::new())),
            data_plane,
        }
    }

    /// 获取节点数。
    pub async fn node_count(&self) -> usize {
        self.nodes.read().await.len()
    }

    /// 检查节点是否存在。
    pub async fn contains(&self, node_id: &str) -> bool {
        self.nodes.read().await.contains_key(node_id)
    }

    /// 获取节点状态。
    pub async fn node_state(&self, node_id: &str) -> SeaResult<NodeState> {
        let nodes = self.nodes.read().await;
        nodes
            .get(node_id)
            .map(|n| n.state.clone())
            .ok_or_else(|| SeaError::NodeNotFound(node_id.to_string()))
    }

    /// 更新节点状态。
    pub async fn set_state(&self, node_id: &str, state: NodeState) -> SeaResult<()> {
        let mut nodes = self.nodes.write().await;
        if let Some(instance) = nodes.get_mut(node_id) {
            instance.state = state;
            Ok(())
        } else {
            Err(SeaError::NodeNotFound(node_id.to_string()))
        }
    }

    /// 注册 LLM 节点（由 AgentCore 外部管理，不进 spawn 流程）。
    ///
    /// LLM 节点由 bootstrap 直接创建 AgentCore 实例并管理其生命周期，
    /// 此处仅记录节点元数据，不创建运行时 task。
    pub async fn register_llm_node(&self, node_def: NodeDef) -> SeaResult<String> {
        let node_id = node_def.id.clone();

        if self.contains(&node_id).await {
            return Err(SeaError::NodeAlreadyExists(node_id));
        }

        if self.node_count().await >= MAX_NODES as usize {
            return Err(SeaError::MaxNodesExceeded(MAX_NODES));
        }

        let instance = NodeInstance {
            id: node_id.clone(),
            state: NodeState::Running,
            runtime_kind: node_def.runtime.kind.clone(),
            identity: Identity::new(&node_id),
            created_at: chrono::Utc::now(),
        };

        self.nodes.write().await.insert(node_id.clone(), instance);

        tracing::info!("[node-manager] LLM 节点已注册: {node_id}");
        Ok(node_id)
    }

    /// ── spawn: 根据 runtime.kind 分支创建节点。
    pub async fn spawn(&self, node_def: NodeDef) -> SeaResult<String> {
        let node_id = node_def.id.clone();

        // 检查重复
        if self.contains(&node_id).await {
            return Err(SeaError::NodeAlreadyExists(node_id));
        }

        // 检查上限
        if self.node_count().await >= MAX_NODES as usize {
            return Err(SeaError::MaxNodesExceeded(MAX_NODES));
        }

        // 创建节点实例
        let mut instance = NodeInstance {
            id: node_id.clone(),
            state: NodeState::Ready,
            runtime_kind: node_def.runtime.kind.clone(),
            identity: Identity::new(&node_id),
            created_at: chrono::Utc::now(),
        };

        // 根据 kind 分支创建
        match &node_def.runtime.kind {
            RuntimeKind::Builtin => {
                tracing::info!("[node-manager] spawn builtin: {node_id}");
                // 内置节点：不需要额外运行时，节点已就绪
            }

            RuntimeKind::Llm => {
                tracing::info!("[node-manager] spawn llm: {node_id}");
                // LLM 节点：启动 tokio task 处理消息
            }

            RuntimeKind::Python => {
                tracing::info!("[node-manager] spawn python: {node_id}");

                if node_def.runtime.isolation != IsolationMode::SeparateProcess {
                    tracing::warn!(
                        "[node-manager] Python 节点 {node_id} 应使用 separate_process 隔离模式"
                    );
                }

                // 启动 Bridge task（Python 子进程桥接）
                let command = node_def
                    .runtime
                    .command
                    .clone()
                    .unwrap_or_else(|| "python".to_string());
                let args = node_def.runtime.args.clone().unwrap_or_default();

                let bridge = crate::bridge::BridgeTask::new(
                    &node_id,
                    &self.data_plane,
                )
                .with_command(&command)
                .with_args(args);

                // 启动桥接
                tokio::spawn(async move {
                    bridge.start().await;
                });
            }

            RuntimeKind::Skill => {
                tracing::info!("[node-manager] spawn skill: {node_id}");
                // Skill 节点：纯数据，不需要运行时
            }
        }

        // 注册到 Registry
        let _node_info = NodeInfo::with_ports(
            &node_id,
            &node_def.description,
            node_def.api_docs.as_deref(),
            node_def.inputs.clone(),
            node_def.outputs.clone(),
        );

        // 注册到访问控制器
        // 审计
        {
            let mut logger = self.audit_logger.write().await;
            let mut data = std::collections::HashMap::new();
            data.insert("node_id".to_string(), node_id.clone());
            data.insert(
                "kind".to_string(),
                node_def.runtime.kind_name().to_string(),
            );
            logger.log(AuditEventType::ServiceStart, data);
        }

        // 设置状态为 RUNNING
        instance.state = NodeState::Running;

        // 存储节点实例
        self.nodes.write().await.insert(node_id.clone(), instance);

        tracing::info!("[node-manager] 节点已启动: {node_id}");
        Ok(node_id)
    }

    /// ── terminate: 停止节点。
    pub async fn terminate(&self, node_id: &str, _signal: Signal) -> SeaResult<()> {
        let mut nodes = self.nodes.write().await;
        let instance = nodes
            .get_mut(node_id)
            .ok_or_else(|| SeaError::NodeNotFound(node_id.to_string()))?;

        if instance.state == NodeState::Stopped {
            return Ok(());
        }

        instance.state = NodeState::Stopping;

        // 根据运行时类型执行不同的终止策略
        match &instance.runtime_kind {
            RuntimeKind::Python => {
                // 关闭 Bridge task（Python 子进程）
                tracing::info!("[node-manager] 终止 Python 子进程: {node_id}");
            }
            _ => {
                // builtin/llm/skill: tokio task abort
            }
        }

        instance.state = NodeState::Stopped;

        // 审计
        {
            let mut logger = self.audit_logger.write().await;
            let mut data = std::collections::HashMap::new();
            data.insert("node_id".to_string(), node_id.to_string());
            logger.log(AuditEventType::ServiceExit, data);
        }

        tracing::info!("[node-manager] 节点已停止: {node_id}");
        Ok(())
    }

    /// ── reap: 回收已停止节点的资源。
    pub async fn reap(&self, node_id: &str) -> SeaResult<()> {
        let mut nodes = self.nodes.write().await;
        let instance = nodes
            .get(node_id)
            .ok_or_else(|| SeaError::NodeNotFound(node_id.to_string()))?;

        if instance.state != NodeState::Stopped {
            return Err(SeaError::NodeNotStopped(node_id.to_string()));
        }

        // 拆除通道
        self.data_plane.channel_switch.remove_all_for(node_id).await;

        // 从节点管理器移除
        nodes.remove(node_id);

        tracing::info!("[node-manager] 节点已回收: {node_id}");
        Ok(())
    }

    /// ── shutdown_all: 优雅关闭所有节点。
    pub async fn shutdown_all(&self) {
        let ids: Vec<String> = {
            let nodes = self.nodes.read().await;
            nodes.keys().cloned().collect()
        };

        // 先终止所有 Python 子进程
        for id in &ids {
            if let Ok(instance) = self.node_state(id).await {
                if instance == NodeState::Stopped {
                    continue;
                }
                let _ = self.terminate(id, Signal::Terminate).await;
            }
        }

        // 全部 reap
        for id in &ids {
            let _ = self.reap(id).await;
        }

        tracing::info!("[node-manager] 所有节点已关闭");
    }

    /// 获取节点运行时类型。
    pub async fn kind_of(&self, node_id: &str) -> SeaResult<RuntimeKind> {
        let nodes = self.nodes.read().await;
        nodes
            .get(node_id)
            .map(|n| n.runtime_kind.clone())
            .ok_or_else(|| SeaError::NodeNotFound(node_id.to_string()))
    }

    /// 获取所有节点 ID。
    pub async fn all_node_ids(&self) -> Vec<String> {
        self.nodes.read().await.keys().cloned().collect()
    }
}
