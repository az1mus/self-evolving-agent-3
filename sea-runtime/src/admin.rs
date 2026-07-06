use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use sea_common::{
    NodeInfo, Proposal, ProposalOperation,
    ProposalResult, SeaError,
    MAX_NODES, MAX_LLM_NODES, MAX_PYTHON_NODES, PROTECTED_NODES,
};

use crate::data_plane::DataPlane;
use crate::node_manager::NodeManager;
use crate::registry::Registry;

/// Admin — 自演进管理。
///
/// 接收自演进提案，校验合法性，触发 NodeManager spawn/terminate，
/// 更新 Registry，记录审计。
pub struct Admin {
    node_manager: Arc<NodeManager>,
    registry: Arc<Registry>,
    data_plane: Arc<DataPlane>,
    auto_approve: bool,
    /// kind 计数缓存: node_id -> kind
    kind_cache: RwLock<HashMap<String, String>>,
}

impl Admin {
    pub fn new(
        node_manager: Arc<NodeManager>,
        registry: Arc<Registry>,
        data_plane: Arc<DataPlane>,
        auto_approve: bool,
    ) -> Self {
        Self {
            node_manager,
            registry,
            data_plane,
            auto_approve,
            kind_cache: RwLock::new(HashMap::new()),
        }
    }

    /// 重建 kind 缓存。
    pub async fn rebuild_kind_cache(&self) {
        let mut cache = self.kind_cache.write().await;
        cache.clear();
        for node_id in self.node_manager.all_node_ids().await {
            if let Ok(kind) = self.node_manager.kind_of(&node_id).await {
                cache.insert(node_id, kind.as_str().to_string());
            }
        }
    }

    /// 校验提案者身份。
    fn is_valid_proposer(&self, proposed_by: &str) -> bool {
        proposed_by == "human" || true // 简化：后续接入 Registry 校验
    }

    /// 处理提案入口。
    pub async fn process_proposal(&self, proposal: Proposal) -> ProposalResult {
        let proposal_id = proposal.proposal_id.clone();

        // 校验 proposed_by
        if !self.is_valid_proposer(&proposal.proposed_by) {
            return ProposalResult::error(
                &proposal_id,
                &SeaError::InvalidProposer(proposal.proposed_by.clone()).to_string(),
            );
        }

        match proposal.operation {
            ProposalOperation::Add => self.proposal_add(proposal).await,
            ProposalOperation::Update => self.proposal_update(proposal).await,
            ProposalOperation::Remove => self.proposal_remove(proposal).await,
        }
    }

    /// 新增提案。
    async fn proposal_add(&self, proposal: Proposal) -> ProposalResult {
        let proposal_id = proposal.proposal_id.clone();
        let node = match &proposal.node {
            Some(n) => n.clone(),
            None => {
                return ProposalResult::error(&proposal_id, "提案缺少 node 定义");
            }
        };

        // 1) id 唯一性
        if self.registry.query_by_id(&node.id).await.is_ok() {
            return ProposalResult::error(
                &proposal_id,
                &SeaError::NodeAlreadyExists(node.id.clone()).to_string(),
            );
        }

        // 2) 节点总数上限
        let total = self.registry.list_all().await.len();
        if total >= MAX_NODES as usize {
            return ProposalResult::error(
                &proposal_id,
                &SeaError::MaxNodesExceeded(MAX_NODES).to_string(),
            );
        }

        // 3) 按 kind 检查独立上限
        match &node.runtime.kind {
            sea_common::RuntimeKind::Llm => {
                let count = self.count_by_kind("llm").await;
                if count >= MAX_LLM_NODES {
                    return ProposalResult::error(
                        &proposal_id,
                        &SeaError::MaxLlmNodesExceeded(MAX_LLM_NODES).to_string(),
                    );
                }
            }
            sea_common::RuntimeKind::Python => {
                let count = self.count_by_kind("python").await;
                if count >= MAX_PYTHON_NODES {
                    return ProposalResult::error(
                        &proposal_id,
                        &SeaError::MaxPythonNodesExceeded(MAX_PYTHON_NODES).to_string(),
                    );
                }
            }
            sea_common::RuntimeKind::Builtin => {
                return ProposalResult::error(
                    &proposal_id,
                    "不允许动态创建 builtin 核心节点",
                );
            }
            sea_common::RuntimeKind::Skill => {}
        }

        // 4) 格式合法性
        if node.inputs.is_empty() && node.runtime.kind != sea_common::RuntimeKind::Skill {
            return ProposalResult::error(&proposal_id, "非 skill 节点至少需要一个输入端口");
        }
        if node.outputs.is_empty() {
            return ProposalResult::error(&proposal_id, "节点至少需要一个输出端口");
        }

        // 5) 执行 spawn
        match self.node_manager.spawn(node.clone()).await {
            Ok(node_id) => {
                // 建立通道
                if let Some(channels) = &node.channels {
                    for (source, targets) in channels {
                        for target in targets {
                            let _ = self.data_plane.channel_create(source, target).await;
                        }
                    }
                }

                // 注册到 Registry
                let node_info = NodeInfo::with_ports(
                    &node_id,
                    &node.description,
                    node.api_docs.as_deref(),
                    node.inputs.clone(),
                    node.outputs.clone(),
                );
                let _ = self.registry.register(node_info).await;

                // 更新 kind 缓存
                self.kind_cache
                    .write()
                    .await
                    .insert(node_id.clone(), node.runtime.kind_name().to_string());

                ProposalResult::accepted(&proposal_id, &node_id)
            }
            Err(e) => ProposalResult::error(&proposal_id, &e.to_string()),
        }
    }

    /// 更新提案（简化版）。
    async fn proposal_update(&self, proposal: Proposal) -> ProposalResult {
        let proposal_id = proposal.proposal_id.clone();
        let node_id = match &proposal.node_id {
            Some(id) => id.clone(),
            None => return ProposalResult::error(&proposal_id, "更新提案缺少 node_id"),
        };

        // 检查节点存在
        if self.registry.query_by_id(&node_id).await.is_err() {
            return ProposalResult::error(
                &proposal_id,
                &SeaError::NodeNotFound(node_id).to_string(),
            );
        }

        let changes = match &proposal.changes {
            Some(c) => c.clone(),
            None => return ProposalResult::error(&proposal_id, "更新提案缺少 changes"),
        };

        match changes.target.as_str() {
            "description" => {
                if let Some(desc) = &changes.new_description {
                    let _ = self.registry.update_description(&node_id, desc).await;
                }
            }
            "prompt" => {
                // 仅 LLM 节点
                let kind = self.node_manager.kind_of(&node_id).await;
                if kind.map(|k| k != sea_common::RuntimeKind::Llm).unwrap_or(true) {
                    return ProposalResult::error(&proposal_id, "prompt 更新仅适用于 LLM 节点");
                }
                // TODO: 发送 RELOAD 信号
            }
            "code" => {
                // 仅 Python 节点
                let kind = self.node_manager.kind_of(&node_id).await;
                if kind.map(|k| k != sea_common::RuntimeKind::Python).unwrap_or(true) {
                    return ProposalResult::error(&proposal_id, "code 更新仅适用于 Python 节点");
                }
                // 终止旧进程 → 重新 spawn
                let _ = self.node_manager.terminate(&node_id, sea_common::Signal::Terminate).await;
                let _ = self.node_manager.reap(&node_id).await;
                // TODO: 更新代码后重新 spawn
            }
            "ports" => {
                if let Some(inputs) = &changes.new_inputs {
                    if let Some(outputs) = &changes.new_outputs {
                        let _ = self
                            .registry
                            .update_ports(&node_id, inputs.clone(), outputs.clone())
                            .await;
                    }
                }
            }
            "channels" => {
                // 拆除旧通道 / 建立新通道
                if let Some(remove_list) = &changes.remove_channels {
                    for (src, tgt) in remove_list {
                        let _ = self.data_plane.channel_switch.remove(src, tgt).await;
                    }
                }
                if let Some(add_map) = &changes.add_channels {
                    for (src, targets) in add_map {
                        for tgt in targets {
                            let _ = self.data_plane.channel_create(src, tgt).await;
                        }
                    }
                }
            }
            other => {
                return ProposalResult::error(&proposal_id, &format!("未知更新目标: {other}"));
            }
        }

        ProposalResult::accepted(&proposal_id, &node_id)
    }

    /// 删除提案。
    async fn proposal_remove(&self, proposal: Proposal) -> ProposalResult {
        let proposal_id = proposal.proposal_id.clone();
        let node_id = match &proposal.node_id {
            Some(id) => id.clone(),
            None => return ProposalResult::error(&proposal_id, "删除提案缺少 node_id"),
        };

        // 1) 检查节点存在
        if self.registry.query_by_id(&node_id).await.is_err() {
            return ProposalResult::error(
                &proposal_id,
                &SeaError::NodeNotFound(node_id).to_string(),
            );
        }

        // 2) 检查是否受保护的核心节点
        if PROTECTED_NODES.contains(&node_id.as_str()) {
            return ProposalResult::error(
                &proposal_id,
                &SeaError::ProtectedNode(node_id).to_string(),
            );
        }

        // 3) 执行删除
        let _ = self
            .node_manager
            .terminate(&node_id, sea_common::Signal::Terminate)
            .await;
        let _ = self.node_manager.reap(&node_id).await;

        // 4) Registry 注销
        let _ = self.registry.unregister(&node_id).await;

        // 5) 清理通道
        self.data_plane.channel_switch.remove_all_for(&node_id).await;

        // 6) 更新 kind 缓存
        self.kind_cache.write().await.remove(&node_id);

        ProposalResult::accepted(&proposal_id, &node_id)
    }

    /// 按 kind 计数（使用缓存）。
    async fn count_by_kind(&self, kind: &str) -> u32 {
        let cache = self.kind_cache.read().await;
        cache.values().filter(|v| *v == kind).count() as u32
    }
}
