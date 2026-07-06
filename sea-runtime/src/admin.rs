use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use sea_common::{
    Message, NodeInfo, Proposal, ProposalOperation,
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
    /// 输入通道接收端（admin:in）。
    rx: Option<mpsc::Receiver<Message>>,
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
            rx: None,
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

    /// 绑定输入通道接收端。
    pub fn bind_rx(&mut self, rx: mpsc::Receiver<Message>) {
        self.rx = Some(rx);
    }

    /// 启动 Admin 主循环。
    ///
    /// 接收 `admin:in` 消息，解析为 Proposal，处理后将结果返回 `agent-core:result`。
    /// 解析失败时也有 fallback 机制，保证不会让调用方（agent-core）空等。
    pub async fn run(&mut self) {
        let mut rx = self.rx.take().expect("Admin: 未绑定输入通道");

        tracing::info!("[admin] 自演进管理已启动");

        while let Some(msg) = rx.recv().await {
            let trace_id = msg.trace_id.clone();

            // 先尝试严格解析，失败则走 fallback 路径
            let proposal_result = match msg.content_as_json() {
                Ok(raw_value) => {
                    // 路径 A: 严格反序列化为 Proposal
                    match serde_json::from_value::<Proposal>(raw_value.clone()) {
                        Ok(p) => {
                            tracing::info!(
                                "[admin] 收到提案: proposal_id={}, operation={:?}",
                                p.proposal_id,
                                p.operation,
                            );
                            // 自动设置 proposed_by（如果 LLM 未设置）
                            let proposal = Proposal {
                                proposed_by: if p.proposed_by.is_empty() {
                                    "agent-core".to_string()
                                } else {
                                    p.proposed_by
                                },
                                ..p
                            };
                            self.process_proposal(proposal).await
                        }
                        Err(strict_err) => {
                            // 路径 B: 严格解析失败，尝试宽松 fallback 解析
                            tracing::warn!(
                                "[admin] 严格提案解析失败，尝试 fallback: {strict_err}"
                            );
                            match try_parse_proposal_fallback(&raw_value) {
                                Ok(proposal) => {
                                    tracing::info!(
                                        "[admin] fallback 解析成功: proposal_id={}, operation={:?}",
                                        proposal.proposal_id,
                                        proposal.operation,
                                    );
                                    self.process_proposal(proposal).await
                                }
                                Err(fallback_err) => {
                                    tracing::error!(
                                        "[admin] fallback 也失败: {fallback_err}，原始 JSON: {}",
                                        raw_value.to_string().chars().take(300).collect::<String>(),
                                    );
                                    ProposalResult::error(
                                        "unknown",
                                        &format!("提案解析失败（严格+fallback 均失败）: {fallback_err}"),
                                    )
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("[admin] 消息不是合法 JSON: {e}");
                    ProposalResult::error("unknown", &format!("消息体不是合法 JSON: {e}"))
                }
            };

            tracing::info!(
                "[admin] 提案结果: status={:?}, node_id={:?}, error={:?}",
                proposal_result.status,
                proposal_result.node_id,
                proposal_result.error,
            );

            // 始终返回结果，避免 agent-core 空等
            let result_msg = Message::with_json(trace_id, &serde_json::json!(proposal_result));
            let _ = self.data_plane.send("agent-core:result", result_msg).await;
        }

        tracing::info!("[admin] 自演进管理已停止");
    }
}

/// 宽松的提案解析 fallback：当严格反序列化失败时，
/// 从 JSON Value 中手动提取字段，并为缺失的字段填充默认值。
fn try_parse_proposal_fallback(raw: &serde_json::Value) -> Result<Proposal, String> {
    let obj = raw.as_object().ok_or_else(|| "根节点不是 JSON 对象".to_string())?;

    let operation = obj
        .get("operation")
        .and_then(|v| v.as_str())
        .unwrap_or("add");
    let proposal_id = obj
        .get("proposal_id")
        .and_then(|v| v.as_str())
        .unwrap_or("prop-fallback")
        .to_string();
    let proposed_by = obj
        .get("proposed_by")
        .and_then(|v| v.as_str())
        .unwrap_or("agent-core")
        .to_string();
    let reason = obj
        .get("reason")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let proposal_operation = match operation {
        "update" => ProposalOperation::Update,
        "remove" => ProposalOperation::Remove,
        _ => ProposalOperation::Add,
    };

    // fallback 解析 node 字段：为 inputs/outputs 填充默认 port
    let node = obj.get("node").and_then(|n| {
        let mut node_obj = match n.as_object() {
            Some(o) => o.clone(),
            None => return None,
        };

        // 确保 inputs 中每个都有 port 字段
        if let Some(inputs) = node_obj.get_mut("inputs") {
            if let Some(arr) = inputs.as_array_mut() {
                for item in arr.iter_mut() {
                    if let Some(obj_item) = item.as_object_mut() {
                        if !obj_item.contains_key("port") {
                            obj_item.insert("port".to_string(), serde_json::json!("in"));
                        }
                        if !obj_item.contains_key("format") {
                            obj_item.insert("format".to_string(), serde_json::json!("application/json"));
                        }
                    }
                }
            }
        }

        // 确保 outputs 中每个都有 port 字段
        if let Some(outputs) = node_obj.get_mut("outputs") {
            if let Some(arr) = outputs.as_array_mut() {
                for item in arr.iter_mut() {
                    if let Some(obj_item) = item.as_object_mut() {
                        if !obj_item.contains_key("port") {
                            obj_item.insert("port".to_string(), serde_json::json!("out"));
                        }
                        if !obj_item.contains_key("format") {
                            obj_item.insert("format".to_string(), serde_json::json!("application/json"));
                        }
                    }
                }
            }
        }

        // 确保 runtime 字段存在
        if !node_obj.contains_key("runtime") {
            node_obj.insert(
                "runtime".to_string(),
                serde_json::json!({
                    "kind": "python"
                }),
            );
        }

        // 尝试用修补后的值反序列化 NodeDef
        let patched = serde_json::Value::Object(node_obj);
        match serde_json::from_value::<sea_common::NodeDef>(patched) {
            Ok(n) => Some(n),
            Err(e) => {
                tracing::warn!("[admin] fallback node 解析仍失败: {e}");
                None
            }
        }
    });

    let node_id = obj
        .get("node_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let proposal = Proposal {
        proposal_id,
        proposed_by,
        operation: proposal_operation,
        node,
        node_id,
        changes: None,
        reason,
    };

    Ok(proposal)
}
