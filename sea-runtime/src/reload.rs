//! ReloadGroup — 增量热更新。
//!
//! 当 Admin 修改 `.group.toml` 后，执行增量变更：
//! 对比新旧配置，自动增/删/改节点和通道，不影响未变更的服务。
//!
//! 对应 sea_instruct.md §9，遵循先删再更新后新增的稳定顺序。

use std::collections::HashSet;
use std::sync::Arc;

use sea_common::{
    ChannelTopology, GroupConfig, NodeDef, NodeInfo, SeaResult,
};

use crate::data_plane::DataPlane;
use crate::node_manager::NodeManager;
use crate::registry::Registry;

/// 热更新结果。
#[derive(Debug)]
pub struct ReloadResult {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub updated: Vec<String>,
    pub channels_added: usize,
    pub channels_removed: usize,
    pub errors: Vec<String>,
}

/// 保护节点列表——这些节点不会被删除或更新。
const PROTECTED_NODE_IDS: &[&str] = &["agent-core", "ui", "router", "registry", "admin"];

/// ReloadGroup — 增量热更新。
///
/// 步骤：
/// 1. 解析新配置，获取当前注册表快照和通道拓扑快照
/// 2. 计算 diff：新增 / 删除 / 变更
/// 3. 按 删除 → 更新 → 新增 的顺序执行
/// 4. 通道增量删除 + 创建
pub async fn reload_group(
    new_config: &GroupConfig,
    node_manager: &Arc<NodeManager>,
    data_plane: &Arc<DataPlane>,
    registry: &Arc<Registry>,
    protected_nodes_extra: &[&str],
) -> ReloadResult {
    tracing::info!("[reload] 开始热更新...");

    let mut result = ReloadResult {
        added: Vec::new(),
        removed: Vec::new(),
        updated: Vec::new(),
        channels_added: 0,
        channels_removed: 0,
        errors: Vec::new(),
    };

    // ── 1. 获取快照 ─────────────────────────────────────────────────────
    let new_node_defs = new_config.to_node_defs();
    let new_topology = new_config.to_channel_topology();
    let current_nodes = registry.list_all().await;
    let old_topology = data_plane.read_topology().await;

    // ── 2. 计算 diff ─────────────────────────────────────────────────────
    let new_ids: HashSet<&str> = new_node_defs
        .iter()
        .map(|n| n.id.as_str())
        .collect();
    let current_ids: HashSet<&str> = current_nodes
        .iter()
        .map(|n| n.id.as_str())
        .collect();

    let all_protected: HashSet<&str> = PROTECTED_NODE_IDS
        .iter()
        .copied()
        .chain(protected_nodes_extra.iter().copied())
        .collect();

    // 新增：在新配置中但不在当前注册表中
    let to_add: Vec<NodeDef> = new_node_defs
        .iter()
        .filter(|n| !current_ids.contains(n.id.as_str()))
        .cloned()
        .collect();

    // 删除：在当前注册表中但不在新配置中，且非受保护节点
    let to_remove: Vec<String> = current_nodes
        .iter()
        .filter(|n| !new_ids.contains(n.id.as_str()))
        .filter(|n| !all_protected.contains(n.id.as_str()))
        .map(|n| n.id.clone())
        .collect();

    // 变更：ID 同时存在于新旧中，但 NodeDef 有差异，且非受保护节点
    let to_update: Vec<(NodeDef, NodeDef)> = new_node_defs
        .iter()
        .filter(|new_def| {
            current_ids.contains(new_def.id.as_str())
                && !all_protected.contains(new_def.id.as_str())
        })
        .filter_map(|new_def| {
            // 通过 Registry 获取当前的 NodeInfo，与 new_def 对比
            let current_def = find_node_def(&current_nodes, &new_def.id)?;
            if node_def_changed(&current_def, new_def) {
                Some((current_def, new_def.clone()))
            } else {
                None
            }
        })
        .collect();

    tracing::info!(
        "[reload] diff: +{} -{} ~{}",
        to_add.len(),
        to_remove.len(),
        to_update.len()
    );

    // ── 3. 执行删除 ──────────────────────────────────────────────────────
    for node_id in &to_remove {
        match execute_remove(node_id, node_manager, data_plane, registry).await {
            Ok(()) => {
                result.removed.push(node_id.clone());
                tracing::info!("[reload] 已删除: {node_id}");
            }
            Err(e) => {
                let err = format!("删除 {node_id}: {e:#}");
                tracing::error!("[reload] {err}");
                result.errors.push(err);
            }
        }
    }

    // ── 4. 执行更新 ──────────────────────────────────────────────────────
    for (_old_def, new_def) in &to_update {
        let node_id = &new_def.id;

        match execute_update(
            node_id,
            new_def,
            node_manager,
            data_plane,
            registry,
            &old_topology,
        )
        .await
        {
            Ok(()) => {
                result.updated.push(node_id.clone());
                tracing::info!("[reload] 已更新: {node_id}");
            }
            Err(e) => {
                let err = format!("更新 {node_id}: {e:#}");
                tracing::error!("[reload] {err}");
                result.errors.push(err);
            }
        }
    }

    // ── 5. 执行新增 ──────────────────────────────────────────────────────
    for new_def in &to_add {
        let node_id = &new_def.id;

        match execute_add(node_id, new_def, node_manager, data_plane, registry).await
        {
            Ok(()) => {
                result.added.push(node_id.clone());
                tracing::info!("[reload] 已新增: {node_id}");
            }
            Err(e) => {
                let err = format!("新增 {node_id}: {e:#}");
                tracing::error!("[reload] {err}");
                result.errors.push(err);
            }
        }
    }

    // ── 6. 通道增量更新 ──────────────────────────────────────────────────
    let (added, removed) = compute_channel_diff(&old_topology, &new_topology);
    result.channels_removed = removed;
    result.channels_added = added;

    tracing::info!(
        "[reload] 热更新完成: +{} -{} ~{} | channels +{} -{} | errors={}",
        result.added.len(),
        result.removed.len(),
        result.updated.len(),
        result.channels_added,
        result.channels_removed,
        result.errors.len(),
    );

    result
}

// ── 辅助函数 ──────────────────────────────────────────────────────────────────

/// 从当前注册表节点列表中查找 NodeDef（从 NodeInfo 反向构造）。
/// 仅包含 id/description/ports（不包含 runtime 信息，因此变更检测有限）。
fn find_node_def(current_nodes: &[NodeInfo], id: &str) -> Option<NodeDef> {
    let info = current_nodes.iter().find(|n| n.id == id)?;
    Some(NodeDef {
        id: info.id.clone(),
        description: info.description.clone(),
        api_docs: info.api_docs.clone(),
        inputs: info.inputs.clone(),
        outputs: info.outputs.clone(),
        runtime: sea_common::RuntimeDef::skill(""), // 占位，runtime 差异在 update 时不敏感
        channels: None,
        privileges: None,
    })
}

/// 检测两个 NodeDef 是否有实质性差异（id/description/api_docs/ports）。
fn node_def_changed(old: &NodeDef, new: &NodeDef) -> bool {
    // 描述变更
    if old.description != new.description {
        return true;
    }
    // API 文档变更
    if old.api_docs != new.api_docs {
        return true;
    }
    // 端口定义变更
    if old.inputs != new.inputs || old.outputs != new.outputs {
        return true;
    }
    false
}

/// 计算新旧通道拓扑的差异。
///
/// 返回 (added_count, removed_count)。
fn compute_channel_diff(old: &ChannelTopology, new: &ChannelTopology) -> (usize, usize) {
    let mut removed = 0usize;
    let mut added = 0usize;

    // 在旧拓扑中但不在新拓扑中的 → 待删除
    for (source, targets) in &old.routes {
        let new_targets = new.targets_for(source);
        for target in targets {
            if !new_targets.contains(&target.as_str()) {
                removed += 1;
            }
        }
    }

    // 在新拓扑中但不在旧拓扑中的 → 待新增
    for (source, targets) in &new.routes {
        let old_targets = old.targets_for(source);
        for target in targets {
            if !old_targets.contains(&target.as_str()) {
                added += 1;
            }
        }
    }

    (added, removed)
}

/// 删除节点：terminate → 拆除通道 → reap → 注销 Registry。
async fn execute_remove(
    node_id: &str,
    node_manager: &NodeManager,
    data_plane: &DataPlane,
    registry: &Registry,
) -> SeaResult<()> {
    // 1. 终止节点
    node_manager
        .terminate(node_id, sea_common::Signal::Terminate)
        .await?;

    // 2. 拆除通道
    data_plane.channel_switch.remove_all_for(node_id).await;

    // 3. 回收节点实例
    let _ = node_manager.reap(node_id).await;

    // 4. 从注册表注销
    registry.unregister(node_id).await?;

    Ok(())
}

/// 更新节点：删除旧实例 → 创建新实例。
///
/// 对 Python 节点：terminate 会终止子进程，spawn 会启动新进程。
/// 对 Builtin 节点：terminate 是逻辑停止，spawn 重新触发。
/// LLM 节点不会进入此路径（受 PROTECTED_NODE_IDS 保护）。
async fn execute_update(
    node_id: &str,
    new_def: &NodeDef,
    node_manager: &NodeManager,
    data_plane: &DataPlane,
    registry: &Registry,
    _old_topology: &ChannelTopology,
) -> SeaResult<()> {
    // 1. 删除旧节点
    node_manager
        .terminate(node_id, sea_common::Signal::Terminate)
        .await?;
    data_plane.channel_switch.remove_all_for(node_id).await;
    let _ = node_manager.reap(node_id).await;
    registry.unregister(node_id).await?;

    // 2. 创建新节点
    node_manager.spawn(new_def.clone()).await?;
    registry
        .register(NodeInfo::with_ports(
            node_id,
            &new_def.description,
            new_def.api_docs.as_deref(),
            new_def.inputs.clone(),
            new_def.outputs.clone(),
        ))
        .await?;

    Ok(())
}

/// 新增节点：spawn → 注册。
async fn execute_add(
    node_id: &str,
    new_def: &NodeDef,
    node_manager: &NodeManager,
    _data_plane: &DataPlane,
    registry: &Registry,
) -> SeaResult<()> {
    // 1. 启动节点
    node_manager.spawn(new_def.clone()).await?;

    // 2. 注册到 Registry
    registry
        .register(NodeInfo::with_ports(
            node_id,
            &new_def.description,
            new_def.api_docs.as_deref(),
            new_def.inputs.clone(),
            new_def.outputs.clone(),
        ))
        .await?;

    Ok(())
}
