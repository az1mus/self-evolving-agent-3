use std::collections::{HashMap, HashSet};

use crate::identity::PrivilegeMask;

/// access_check 判定结果。
#[derive(Debug, Clone, PartialEq)]
pub enum AccessDecision {
    /// 允许通过。
    Permit,
    /// 拒绝，包含原因。
    Deny(String),
}

/// 通道白名单条目。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WhiteListEntry {
    pub source: String,
    pub source_port: String,
    pub target: String,
    pub target_port: String,
}

impl WhiteListEntry {
    pub fn new(source: &str, source_port: &str, target: &str, target_port: &str) -> Self {
        Self {
            source: source.to_string(),
            source_port: source_port.to_string(),
            target: target.to_string(),
            target_port: target_port.to_string(),
        }
    }
}

impl std::fmt::Display for WhiteListEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}->{}:{}",
            self.source, self.source_port, self.target, self.target_port
        )
    }
}

/// 访问控制器 —— 执行通道白名单和权限检查。
#[derive(Debug)]
pub struct AccessChecker {
    /// 静态白名单 (由 group config 声明)。
    whitelist: HashSet<WhiteListEntry>,

    /// 运行时动态创建通道的白名单。
    runtime_whitelist: HashSet<WhiteListEntry>,

    /// 节点身份表。
    identities: HashMap<String, String>,

    /// 节点类型表 (in_process / separate_process)。
    node_types: HashMap<String, String>,

    /// 特权掩码表。
    privilege_masks: HashMap<String, PrivilegeMask>,

    /// 进程内节点互信 (默认 true)。
    in_process_trusted: bool,
}

impl AccessChecker {
    pub fn new() -> Self {
        Self {
            whitelist: HashSet::new(),
            runtime_whitelist: HashSet::new(),
            identities: HashMap::new(),
            node_types: HashMap::new(),
            privilege_masks: HashMap::new(),
            in_process_trusted: true,
        }
    }

    /// 从通道拓扑加载静态白名单。
    pub fn load_channels(
        &mut self,
        channels: &crate::channel::ChannelTopology,
    ) {
        for (source, targets) in &channels.routes {
            let (src_node, src_port) = source.split_once(':').unwrap_or((source, "out"));
            for target in targets {
                let (tgt_node, tgt_port) = target.split_once(':').unwrap_or((target, "in"));
                self.whitelist.insert(WhiteListEntry::new(
                    src_node, src_port, tgt_node, tgt_port,
                ));
            }
        }
    }

    /// 注册节点身份。
    pub fn register_node(&mut self, node_id: &str, principal: &str) {
        self.identities
            .insert(node_id.to_string(), principal.to_string());
    }

    /// 记录节点类型，用于进程内互信判断。
    pub fn set_node_type(&mut self, node_id: &str, is_separate_process: bool) {
        self.node_types.insert(
            node_id.to_string(),
            if is_separate_process {
                "separate_process"
            } else {
                "in_process"
            }
            .to_string(),
        );
    }

    /// 绑定初始权限。
    pub fn set_privileges(&mut self, node_id: &str, privileges: &[String]) {
        self.privilege_masks
            .insert(node_id.to_string(), PrivilegeMask::new(privileges));
    }

    /// 运行时注册通道到白名单。
    pub fn register_channel(
        &mut self,
        source: &str,
        source_port: &str,
        target: &str,
        target_port: &str,
    ) {
        self.runtime_whitelist.insert(WhiteListEntry::new(
            source, source_port, target, target_port,
        ));
    }

    /// 权限判定。
    pub fn check(
        &self,
        source_node: &str,
        source_port: &str,
        target_node: &str,
        target_port: &str,
    ) -> AccessDecision {
        // 0) 进程内互信
        let source_type = self.node_types.get(source_node).map(|s| s.as_str());
        let target_type = self.node_types.get(target_node).map(|s| s.as_str());
        if self.in_process_trusted {
            let src_sep = source_type == Some("separate_process");
            let tgt_sep = target_type == Some("separate_process");
            if !src_sep && !tgt_sep {
                return AccessDecision::Permit;
            }
        }

        // 1) 检查特权掩码：发送方是否有 channel_send 权限
        if let Some(mask) = self.privilege_masks.get(source_node) {
            if !mask.has(crate::identity::privileges::CHANNEL_SEND) {
                return AccessDecision::Deny("缺少 channel_send 权限 (已降权)".to_string());
            }
        }

        // 2) 检查白名单
        let entry = WhiteListEntry::new(source_node, source_port, target_node, target_port);
        if self.whitelist.contains(&entry) || self.runtime_whitelist.contains(&entry) {
            return AccessDecision::Permit;
        }

        AccessDecision::Deny("通道不在白名单中".to_string())
    }

    /// 不可逆降权。
    pub fn drop_privilege(&mut self, node_id: &str, privilege: &str) -> bool {
        if let Some(mask) = self.privilege_masks.get_mut(node_id) {
            mask.drop(privilege)
        } else {
            false
        }
    }

    /// 查询节点是否持有指定权限。
    pub fn has_privilege(&self, node_id: &str, privilege: &str) -> bool {
        self.privilege_masks
            .get(node_id)
            .map(|mask| mask.has(privilege))
            .unwrap_or(false)
    }
}

impl Default for AccessChecker {
    fn default() -> Self {
        Self::new()
    }
}
