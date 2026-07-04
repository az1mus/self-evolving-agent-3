use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 通道标识符。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ChannelId {
    pub source: String,
    pub target: String,
}

impl ChannelId {
    pub fn new(source: &str, target: &str) -> Self {
        Self {
            source: source.to_string(),
            target: target.to_string(),
        }
    }
}

impl std::fmt::Display for ChannelId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}->{}", self.source, self.target)
    }
}

/// 通道拓扑表 —— 管理所有端口间的消息路由路径。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelTopology {
    /// `"source_node:port" -> ["target_node:port", ...]`
    pub routes: HashMap<String, Vec<String>>,

    /// 通道容量 (默认 256)。
    #[serde(default = "default_capacity")]
    pub capacity: usize,
}

const fn default_capacity() -> usize {
    256
}

impl ChannelTopology {
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
            capacity: 256,
        }
    }

    /// 添加通道绑定。
    pub fn add_channel(&mut self, source: &str, target: &str) {
        self.routes
            .entry(source.to_string())
            .or_default()
            .push(target.to_string());
    }

    /// 移除通道绑定。
    pub fn remove_channel(&mut self, source: &str, target: &str) {
        if let Some(targets) = self.routes.get_mut(source) {
            targets.retain(|t| t != target);
            if targets.is_empty() {
                self.routes.remove(source);
            }
        }
    }

    /// 获取从 source 出发的所有目标。
    pub fn targets_for(&self, source: &str) -> Vec<&str> {
        self.routes
            .get(source)
            .map(|t| t.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// 移除与特定节点相关的所有通道。
    pub fn remove_all_for(&mut self, node_id: &str) {
        // 删除以该节点为源的所有路由
        self.routes.retain(|source, _| {
            let src_node = source.split(':').next().unwrap_or("");
            src_node != node_id
        });

        // 删除以该节点为目标的所有引用
        for targets in self.routes.values_mut() {
            targets.retain(|t| {
                let tgt_node = t.split(':').next().unwrap_or("");
                tgt_node != node_id
            });
        }

        // 清理空条目
        self.routes.retain(|_, targets| !targets.is_empty());
    }

    /// 检查是否存在通道引用到指定节点。
    pub fn references_node(&self, node_id: &str) -> bool {
        // 检查作为源
        for source in self.routes.keys() {
            if source.split(':').next() == Some(node_id) {
                return true;
            }
        }
        // 检查作为目标
        for targets in self.routes.values() {
            for target in targets {
                if target.split(':').next() == Some(node_id) {
                    return true;
                }
            }
        }
        false
    }

    /// 获取所有涉及指定节点的通道 (source->target)。
    pub fn channels_for_node(&self, node_id: &str) -> Vec<(String, String)> {
        let mut result = Vec::new();
        for (source, targets) in &self.routes {
            let src_node = source.split(':').next().unwrap_or("");
            if src_node == node_id {
                for target in targets {
                    result.push((source.clone(), target.clone()));
                }
            }
        }
        // 也需要收集以该节点为目标的通道
        for (source, targets) in &self.routes {
            for target in targets {
                let tgt_node = target.split(':').next().unwrap_or("");
                if tgt_node == node_id {
                    let entry = (source.clone(), target.clone());
                    if !result.contains(&entry) {
                        result.push(entry);
                    }
                }
            }
        }
        result
    }

    /// 返回拓扑的总通道数。
    pub fn channel_count(&self) -> usize {
        self.routes.values().map(|t| t.len()).sum()
    }
}

impl Default for ChannelTopology {
    fn default() -> Self {
        Self::new()
    }
}

/// 通道统计信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelStats {
    pub msg_sent: u64,
    pub msg_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub queue_depth: u32,
    pub queue_capacity: u32,

    /// 使用率百分比。
    pub usage_percent: f64,

    /// 死信计数：因通道满或关闭而丢弃的消息总数。
    pub dead_letter_count: u64,

    /// 背压事件计数：通道使用率超过阈值的次数。
    pub backpressure_events: u64,
}

impl ChannelStats {
    pub fn new(queue_capacity: u32) -> Self {
        Self {
            msg_sent: 0,
            msg_received: 0,
            bytes_sent: 0,
            bytes_received: 0,
            queue_depth: 0,
            queue_capacity,
            usage_percent: 0.0,
            dead_letter_count: 0,
            backpressure_events: 0,
        }
    }
}
