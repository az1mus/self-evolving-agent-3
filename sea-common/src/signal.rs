use serde::{Deserialize, Serialize};

use crate::message::Message;

/// 应用层信号，非 OS 信号。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Signal {
    /// 暂停接收新消息。
    #[serde(rename = "pause")]
    Pause,

    /// 恢复接收新消息。
    #[serde(rename = "resume")]
    Resume,

    /// 配置热更新。
    #[serde(rename = "reload")]
    Reload,

    /// 优雅终止。
    #[serde(rename = "terminate")]
    Terminate,

    /// 强制终止。
    #[serde(rename = "kill")]
    Kill,

    /// 自定义信号。
    #[serde(rename = "custom")]
    Custom(String),
}

impl Signal {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::Reload => "reload",
            Self::Terminate => "terminate",
            Self::Kill => "kill",
            Self::Custom(_) => "custom",
        }
    }
}

impl std::fmt::Display for Signal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Custom(detail) => write!(f, "custom({detail})"),
            other => write!(f, "{}", other.as_str()),
        }
    }
}

/// 信号通道 —— 管理所有节点的信号收发。
#[derive(Debug)]
pub struct SignalChannel {
    senders: std::collections::HashMap<String, tokio::sync::mpsc::Sender<Signal>>,
}

impl SignalChannel {
    pub fn new() -> Self {
        Self {
            senders: std::collections::HashMap::new(),
        }
    }

    /// 注册节点的信号接收器。
    pub fn register(
        &mut self,
        node_id: &str,
        rx: tokio::sync::mpsc::Receiver<Signal>,
    ) -> tokio::sync::mpsc::Sender<Signal> {
        let (tx, _new_rx) = tokio::sync::mpsc::channel(16);
        // 将传入的 rx 替换为新的 rx (实际使用者应持有 rx)
        drop(rx);
        self.senders.insert(node_id.to_string(), tx.clone());
        tx
    }

    /// 向指定节点发送信号。
    pub async fn send(&self, node_id: &str, signal: Signal) -> Result<(), crate::SeaError> {
        let sender = self
            .senders
            .get(node_id)
            .ok_or_else(|| crate::SeaError::NodeNotFound(node_id.to_string()))?;
        sender
            .send(signal)
            .await
            .map_err(|_| crate::SeaError::ChannelBroken(node_id.to_string()))
    }

    /// 向所有节点广播信号。
    pub async fn broadcast(&self, signal: &Signal) {
        for sender in self.senders.values() {
            let _ = sender.send(signal.clone()).await;
        }
    }

    /// 移除节点的信号通道 (节点销毁时调用)。
    pub fn unregister(&mut self, node_id: &str) {
        self.senders.remove(node_id);
    }
}

impl Default for SignalChannel {
    fn default() -> Self {
        Self::new()
    }
}

/// sentinel 值 —— 标记"完成当前任务后退出"。
pub const COMPLETE_AND_EXIT: &str = "<complete_and_exit>";

/// 状态机状态。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeState {
    #[serde(rename = "ready")]
    Ready,

    #[serde(rename = "running")]
    Running,

    #[serde(rename = "paused")]
    Paused,

    #[serde(rename = "stopping")]
    Stopping,

    #[serde(rename = "stopped")]
    Stopped,
}

impl NodeState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        }
    }
}

/// 背压通知结构。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Backpressure {
    pub source_service: String,
    pub channel_id: String,
    pub level: BackpressureLevel,
    pub queue_depth: u32,
    pub suggested_pause_ms: u32,
}

/// 背压等级。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BackpressureLevel {
    #[serde(rename = "low")]
    Low,

    #[serde(rename = "medium")]
    Medium,

    #[serde(rename = "high")]
    High,

    #[serde(rename = "critical")]
    Critical,
}

/// 死信消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetter {
    pub original_message: Message,
    pub reason: DeadLetterReason,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub context: std::collections::HashMap<String, String>,
}

/// 死信原因。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DeadLetterReason {
    #[serde(rename = "no_route")]
    NoRoute,

    #[serde(rename = "ttl_expired")]
    TtlExpired,

    #[serde(rename = "channel_broken")]
    ChannelBroken,

    #[serde(rename = "channel_full")]
    ChannelFull,
}
