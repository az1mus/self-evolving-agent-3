use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 审计事件类型。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AuditEventType {
    /// 服务进入 READY 状态。
    ServiceStart,

    /// 服务退出 (含退出码和终止原因)。
    ServiceExit,

    /// access_check 返回 Deny。
    AccessViolation,

    /// privilege_transition 执行。
    PrivilegeDrop,

    /// dynamic_spawn 创建新服务。
    DynamicSpawn,

    /// 通道异常 (满、断开、超时)。
    ChannelError,

    /// 自演进提案。
    ProposalEvent,

    /// 运行时启动。
    RuntimeStart,

    /// 运行时关闭。
    RuntimeShutdown,

    /// 死信丢弃。
    DeadLetterDropped,

    /// 动作解析失败。
    ActionParseFailure,
}

/// 审计事件。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_type: AuditEventType,
    pub timestamp_ms: i64,
    pub data: HashMap<String, String>,
}

impl AuditEvent {
    pub fn new(event_type: AuditEventType) -> Self {
        Self {
            event_type,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            data: HashMap::new(),
        }
    }

    pub fn with_data(mut self, key: &str, value: &str) -> Self {
        self.data.insert(key.to_string(), value.to_string());
        self
    }
}

/// 审计批量上报消息 (SDK → 控制面)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditBatch {
    pub seq: u64,
    pub events: Vec<AuditEvent>,
}

impl AuditBatch {
    pub fn new(seq: u64, events: Vec<AuditEvent>) -> Self {
        Self { seq, events }
    }
}

/// 审计批量 ACK (控制面 → SDK)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditAck {
    pub seq: u64,
}

impl AuditAck {
    pub fn new(seq: u64) -> Self {
        Self { seq }
    }
}

/// 审计日志收集器 (简化版)。
#[derive(Debug, Default)]
pub struct AuditLogger {
    events: Vec<AuditEvent>,
}

impl AuditLogger {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    /// 记录一条审计事件。
    pub fn log(&mut self, event_type: AuditEventType, data: HashMap<String, String>) {
        self.events.push(AuditEvent {
            event_type,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            data,
        });
    }

    /// 生成批量上报。
    pub fn drain_batch(&mut self) -> AuditBatch {
        let seq = chrono::Utc::now().timestamp_millis() as u64;
        let events = std::mem::take(&mut self.events);
        AuditBatch::new(seq, events)
    }

    /// 清空所有事件。
    pub fn clear(&mut self) {
        self.events.clear();
    }
}
