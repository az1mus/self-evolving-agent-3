use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::SeaResult;

/// 消息优先级范围: 0–255, 默认 128。
pub const DEFAULT_PRIORITY: u8 = 128;
/// 默认消息 TTL (毫秒)。
pub const DEFAULT_TTL_MS: u64 = 30_000;
/// 默认通道容量。
pub const DEFAULT_CHANNEL_CAPACITY: usize = 256;

/// 父追踪信息，用于子请求场景。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParentTrace {
    pub parent_message_id: String,
    pub sequence: u32,
}

impl ParentTrace {
    pub fn new(parent_message_id: String, sequence: u32) -> Self {
        Self {
            parent_message_id,
            sequence,
        }
    }
}

/// 核心消息类型 —— 系统中所有节点间通信的唯一载体。
///
/// 遵循 IFP v3.1 消息格式规范：
/// - `trace_id` 在一次外部请求进入系统时创建，贯穿全部子消息
/// - `message_id` 每条消息唯一
/// - `in_response_to` 用于直接的一对一回复
/// - `parent_trace` 用于子请求追踪
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    /// 追踪链 ID，贯穿一次完整请求的全部消息。
    pub trace_id: String,

    /// 消息唯一标识 (UUID v4)。
    pub message_id: String,

    /// 消息载荷 (原始字节)。
    #[serde(with = "serde_bytes")]
    pub content: Vec<u8>,

    /// 载荷 MIME 类型 (默认 `application/octet-stream`)。
    #[serde(default = "default_content_type")]
    pub content_type: String,

    /// 优先级 0-255 (默认 128)。
    #[serde(default = "default_priority")]
    pub priority: u8,

    /// 生存时间 (毫秒)。
    #[serde(default = "default_ttl_ms")]
    pub ttl_ms: u64,

    /// 创建时间。
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,

    /// 被回复的消息 ID (非子请求场景下的直接回复)。
    #[serde(default)]
    pub in_response_to: Option<String>,

    /// 父消息追踪 (子请求场景)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_trace: Option<ParentTrace>,

    /// 是否为流式消息分片。
    #[serde(default)]
    pub stream: bool,

    /// 是否为流式消息的最后一片。
    #[serde(default)]
    pub stream_final: bool,
}

fn default_content_type() -> String {
    "application/octet-stream".to_string()
}

fn default_priority() -> u8 {
    DEFAULT_PRIORITY
}

fn default_ttl_ms() -> u64 {
    DEFAULT_TTL_MS
}

impl Message {
    /// 创建一个新的消息。
    pub fn new(trace_id: String, content: Vec<u8>) -> Self {
        Self {
            trace_id,
            message_id: Uuid::new_v4().to_string(),
            content,
            content_type: default_content_type(),
            priority: DEFAULT_PRIORITY,
            ttl_ms: DEFAULT_TTL_MS,
            created_at: Utc::now(),
            in_response_to: None,
            parent_trace: None,
            stream: false,
            stream_final: false,
        }
    }

    /// 快速创建 JSON 载荷消息。
    pub fn with_json(trace_id: String, value: &serde_json::Value) -> Self {
        let content = serde_json::to_vec(value).unwrap_or_default();
        Self {
            trace_id,
            message_id: Uuid::new_v4().to_string(),
            content,
            content_type: "application/json".to_string(),
            priority: DEFAULT_PRIORITY,
            ttl_ms: DEFAULT_TTL_MS,
            created_at: Utc::now(),
            in_response_to: None,
            parent_trace: None,
            stream: false,
            stream_final: false,
        }
    }

    /// 创建文本载荷消息。
    pub fn with_text(trace_id: String, text: &str) -> Self {
        Self {
            trace_id,
            message_id: Uuid::new_v4().to_string(),
            content: text.as_bytes().to_vec(),
            content_type: "text/plain".to_string(),
            priority: DEFAULT_PRIORITY,
            ttl_ms: DEFAULT_TTL_MS,
            created_at: Utc::now(),
            in_response_to: None,
            parent_trace: None,
            stream: false,
            stream_final: false,
        }
    }

    /// 创建 markdown 载荷消息。
    pub fn with_markdown(trace_id: String, md: &str) -> Self {
        Self {
            trace_id,
            message_id: Uuid::new_v4().to_string(),
            content: md.as_bytes().to_vec(),
            content_type: "text/markdown".to_string(),
            priority: DEFAULT_PRIORITY,
            ttl_ms: DEFAULT_TTL_MS,
            created_at: Utc::now(),
            in_response_to: None,
            parent_trace: None,
            stream: false,
            stream_final: false,
        }
    }

    /// 设置为对另一消息的回复。
    pub fn reply_to(mut self, in_response_to: &str) -> Self {
        self.in_response_to = Some(in_response_to.to_string());
        self
    }

    /// 设置为子请求。
    pub fn with_parent(mut self, parent_message_id: &str, sequence: u32) -> Self {
        self.parent_trace = Some(ParentTrace::new(
            parent_message_id.to_string(),
            sequence,
        ));
        self
    }

    /// 设置流式标记。
    pub fn with_stream(mut self, stream: bool, stream_final: bool) -> Self {
        self.stream = stream;
        self.stream_final = stream_final;
        self
    }

    /// 将载荷解析为 JSON。
    pub fn content_as_json(&self) -> SeaResult<serde_json::Value> {
        serde_json::from_slice(&self.content)
            .map_err(|e| crate::SeaError::InvalidArg(format!("JSON 解析失败: {e}")))
    }

    /// 将载荷解析为 UTF-8 文本。
    pub fn content_as_text(&self) -> SeaResult<String> {
        String::from_utf8(self.content.clone())
            .map_err(|e| crate::SeaError::InvalidArg(format!("UTF-8 解码失败: {e}")))
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            trace_id: String::new(),
            message_id: Uuid::new_v4().to_string(),
            content: Vec::new(),
            content_type: default_content_type(),
            priority: DEFAULT_PRIORITY,
            ttl_ms: DEFAULT_TTL_MS,
            created_at: Utc::now(),
            in_response_to: None,
            parent_trace: None,
            stream: false,
            stream_final: false,
        }
    }
}
