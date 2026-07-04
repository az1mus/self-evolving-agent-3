use serde::{Deserialize, Serialize};

use crate::error::SeaResult;

// ─── LLM 请求/响应类型 ───

/// LLM Chat Completion 请求。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmRequest {
    pub model: String,
    pub messages: Vec<LlmMessage>,
    pub system: Option<String>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u32>,
    pub stream: bool,
}

impl LlmRequest {
    pub fn new(model: &str) -> Self {
        Self {
            model: model.to_string(),
            messages: Vec::new(),
            system: None,
            temperature: None,
            max_tokens: None,
            stream: false,
        }
    }

    pub fn with_system(mut self, system: &str) -> Self {
        self.system = Some(system.to_string());
        self
    }

    pub fn with_message(mut self, msg: LlmMessage) -> Self {
        self.messages.push(msg);
        self
    }

    pub fn with_messages(mut self, msgs: Vec<LlmMessage>) -> Self {
        self.messages = msgs;
        self
    }

    pub fn with_temperature(mut self, t: f64) -> Self {
        self.temperature = Some(t);
        self
    }

    pub fn with_stream(mut self, stream: bool) -> Self {
        self.stream = stream;
        self
    }
}

/// LLM 对话消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmMessage {
    pub role: String, // "user" | "assistant" | "system"
    pub content: String,
}

impl LlmMessage {
    pub fn user(content: &str) -> Self {
        Self {
            role: "user".to_string(),
            content: content.to_string(),
        }
    }

    pub fn assistant(content: &str) -> Self {
        Self {
            role: "assistant".to_string(),
            content: content.to_string(),
        }
    }

    pub fn system(content: &str) -> Self {
        Self {
            role: "system".to_string(),
            content: content.to_string(),
        }
    }
}

/// OpenAI 兼容的 Chat Completion API 响应。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmResponse {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub object: String,
    #[serde(default)]
    pub created: u64,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub choices: Vec<LlmChoice>,
    #[serde(default)]
    pub usage: Option<LlmUsage>,
}

/// LLM 回复选项。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmChoice {
    pub index: u32,
    pub message: LlmResponseMessage,
    #[serde(default)]
    pub finish_reason: Option<String>,
}

/// LLM 回复消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmResponseMessage {
    pub role: String,
    pub content: String,
}

/// Token 用量。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

// ─── SSE 流式分片 ───

/// SSE 流式数据块（SSE `data:` 行解析结果）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmStreamChunk {
    pub id: Option<String>,
    pub object: Option<String>,
    pub created: Option<u64>,
    pub model: Option<String>,
    pub choices: Vec<LlmStreamChoice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmStreamChoice {
    pub index: u32,
    pub delta: LlmStreamDelta,
    #[serde(default)]
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmStreamDelta {
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
}

// ─── LLM 配置 ───

/// LLM 连接配置（从环境变量或配置文件读取）。
#[derive(Debug, Clone)]
pub struct LlmConfig {
    /// API 端点 URL (如 `https://api.openai.com/v1/chat/completions`)。
    pub api_url: String,
    /// API Key。
    pub api_key: String,
    /// 模型名称。
    pub model: String,
    /// 请求超时（秒）。
    pub timeout_secs: u64,
}

impl LlmConfig {
    /// 从环境变量构建 LLM 配置。
    ///
    /// - `SEA_API_URL` — API 端点（默认 `https://api.openai.com/v1/chat/completions`）
    /// - `SEA_API_KEY` — API Key（必须）
    /// - `SEA_MODEL` — 模型名（默认 `gpt-4o`）
    /// - `SEA_TIMEOUT` — 超时秒数（默认 60）
    pub fn from_env() -> SeaResult<Self> {
        let api_url = std::env::var("SEA_API_URL").unwrap_or_else(|_| {
            "https://api.openai.com/v1/chat/completions".to_string()
        });
        let api_key = std::env::var("SEA_API_KEY")
            .map_err(|_| crate::error::SeaError::InvalidArg("SEA_API_KEY 环境变量未设置".into()))?;
        let model = std::env::var("SEA_MODEL")
            .unwrap_or_else(|_| "gpt-4o".to_string());
        let timeout_secs = std::env::var("SEA_TIMEOUT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60);

        Ok(Self {
            api_url,
            api_key,
            model,
            timeout_secs,
        })
    }
}

/// 对话历史条目（供 agent-core 维护上下文使用）。
#[derive(Debug, Clone)]
pub struct ConversationEntry {
    pub role: String,
    pub content: String,
    pub trace_id: Option<String>,
}

impl ConversationEntry {
    pub fn user(content: &str, trace_id: &str) -> Self {
        Self {
            role: "user".to_string(),
            content: content.to_string(),
            trace_id: Some(trace_id.to_string()),
        }
    }

    pub fn assistant(content: &str) -> Self {
        Self {
            role: "assistant".to_string(),
            content: content.to_string(),
            trace_id: None,
        }
    }

    pub fn system(content: &str) -> Self {
        Self {
            role: "system".to_string(),
            content: content.to_string(),
            trace_id: None,
        }
    }

    /// 转换为 LLM API 消息格式。
    pub fn to_llm_message(&self) -> LlmMessage {
        LlmMessage {
            role: self.role.clone(),
            content: self.content.clone(),
        }
    }
}

/// 动作类型 — LLM 回复中解析出的动作。
#[derive(Debug, Clone)]
pub enum LlmAction {
    /// 直接回复用户。
    Reply {
        content: String,
    },
    /// 调用子节点。
    Call {
        target: Option<String>,
        capability: Option<String>,
        payload: serde_json::Value,
    },
    /// 自演进提案。
    Register {
        proposal: serde_json::Value,
    },
}

/// 动作解析结果。
#[derive(Debug, Clone)]
pub struct ActionParseResult {
    pub actions: Vec<LlmAction>,
    /// 非动作文本（纯回复内容）。
    pub reply_text: Option<String>,
}

// ─── 动作解析器 ───

/// 从 LLM 回复中解析动作。
///
/// 支持三种解析策略：
/// 1. `<action type="...">JSON</action>` XML 标签
/// 2. 从 Markdown 代码块中提取 JSON
/// 3. 全文正则搜索 JSON 动作模式（兜底）
pub fn parse_actions(llm_content: &str) -> ActionParseResult {
    let mut actions = Vec::new();

    // 策略 1: XML <action> 标签
    let action_blocks = extract_xml_action_blocks(llm_content);
    for (action_type, json_str) in &action_blocks {
        if let Some(action) = try_parse_action(action_type, json_str) {
            actions.push(action);
        }
    }

    if !actions.is_empty() {
        return ActionParseResult {
            actions,
            reply_text: strip_action_blocks(llm_content),
        };
    }

    // 策略 2: Markdown JSON 代码块
    let json_blocks = extract_markdown_json(llm_content);
    for block in &json_blocks {
        if let Some(action) = try_parse_action_from_json_value(block) {
            actions.push(action);
        }
    }

    if !actions.is_empty() {
        return ActionParseResult {
            actions,
            reply_text: Some(llm_content.to_string()),
        };
    }

    // 策略 3: 全文正则搜索（兜底）
    let json_patterns = extract_json_by_regex(llm_content);
    for pattern in &json_patterns {
        if let Some(action) = try_parse_action_from_json_value(pattern) {
            actions.push(action);
        }
    }

    // 没有动作 → 整个内容是纯回复
    if actions.is_empty() {
        ActionParseResult {
            actions: vec![LlmAction::Reply {
                content: llm_content.to_string(),
            }],
            reply_text: Some(llm_content.to_string()),
        }
    } else {
        ActionParseResult {
            actions,
            reply_text: strip_action_blocks(llm_content),
        }
    }
}

/// 提取 XML <action> 块。
fn extract_xml_action_blocks(content: &str) -> Vec<(String, String)> {
    let mut results = Vec::new();
    let mut remaining = content;

    loop {
        let start_tag_open = match remaining.find("<action") {
            Some(pos) => pos,
            None => break,
        };

        let type_start = start_tag_open + "<action".len();
        let type_end = match remaining[type_start..].find('>') {
            Some(pos) => type_start + pos,
            None => break,
        };

        let attrs = &remaining[start_tag_open..type_end];

        // 提取 type 属性
        let action_type = if let Some(t_start) = attrs.find("type=\"") {
            let val_start = t_start + "type=\"".len();
            let val_end = attrs[val_start..].find('"').map(|p| val_start + p).unwrap_or(attrs.len());
            attrs[val_start..val_end].to_string()
        } else {
            "reply".to_string()
        };

        let content_start = type_end + 1;
        let close_tag = format!("</action>");
        let content_end = match remaining[content_start..].find(&close_tag) {
            Some(pos) => content_start + pos,
            None => break,
        };

        let inner = &remaining[content_start..content_end];
        results.push((action_type, inner.trim().to_string()));

        // 继续搜索
        let next_start = content_end + close_tag.len();
        if next_start >= remaining.len() {
            break;
        }
        remaining = &remaining[next_start..];
    }

    results
}

/// 从 JSON Value 尝试解析动作。
fn try_parse_action_from_json_value(value: &serde_json::Value) -> Option<LlmAction> {
    let obj = value.as_object()?;

    // 检查是否包含 operation 字段（自演进提案）
    if let Some(op) = obj.get("operation").and_then(|v| v.as_str()) {
        if matches!(op, "add" | "update" | "remove") {
            return Some(LlmAction::Register {
                proposal: value.clone(),
            });
        }
    }

    // 检查是否包含 target 或 capability 字段（调用请求）
    let has_target = obj.contains_key("target");
    let has_capability = obj.contains_key("capability");
    let has_payload = obj.contains_key("payload");

    if (has_target || has_capability) && has_payload {
        return Some(LlmAction::Call {
            target: obj.get("target").and_then(|v| v.as_str()).map(|s| s.to_string()),
            capability: obj.get("capability").and_then(|v| v.as_str()).map(|s| s.to_string()),
            payload: obj.get("payload").cloned().unwrap_or(serde_json::Value::Null),
        });
    }

    None
}

/// 尝试解析 XML action 块中的 JSON。
fn try_parse_action(action_type: &str, json_str: &str) -> Option<LlmAction> {
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;

    match action_type {
        "call" => {
            Some(LlmAction::Call {
                target: value.get("target").and_then(|v| v.as_str()).map(|s| s.to_string()),
                capability: value.get("capability").and_then(|v| v.as_str()).map(|s| s.to_string()),
                payload: value.get("payload").cloned().unwrap_or(value),
            })
        }
        "register" | "proposal" => {
            Some(LlmAction::Register {
                proposal: value,
            })
        }
        "reply" => {
            let text = value.get("content").and_then(|v| v.as_str()).unwrap_or(json_str);
            Some(LlmAction::Reply {
                content: text.to_string(),
            })
        }
        _ => None,
    }
}

/// 提取 Markdown JSON 代码块。
fn extract_markdown_json(content: &str) -> Vec<serde_json::Value> {
    let mut results = Vec::new();
    let mut remaining = content;

    loop {
        let start = match remaining.find("```json") {
            Some(pos) => pos + "```json".len(),
            None => break,
        };

        let end = match remaining[start..].find("```") {
            Some(pos) => start + pos,
            None => break,
        };

        let json_str = remaining[start..end].trim();
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(json_str) {
            results.push(value);
        }

        let next_start = end + "```".len();
        if next_start >= remaining.len() {
            break;
        }
        remaining = &remaining[next_start..];
    }

    results
}

/// 提取 JSON 动作模式（正则兜底）。
fn extract_json_by_regex(content: &str) -> Vec<serde_json::Value> {
    let mut results = Vec::new();

    // 逐字符扫描寻找 `{"operation":` 或 `{"target":` 或 `{"capability":`
    let patterns = [
        r#"{"operation":"#,
        r#"{"target":"#,
        r#"{"capability":"#,
    ];

    for pattern in &patterns {
        let mut search_from = 0;
        while let Some(start) = content[search_from..].find(pattern) {
            let abs_start = search_from + start;
            // 从 abs_start 开始找匹配的 }
            let mut depth = 0;
            let mut in_string = false;
            let mut end_pos = content.len();

            for (i, ch) in content[abs_start..].char_indices() {
                if ch == '"' && (i == 0 || content.as_bytes()[abs_start + i - 1] != b'\\') {
                    in_string = !in_string;
                }
                if !in_string {
                    match ch {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                end_pos = abs_start + i + 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }

            let json_str = &content[abs_start..end_pos];
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(json_str) {
                results.push(value);
            }

            search_from = abs_start + 1;
        }
    }

    results
}

/// 移除内容中的 action 块，返回纯文本部分。
fn strip_action_blocks(content: &str) -> Option<String> {
    let mut result = content.to_string();

    // 移除 <action>...</action>
    loop {
        let start = result.find("<action")?;
        let close_start = result[start..].find('>')?;
        let content_start = start + close_start + 1;
        let end = result[content_start..].find("</action>")? + content_start + "</action>".len();
        result = format!("{}{}", &result[..start], &result[end..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_xml_action_call() {
        let content = r#"我需要调用文件节点。
<action type="call">
{"target": "file_rw", "payload": {"action": "read", "path": "/tmp/test.txt"}}
</action>
"#;
        let result = parse_actions(content);
        assert!(!result.actions.is_empty());
        match &result.actions[0] {
            LlmAction::Call { target, payload, .. } => {
                assert_eq!(target.as_deref(), Some("file_rw"));
                assert_eq!(payload["action"], "read");
            }
            _ => panic!("预期 Call 动作"),
        }
    }

    #[test]
    fn test_parse_xml_action_register() {
        let content = r#"我需要一个新节点。
<action type="register">
{"operation": "add", "node": {"id": "test_node", "description": "test"}}
</action>
"#;
        let result = parse_actions(content);
        assert!(!result.actions.is_empty());
        match &result.actions[0] {
            LlmAction::Register { proposal } => {
                assert_eq!(proposal["operation"], "add");
            }
            _ => panic!("预期 Register 动作"),
        }
    }

    #[test]
    fn test_parse_no_action_pure_reply() {
        let content = "你好，我是助手。有什么可以帮助你的？";
        let result = parse_actions(content);
        assert_eq!(result.actions.len(), 1);
        match &result.actions[0] {
            LlmAction::Reply { content: text } => {
                assert_eq!(text, "你好，我是助手。有什么可以帮助你的？");
            }
            _ => panic!("预期 Reply 动作"),
        }
    }

    #[test]
    fn test_extract_markdown_json() {
        let content = r#"以下是一个调用请求：
```json
{"target": "shell_exec", "payload": {"command": "ls -la"}}
```
"#;
        let blocks = extract_markdown_json(content);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["target"], "shell_exec");
    }

    #[test]
    fn test_llm_config_from_env() {
        // 设置环境变量（仅测试路径，不依赖真实 key）
        unsafe {
            std::env::set_var("SEA_API_KEY", "test-key-123");
            std::env::set_var("SEA_MODEL", "gpt-4o-mini");
        }
        let config = LlmConfig::from_env().expect("from_env failed");
        assert_eq!(config.model, "gpt-4o-mini");
        assert_eq!(config.api_key, "test-key-123");
        unsafe {
            std::env::remove_var("SEA_API_KEY");
            std::env::remove_var("SEA_MODEL");
        }
    }

    #[test]
    fn test_conversation_entry_conversion() {
        let entry = ConversationEntry::user("你好", "trace-001");
        let msg = entry.to_llm_message();
        assert_eq!(msg.role, "user");
        assert_eq!(msg.content, "你好");
    }

    #[test]
    fn test_parse_reply_action() {
        let xml = r#"<action type="reply">{"content": "已为你完成操作"}</action>"#;
        let blocks = extract_xml_action_blocks(xml);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].0, "reply");
    }
}
