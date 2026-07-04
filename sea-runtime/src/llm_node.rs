use std::sync::Arc;
use tokio::sync::mpsc;

use sea_common::{
    ConversationEntry, LlmAction, LlmConfig, LlmMessage, LlmRequest,
    Message, SeaError, SeaResult, parse_actions,
};

use crate::data_plane::DataPlane;
use crate::registry::Registry;

/// AgentCore — 核心 LLM 推理节点（Supervisor）。
///
/// 职责：
/// - 接收用户消息
/// - 查询 Registry 获取当前节点列表
/// - 构建 system prompt + 对话历史
/// - 调用 LLM API
/// - 解析动作（reply / call / register）
/// - 维护对话上下文
///
/// 对应 sea_instruct.md §12 自演进闭环。
pub struct AgentCore {
    node_id: String,
    data_plane: Arc<DataPlane>,
    registry: Arc<Registry>,

    /// 用户输入接收端（agent-core:in）
    input_rx: Option<mpsc::Receiver<Message>>,

    /// 子节点结果接收端（agent-core:result）
    result_rx: Option<mpsc::Receiver<Message>>,

    /// LLM 配置
    llm_config: LlmConfig,

    /// 对话历史
    history: Vec<ConversationEntry>,

    /// LLM HTTP 客户端
    client: reqwest::Client,

    running: bool,
}

impl AgentCore {
    pub fn new(
        node_id: &str,
        data_plane: Arc<DataPlane>,
        registry: Arc<Registry>,
        llm_config: LlmConfig,
    ) -> Self {
        Self {
            node_id: node_id.to_string(),
            data_plane,
            registry,
            input_rx: None,
            result_rx: None,
            llm_config: llm_config.clone(),
            history: Vec::new(),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(llm_config.timeout_secs))
                .build()
                .expect("创建 HTTP 客户端失败"),
            running: false,
        }
    }

    /// 绑定输入通道接收端。
    pub fn bind_input_rx(&mut self, rx: mpsc::Receiver<Message>) {
        self.input_rx = Some(rx);
    }

    /// 绑定结果通道接收端。
    pub fn bind_result_rx(&mut self, rx: mpsc::Receiver<Message>) {
        self.result_rx = Some(rx);
    }

    /// 启动 AgentCore 主循环。
    pub async fn run(&mut self) {
        let mut input_rx = self.input_rx.take().expect("AgentCore: 未绑定输入通道");
        let mut result_rx = self.result_rx.take().expect("AgentCore: 未绑定结果通道");
        self.running = true;

        tracing::info!("[agent-core] 推理引擎已启动 (model={})", self.llm_config.model);

        loop {
            tokio::select! {
                // 用户消息
                Some(msg) = input_rx.recv() => {
                    if let Err(e) = self.handle_user_message(msg).await {
                        tracing::error!("[agent-core] 处理用户消息失败: {e}");
                    }
                }

                // 子节点结果
                Some(msg) = result_rx.recv() => {
                    self.handle_node_result(msg).await;
                }

                else => {
                    // 所有通道关闭
                    tracing::info!("[agent-core] 输入通道已关闭");
                    break;
                }
            }
        }

        self.running = false;
        tracing::info!("[agent-core] 推理引擎已停止");
    }

    /// 处理用户消息。
    async fn handle_user_message(&mut self, msg: Message) -> SeaResult<()> {
        let text = msg.content_as_text().unwrap_or_default();

        // 追加到对话历史
        self.history.push(ConversationEntry::user(&text, &msg.trace_id));

        // 触发 LLM 推理
        let result = self.invoke_llm().await;

        match result {
            Ok(response_text) => {
                // 解析动作
                let parsed = parse_actions(&response_text);

                for action in &parsed.actions {
                    match action {
                        LlmAction::Reply { content } => {
                            // 发送回复到 UI
                            let reply = Message::with_markdown(
                                msg.trace_id.clone(),
                                content,
                            )
                            .reply_to(&msg.message_id);

                            self.data_plane
                                .send("agent-core:out", reply)
                                .await?;

                            // 追加 assistant 到历史
                            self.history
                                .push(ConversationEntry::assistant(content));
                        }

                        LlmAction::Call {
                            target,
                            capability,
                            payload,
                        } => {
                            // 构造调用请求 → router
                            let mut call_payload = serde_json::json!({
                                "payload": payload,
                            });
                            if let Some(t) = target {
                                call_payload["target"] = serde_json::json!(t);
                            }
                            if let Some(c) = capability {
                                call_payload["capability"] = serde_json::json!(c);
                            }

                            let call_msg = Message::with_json(
                                msg.trace_id.clone(),
                                &call_payload,
                            );

                            self.data_plane
                                .send("agent-core:call", call_msg)
                                .await?;

                            // 在历史中记录"已发出调用"
                            let target_desc = target
                                .clone()
                                .or_else(|| capability.clone())
                                .unwrap_or_else(|| "未知节点".to_string());
                            self.history.push(ConversationEntry::system(&format!(
                                "【调用子节点】已向 {target_desc} 发出调用请求，等待结果..."
                            )));
                        }

                        LlmAction::Register { proposal } => {
                            // 自演进提案 → admin
                            let register_msg = Message::with_json(
                                msg.trace_id.clone(),
                                proposal,
                            );

                            self.data_plane
                                .send("agent-core:register", register_msg)
                                .await?;

                            self.history.push(ConversationEntry::system(
                                "【自演进提案】已发出，等待 admin 处理...",
                            ));
                        }
                    }
                }
            }
            Err(e) => {
                let err_text = format!("LLM 调用失败: {e}");
                tracing::error!("[agent-core] {err_text}");

                let error_reply = Message::with_text(msg.trace_id.clone(), &err_text)
                    .reply_to(&msg.message_id);

                self.data_plane
                    .send("agent-core:out", error_reply)
                    .await?;
            }
        }

        // 对话历史窗口管理
        const MAX_HISTORY: usize = 50;
        if self.history.len() > MAX_HISTORY {
            // 保留 system prompt + 最近的 MAX_HISTORY 条
            self.history = trim_history(&self.history, MAX_HISTORY);
        }

        Ok(())
    }

    /// 处理子节点返回的结果。
    async fn handle_node_result(&mut self, msg: Message) {
        let text = msg.content_as_text().unwrap_or_else(|_| {
            msg.content_as_json()
                .map(|v| v.to_string())
                .unwrap_or_default()
        });

        // 将结果注入对话历史（作为 system 消息）
        let result_entry = ConversationEntry::system(&format!("【子节点结果】\n{text}"));

        // 检查是否原本有等待中的调用记录，替换它
        let last_is_pending = self
            .history
            .last()
            .map(|e| e.content.contains("【调用子节点】"))
            .unwrap_or(false);

        if last_is_pending {
            // 替换最后一条 pending 记录为实际结果
            if let Some(entry) = self.history.last_mut() {
                entry.content = format!("【子节点结果】\n{text}");
            }
        } else {
            self.history.push(result_entry);
        }

        // 如果有 pending 的子节点结果，触发 LLM 继续推理（生成最终回复）
        if let Err(e) = self.invoke_llm().await {
            tracing::error!("[agent-core] 结果后推理失败: {e}");
        }
    }

    /// 调用 LLM API（OpenAI 兼容接口）。
    async fn invoke_llm(&self) -> SeaResult<String> {
        // 获取 Registry 摘要
        let registry_summary = self.registry.generate_summary().await;

        // 构建 system prompt
        let system_prompt = build_system_prompt(&registry_summary);

        // 构建消息列表
        let mut messages: Vec<LlmMessage> = Vec::new();

        // 添加历史消息
        for entry in &self.history {
            messages.push(entry.to_llm_message());
        }

        // 如果历史为空，添加一条占位消息
        if messages.is_empty() {
            messages.push(LlmMessage::user("你好"));
        }

        // 构建 LLM 请求
        let request = LlmRequest::new(&self.llm_config.model)
            .with_system(&system_prompt)
            .with_messages(messages);

        // 序列化请求
        let request_body = serde_json::json!({
            "model": request.model,
            "messages": request.messages,
            "system": request.system,
            "temperature": 0.7,
            "max_tokens": 4096,
            "stream": false,
        });

        // 调用 LLM API
        let response = self
            .client
            .post(&self.llm_config.api_url)
            .header("Authorization", format!("Bearer {}", self.llm_config.api_key))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| SeaError::Internal(format!("LLM API 请求失败: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "无响应体".to_string());
            return Err(SeaError::Internal(format!(
                "LLM API 返回错误 (status={status}): {error_text}"
            )));
        }

        let llm_response: sea_common::llm::LlmResponse = response
            .json()
            .await
            .map_err(|e| SeaError::Internal(format!("LLM 响应解析失败: {e}")))?;

        // 提取回复文本
        let content = llm_response
            .choices
            .first()
            .map(|c| c.message.content.clone())
            .unwrap_or_default();

        Ok(content)
    }
}

/// 构建 system prompt 模板（带 Registry 摘要）。
fn build_system_prompt(registry_summary: &str) -> String {
    format!(
        r#"你是 SEA CLI 的核心助手（Supervisor）。你运行在一个节点化架构中。

{}

## 你的能力
1. 直接回复用户消息
2. 调用节点完成子任务（通过发出 JSON 调用请求）
3. 当现有节点无法满足需求时，**提出自演进提案**

## 调用节点
如果需要调用节点，使用 XML 格式：
<action type="call">
{{"target": "节点 id", "payload": {{ ... }}}}
</action>

或者按能力描述匹配：
<action type="call">
{{"capability": "能力描述", "payload": {{ ... }}}}
</action>

## 自演进提案
当出现以下情况时，提出自演进提案：
- 用户请求的能力当前没有任何节点能提供
- 现有节点执行任务效率低、频繁出错
- 用户明确要求"添加一个能做 XXX 的工具"
- 你可以通过组合新节点大幅提升系统能力

提案格式：
<action type="register">
{{"operation": "add", "proposal_id": "prop-xxx", "proposed_by": "agent-core", "node": {{ "id": "...", "description": "...", "inputs": [...], "outputs": [...], "runtime": {{ "kind": "python", "code": "..." }} }}, "reason": "..." }}
</action>

## 安全约束
- 不要尝试修改或删除核心节点（ui, agent-core, router, admin, registry）
- 确保新节点有明确的输入/输出端口声明
- 所有节点调用通过 <action> XML 标签进行

## 输出规范
- 普通回复直接输出文本
- 需要调用节点时使用 <action type="call"> XML 标签
- 需要自演进时使用 <action type="register"> XML 标签"#,
        registry_summary
    )
}

/// 截断对话历史，只保留最近的 N 条。
fn trim_history(history: &[ConversationEntry], max_len: usize) -> Vec<ConversationEntry> {
    if history.len() <= max_len {
        return history.to_vec();
    }

    // 保留最后 max_len 条
    let start = history.len() - max_len;
    history[start..].to_vec()
}

/// 创建默认的 agent-core LLM 配置。
pub fn default_llm_config() -> Option<LlmConfig> {
    match LlmConfig::from_env() {
        Ok(cfg) => Some(cfg),
        Err(e) => {
            tracing::warn!("[agent-core] LLM 配置不可用: {e}");
            None
        }
    }
}
