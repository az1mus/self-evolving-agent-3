use std::sync::Arc;
use tokio::sync::mpsc;

use sea_common::{LlmConfig, Message, SeaError, SeaResult};

use crate::data_plane::DataPlane;
use crate::registry::Registry;

/// LlmRouter — 智能路由引擎。
///
/// 接收 agent-core 发起的调用请求，按以下优先级决策：
/// 1. `target` 字段 → 精确匹配（快速路径，无需 LLM）
/// 2. LLM 可用 → 用 LLM 分析请求和可用节点，智能决策
/// 3. `capability` 字段 → 静态模糊匹配（降级路径）
///
/// 使用 `send_to_target` 精确投递，避免旧版 router:out 广播 bug。
pub struct Router {
    registry: Arc<Registry>,
    data_plane: Arc<DataPlane>,
    /// 可选 LLM 配置，有则启用智能路由
    llm_config: Option<LlmConfig>,
    /// HTTP 客户端（LLM API 调用用）
    client: Option<reqwest::Client>,
    /// 输入通道接收端（agent-core:call → router:in）。
    rx: Option<mpsc::Receiver<Message>>,
}

impl Router {
    pub fn new(
        registry: Arc<Registry>,
        data_plane: Arc<DataPlane>,
        llm_config: Option<LlmConfig>,
    ) -> Self {
        let client = llm_config.as_ref().map(|cfg| {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(cfg.timeout_secs))
                .build()
                .expect("创建 HTTP 客户端失败")
        });

        Self {
            registry,
            data_plane,
            llm_config,
            client,
            rx: None,
        }
    }

    /// 绑定输入通道。
    pub fn bind_rx(&mut self, rx: mpsc::Receiver<Message>) {
        self.rx = Some(rx);
    }

    /// 启动 Router 主循环。
    pub async fn run(&mut self) {
        let mut rx = self.rx.take().expect("Router: 未绑定输入通道");

        let model = self
            .llm_config
            .as_ref()
            .map(|c| c.model.as_str())
            .unwrap_or("N/A");
        tracing::info!(
            "[router] 智能路由引擎已启动 (model={}, llm={})",
            model,
            if self.llm_config.is_some() { "enabled" } else { "disabled" }
        );

        while let Some(message) = rx.recv().await {
            if let Err(e) = self.dispatch(message).await {
                tracing::warn!("[router] 路由失败: {e}");
            }
        }

        tracing::info!("[router] 智能路由引擎已停止");
    }

    /// 调度逻辑。
    async fn dispatch(&self, message: Message) -> SeaResult<()> {
        let payload = message.content_as_json()?;

        // ── 优先级 1: 精确 target（快速路径，无需 LLM） ──
        if let Some(target_id) = payload.get("target").and_then(|v| v.as_str()) {
            let _info = self
                .registry
                .query_by_id(target_id)
                .await
                .map_err(|_| SeaError::NoRoute(format!("目标节点不存在: {target_id}")))?;

            tracing::info!(
                "[router] 精确路由: → {target_id} (trace={})",
                message.trace_id
            );
            return self
                .route_to(target_id, &payload, &message)
                .await;
        }

        // ── 优先级 2: LLM 智能路由 ──
        if self.llm_config.is_some() && self.client.is_some() {
            let target_id = self.llm_decide_target(&payload).await?;

            tracing::info!(
                "[router] LLM 决策路由: → {target_id} (trace={})",
                message.trace_id
            );
            return self
                .route_to(&target_id, &payload, &message)
                .await;
        }

        // ── 优先级 3: capability 静态匹配（降级路径） ──
        if let Some(capability) = payload.get("capability").and_then(|v| v.as_str()) {
            let matches = self.registry.query_by_capability(capability).await?;

            if matches.is_empty() {
                return Err(SeaError::NoRoute(format!(
                    "没有节点能处理能力: {capability}"
                )));
            }

            if let Some(best) = matches.first() {
                tracing::info!(
                    "[router] 能力匹配路由: → {} (capability={}, trace={})",
                    best.id,
                    capability,
                    message.trace_id
                );
                return self
                    .route_to(&best.id, &payload, &message)
                    .await;
            }
        }

        Err(SeaError::NoRoute(
            "调用请求必须包含 target、capability 或可由 LLM 解析的 payload".to_string(),
        ))
    }

    /// 使用 LLM 分析请求并决策路由目标。
    async fn llm_decide_target(&self, payload: &serde_json::Value) -> SeaResult<String> {
        let config = self
            .llm_config
            .as_ref()
            .ok_or_else(|| SeaError::Internal("LLM 未配置".into()))?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| SeaError::Internal("HTTP 客户端未初始化".into()))?;

        // 获取 Registry 中的可用节点列表
        let registry_summary = self.registry.generate_summary().await;

        let system_prompt = format!(
            r#"你是 SEA CLI 的智能路由决策引擎。你的职责是根据用户的调用意图和可用节点列表，选择最合适的节点来处理请求。

## 可用节点
{registry_summary}

## 决策规则
1. 分析调用请求的意图（action、参数、语义）
2. 从可用节点中选择最能满足需求的节点
3. 输出格式为纯 JSON：{{"target": "节点id"}}
4. **只输出 JSON，不要任何其他文字、解释或 Markdown 标记**"#
        );

        let user_prompt = format!(
            "## 调用请求\n{}",
            serde_json::to_string_pretty(payload).unwrap_or_default()
        );

        let request_body = serde_json::json!({
            "model": config.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.3,
            "max_tokens": 256,
        });

        tracing::debug!(
            "[router] >>> LLM 路由决策 | payload_len={}",
            serde_json::to_string(&request_body).map(|s| s.len()).unwrap_or(0)
        );

        let response = client
            .post(&config.api_url)
            .header("Authorization", format!("Bearer {}", config.api_key))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| SeaError::Internal(format!("LLM 路由请求失败: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "无响应体".to_string());
            return Err(SeaError::Internal(format!(
                "LLM 路由 API 返回错误 (status={status}): {error_text}"
            )));
        }

        let llm_response: sea_common::llm::LlmResponse = response
            .json()
            .await
            .map_err(|e| SeaError::Internal(format!("LLM 路由响应解析失败: {e}")))?;

        let content = llm_response
            .choices
            .first()
            .map(|c| c.message.content.clone())
            .unwrap_or_default();

        tracing::debug!(
            "[router] <<< LLM 路由决策 | content={}",
            content.chars().take(200).collect::<String>()
        );

        // 从响应中提取 JSON
        let json_start = content.find('{');
        let json_end = content.rfind('}');
        let json_str = match (json_start, json_end) {
            (Some(start), Some(end)) if start < end => &content[start..=end],
            _ => {
                return Err(SeaError::Internal(format!(
                    "LLM 输出未包含有效 JSON: {content}"
                )));
            }
        };

        let decision: serde_json::Value = serde_json::from_str(json_str).map_err(|e| {
            SeaError::Internal(format!("LLM 输出 JSON 解析失败: {e} (raw={json_str})"))
        })?;

        decision
            .get("target")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| SeaError::Internal("LLM 决策未包含 target 字段".into()))
    }

    /// 路由到目标节点：直接通过 handler 精确投递。
    async fn route_to(
        &self,
        target_id: &str,
        payload: &serde_json::Value,
        original: &Message,
    ) -> SeaResult<()> {
        // 构造发送给目标节点的消息
        // 透传 payload，不包装额外的 target 字段
        let inner_payload = payload
            .get("payload")
            .cloned()
            .unwrap_or_else(|| payload.clone());

        let route_msg = Message::with_json(original.trace_id.clone(), &inner_payload)
            .reply_to(&original.message_id);

        let target_port = format!("{target_id}:in");

        // 直接投递到目标端口的 handler，不经过 router:out 通道拓扑
        self.data_plane
            .send_to_target(&target_port, route_msg)
            .await
            .map_err(|e| {
                SeaError::NoRoute(format!("投递到 {target_port} 失败: {e}"))
            })?;

        tracing::info!(
            "[router] 已投递 → {target_port} (trace={})",
            original.trace_id
        );

        Ok(())
    }
}
