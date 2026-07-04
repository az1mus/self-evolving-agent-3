use std::sync::Arc;
use tokio::sync::mpsc;

use sea_common::{Message, SeaError, SeaResult};

use crate::registry::Registry;
use crate::data_plane::DataPlane;

/// Router — 无状态消息调度。
///
/// 接收 agent-core 发起的调用请求，按 target 或 capability 匹配目标节点，
/// 路由消息到目标节点的 in 端口。
pub struct Router {
    registry: Arc<Registry>,
    data_plane: Arc<DataPlane>,
    /// 输入通道接收端（agent-core:call -> router:in）。
    rx: Option<mpsc::Receiver<Message>>,
}

impl Router {
    pub fn new(registry: Arc<Registry>, data_plane: Arc<DataPlane>) -> Self {
        Self {
            registry,
            data_plane,
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

        tracing::info!("[router] 消息调度已启动");

        while let Some(message) = rx.recv().await {
            if let Err(e) = self.dispatch(message).await {
                tracing::warn!("[router] 路由失败: {e}");
            }
        }

        tracing::info!("[router] 消息调度已停止");
    }

    /// 调度逻辑。
    async fn dispatch(&self, message: Message) -> SeaResult<()> {
        // 尝试将消息内容解析为 JSON
        let payload = message.content_as_json()?;

        // 优先级 1: 精确 target
        if let Some(target_id) = payload.get("target").and_then(|v| v.as_str()) {
            let node_info = self.registry.query_by_id(target_id).await;

            match node_info {
                Ok(_info) => {
                    // 路由到目标节点的 in 端口
                    let channel_key = format!("{target_id}:in");
                    self.data_plane
                        .send(&channel_key, message)
                        .await?;
                    return Ok(());
                }
                Err(_) => {
                    return Err(SeaError::NoRoute(format!("目标节点不存在: {target_id}")));
                }
            }
        }

        // 优先级 2: 按能力描述
        if let Some(capability) = payload.get("capability").and_then(|v| v.as_str()) {
            let matches = self.registry.query_by_capability(capability).await?;

            if matches.is_empty() {
                return Err(SeaError::NoRoute(format!(
                    "没有节点能处理能力: {capability}"
                )));
            }

            // 取最匹配（排序后第一个）
            if let Some(best) = matches.first() {
                let channel_key = format!("{}:in", best.id);
                self.data_plane.send(&channel_key, message).await?;
                return Ok(());
            }
        }

        Err(SeaError::NoRoute(
            "调用请求必须包含 target 或 capability 字段".to_string(),
        ))
    }
}
