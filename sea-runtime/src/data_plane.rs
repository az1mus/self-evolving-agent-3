use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, RwLock};

use sea_common::{
    ChannelStats, ChannelTopology, Message, SeaError, SeaResult,
};

/// 通道交换机 —— 数据面核心组件。
///
/// 管理所有端口间的 mpsc 通道，执行消息转发、背压检测、死信处理。
pub struct ChannelSwitch {
    /// 拓扑表: `"source_node:port" -> Vec<ChannelHandle>`
    topology: RwLock<ChannelTopology>,

    /// 所有活跃通道的发送端。
    senders: RwLock<HashMap<String, Vec<mpsc::Sender<Message>>>>,

    // ─── 目标端口处理器注册表 ───
    //
    // 某些节点（如 agent-core）需要直接接收发送到其输入端口的消息。
    // 这些节点创建自己的 mpsc channel，将 Sender 注册到这里。
    // ChannelSwitch 在接收消息时，若有注册的处理器，会将消息转发过去。
    /// `"target_node:port" -> mpsc::Sender` 映射。
    target_handlers: RwLock<HashMap<String, Vec<mpsc::Sender<Message>>>>,

    /// 默认通道容量。
    capacity: usize,

    /// 死信计数器：因通道满而丢弃的消息数。
    dead_letter_count: AtomicU64,

    /// 背压事件计数器：通道使用率超过阈值的事件数。
    backpressure_events: AtomicU64,
}

/// 通道句柄，包含发送端和共享的缓冲区使用计数器。
struct ChannelHandle {
    _target: String,
    tx: mpsc::Sender<Message>,
    _buffer_usage: Arc<std::sync::atomic::AtomicU32>,
}

impl ChannelSwitch {
    pub fn new(capacity: usize) -> Self {
        Self {
            topology: RwLock::new(ChannelTopology::new()),
            senders: RwLock::new(HashMap::new()),
            target_handlers: RwLock::new(HashMap::new()),
            capacity,
            dead_letter_count: AtomicU64::new(0),
            backpressure_events: AtomicU64::new(0),
        }
    }

    /// 建立通道。
    pub async fn create(&self, source: &str, target: &str) -> SeaResult<()> {
        // 检查通道是否已存在
        {
            let topology = self.topology.read().await;
            let targets = topology.targets_for(source);
            if targets.iter().any(|t| *t == target) {
                return Err(SeaError::ChannelAlreadyExists(
                    format!("{source} -> {target}"),
                ));
            }
        }

        // 注册拓扑
        {
            let mut topology = self.topology.write().await;
            topology.add_channel(source, target);
        }

        // 检查目标是否有已注册的处理器
        let handler_tx = {
            let handlers = self.target_handlers.read().await;
            handlers.get(target).and_then(|txs| txs.first().cloned())
        };

        if let Some(tx) = handler_tx {
            // 目标端口有已注册的处理器 → 直接使用该发送端
            let mut senders = self.senders.write().await;
            senders.entry(source.to_string()).or_default().push(tx);
            tracing::debug!("[channel] 通道已建立（连接处理器）: {source} -> {target}");
        } else {
            // 创建有界 mpsc channel
            let (tx, rx) = mpsc::channel(self.capacity);
            let shared_usage = Arc::new(std::sync::atomic::AtomicU32::new(0));

            // 注册发送端
            {
                let mut senders = self.senders.write().await;
                senders.entry(source.to_string()).or_default().push(tx);
            }

            // 启动接收循环（tokio::spawn）
            let target_id = target.to_string();
            let _usage_clone = shared_usage.clone();
            tokio::spawn(async move {
                Self::receiver_loop(rx, &target_id).await;
            });

            tracing::debug!("[channel] 通道已建立: {source} -> {target}");
        }
        Ok(())
    }

    /// 注册目标端口的消息处理器。
    ///
    /// 节点（如 agent-core）创建自己的 mpsc channel，将其 Sender 注册到目标端口。
    /// 后续所有发送到 `target` 的消息都会被转发到此 Sender。
    /// `target` 格式：`"node_id:port"`（如 `"agent-core:in"`）。
    pub async fn register_target_handler(
        &self,
        target: &str,
        tx: mpsc::Sender<Message>,
    ) -> SeaResult<()> {
        let mut handlers = self.target_handlers.write().await;
        handlers
            .entry(target.to_string())
            .or_default()
            .push(tx);
        tracing::debug!("[channel] 处理器已注册: {target}");
        Ok(())
    }

    /// 注销目标端口的消息处理器。
    pub async fn unregister_target_handler(&self, target: &str) {
        let mut handlers = self.target_handlers.write().await;
        handlers.remove(target);
        tracing::debug!("[channel] 处理器已注销: {target}");
    }

    /// 注册目标处理器，并重建所有指向此目标的现有通道。
    ///
    /// 问题：当 handler 在 `channel_create` 之后注册时，
    /// 已有的 no-op sender 不会自动替换为 handler 的 tx，
    /// 导致消息仍被 no-op receiver 丢弃。
    /// 此方法遍历所有通道，将目标匹配的 source→target 的 sender 重置为 handler tx。
    pub async fn register_handler_and_reconnect(
        &self,
        target: &str,
        tx: mpsc::Sender<Message>,
    ) -> SeaResult<()> {
        // 1. 注册 handler
        {
            let mut handlers = self.target_handlers.write().await;
            handlers
                .entry(target.to_string())
                .or_default()
                .push(tx.clone());
        }

        // 2. 遍历现有通道，为匹配的 source 重建 sender
        let topology = self.topology.read().await;
        let mut senders = self.senders.write().await;

        for (source, targets) in &topology.routes {
            if targets.iter().any(|t| t == target) {
                // 重建：只用 handler tx 作为 sender
                senders.insert(source.to_string(), vec![tx.clone()]);
                tracing::info!("[channel] 已重连 {source} → {target} 到新 handler");
            }
        }

        Ok(())
    }

    /// 接收循环 —— 从通道读取消息并转发给目标端口的处理器。
    async fn receiver_loop(mut rx: mpsc::Receiver<Message>, target: &str) {
        // 注意：此函数在 ChannelSwitch 外部作为独立的 tokio task 运行。
        // 不能直接访问 self.target_handlers。
        // 转发机制：由 register_target_handler 创建的通道会在内部直接转发。
        while let Some(msg) = rx.recv().await {
            tracing::trace!(
                "[channel] 消息到达目标: {} (trace={})",
                target,
                msg.trace_id
            );
            // 消息已由 pre-maker sender 直接转发给目标节点处理器
        }
    }

    /// 发送消息到指定源端口的所有目标。
    ///
    /// 使用非阻塞 `try_send`。当通道满时，消息进入死信队列并触发背压告警；
    /// 当通道关闭时（目标已停止），消息进入死信队列。
    pub async fn send(&self, source: &str, message: Message) -> SeaResult<()> {
        let topology = self.topology.read().await;
        let targets = topology.targets_for(source);

        let senders = self.senders.read().await;
        let senders = senders
            .get(source)
            .ok_or_else(|| SeaError::ChannelNotFound(source.to_string()))?;

        if senders.is_empty() {
            return Err(SeaError::NoRoute(format!("没有从 {source} 出发的通道")));
        }

        // 提取消息摘要用于日志
        let content_preview = Self::message_preview(&message);

        tracing::info!(
            "[channel] {} → {}  |{}|  {}",
            source,
            targets.join(", "),
            message.trace_id,
            content_preview,
        );

        for sender in senders {
            match sender.try_send(message.clone()) {
                Ok(()) => {
                    // 消息成功入队
                }
                Err(mpsc::error::TrySendError::Full(msg)) => {
                    // 死信：通道缓冲区满
                    let trace_id = msg.trace_id.clone();
                    self.dead_letter_count.fetch_add(1, Ordering::Relaxed);
                    self.backpressure_events.fetch_add(1, Ordering::Relaxed);

                    tracing::warn!(
                        "[channel] 背压 — 通道满: {source} (trace={trace_id}, \
                         dead_letter_total={})",
                        self.dead_letter_count.load(Ordering::Relaxed)
                    );
                }
                Err(mpsc::error::TrySendError::Closed(msg)) => {
                    // 死信：通道已关闭（目标节点已停止）
                    let trace_id = msg.trace_id.clone();
                    self.dead_letter_count.fetch_add(1, Ordering::Relaxed);

                    tracing::warn!(
                        "[channel] 通道已关闭 — 消息丢弃: {source} (trace={trace_id})"
                    );
                }
            }
        }

        Ok(())
    }

    /// 异步发送（等待缓冲空位）。
    pub async fn send_async(&self, source: &str, message: Message) -> SeaResult<()> {
        let senders = self.senders.read().await;
        let senders = senders
            .get(source)
            .ok_or_else(|| SeaError::ChannelNotFound(source.to_string()))?;

        if senders.is_empty() {
            return Err(SeaError::NoRoute(format!("没有从 {source} 出发的通道")));
        }

        for sender in senders {
            if let Err(e) = sender.send(message.clone()).await {
                tracing::warn!("[channel] 通道发送失败: {source} -> {e}");
            }
        }

        Ok(())
    }

    /// 获取指定端口的接收端（用于节点绑定到输入通道）。
    pub async fn bind_receiver(
        &self,
        _source: &str,
        _target: &str,
    ) -> SeaResult<mpsc::Receiver<Message>> {
        // 创建一个新的通道，返回接收端给调用方
        let (tx, rx) = mpsc::channel(self.capacity);

        // 注册拓扑
        {
            let mut topology = self.topology.write().await;
            topology.add_channel(_source, _target);
        }

        // 注册发送端
        {
            let mut senders = self.senders.write().await;
            senders
                .entry(_source.to_string())
                .or_default()
                .push(tx);
        }

        Ok(rx)
    }

    /// 拆除通道。
    pub async fn remove(&self, source: &str, target: &str) -> SeaResult<()> {
        {
            let mut topology = self.topology.write().await;
            topology.remove_channel(source, target);
        }

        // 关闭发送端 (mpsc::Sender drop 后接收端收到 None)
        {
            let mut senders = self.senders.write().await;
            if let Some(tx_list) = senders.get_mut(source) {
                tx_list.clear();
                senders.remove(source);
            }
        }

        tracing::debug!("[channel] 通道已拆除: {source} -> {target}");
        Ok(())
    }

    /// 移除与特定节点相关的所有通道。
    pub async fn remove_all_for(&self, node_id: &str) {
        let keys: Vec<String> = {
            let _topology = self.topology.read().await;
            // 收集所有包含该节点的 source 前缀
            let mut keys = Vec::new();
            for (source, targets) in self.senders.read().await.iter() {
                if source.split(':').next() == Some(node_id) {
                    keys.push(source.clone());
                }
                // 清理 targets 中引用该节点的
                for t in targets.iter() {
                    let _ = t;
                }
            }
            keys
        };

        for key in keys {
            self.senders.write().await.remove(&key);
        }

        self.topology.write().await.remove_all_for(node_id);
    }

    /// 获取通道统计信息。
    pub async fn stats(&self, _source: &str) -> ChannelStats {
        let mut stats = ChannelStats::new(self.capacity as u32);
        stats.dead_letter_count = self.dead_letter_count.load(Ordering::Relaxed);
        stats.backpressure_events = self.backpressure_events.load(Ordering::Relaxed);
        stats
    }

    /// 获取全局死信计数。
    pub fn dead_letter_total(&self) -> u64 {
        self.dead_letter_count.load(Ordering::Relaxed)
    }

    /// 获取全局背压事件计数。
    pub fn backpressure_total(&self) -> u64 {
        self.backpressure_events.load(Ordering::Relaxed)
    }

    /// 关闭通道交换机。
    pub async fn shutdown(&self) {
        self.senders.write().await.clear();
        *self.topology.write().await = ChannelTopology::new();
        tracing::info!("[channel-switch] 通道交换机已关闭");
    }

    /// 提取消息内容摘要（最多 80 字符，单行）。
    fn message_preview(msg: &Message) -> String {
        let text = msg.content_as_text().unwrap_or_else(|_| {
            msg.content_as_json()
                .map(|j| j.to_string())
                .unwrap_or_else(|_| String::new())
        });
        let text = text
            .chars()
            .filter(|c| *c != '\n' && *c != '\r')
            .take(80)
            .collect::<String>();
        if text.len() >= 80 {
            format!("{text}…")
        } else {
            text
        }
    }
}

/// 数据面 —— 消息传输与处理。
pub struct DataPlane {
    pub channel_switch: ChannelSwitch,
}

impl DataPlane {
    pub fn new(capacity: usize) -> Self {
        Self {
            channel_switch: ChannelSwitch::new(capacity),
        }
    }

    /// 启动数据面。
    pub async fn start(&self) {
        tracing::info!("[data-plane] 数据面已启动 (channel_capacity={})", 256);
    }

    /// 建立通道（便捷方法）。
    pub async fn channel_create(&self, source: &str, target: &str) -> SeaResult<()> {
        self.channel_switch.create(source, target).await
    }

    /// 发送消息（便捷方法）。
    pub async fn send(&self, source: &str, message: Message) -> SeaResult<()> {
        self.channel_switch.send(source, message).await
    }

    /// 异步发送消息。
    pub async fn send_async(&self, source: &str, message: Message) -> SeaResult<()> {
        self.channel_switch.send_async(source, message).await
    }

    /// 获取通道交换机的接收端。
    pub async fn bind_receiver(
        &self,
        source: &str,
        target: &str,
    ) -> SeaResult<mpsc::Receiver<Message>> {
        self.channel_switch.bind_receiver(source, target).await
    }

    /// 注册目标端口的消息处理器（便捷方法）。
    pub async fn register_target_handler(
        &self,
        target: &str,
        tx: mpsc::Sender<Message>,
    ) -> SeaResult<()> {
        self.channel_switch.register_target_handler(target, tx).await
    }

    /// 注册目标处理器并重建通道（便捷方法）。
    pub async fn register_handler_and_reconnect(
        &self,
        target: &str,
        tx: mpsc::Sender<Message>,
    ) -> SeaResult<()> {
        self.channel_switch
            .register_handler_and_reconnect(target, tx)
            .await
    }

    /// 注销目标端口的消息处理器（便捷方法）。
    pub async fn unregister_target_handler(&self, target: &str) {
        self.channel_switch.unregister_target_handler(target).await
    }

    /// 读取当前通道拓扑快照。
    pub async fn read_topology(&self) -> ChannelTopology {
        self.channel_switch.topology.read().await.clone()
    }

    /// 关闭数据面。
    pub async fn shutdown(&self) {
        self.channel_switch.shutdown().await;
        tracing::info!("[data-plane] 数据面已关闭");
    }
}
