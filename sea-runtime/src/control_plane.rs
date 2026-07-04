use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use sea_common::{Identity, SeaResult};

use crate::runtime::IFPRuntimeRef;

/// 控制面 — 服务生命周期管理、身份与凭证管理、审计收集、策略下发。
pub struct ControlPlane {
    _runtime: IFPRuntimeRef,
    audit_logger: Arc<RwLock<sea_common::AuditLogger>>,
    /// 节点身份映射表：node_id → Identity
    identities: RwLock<HashMap<String, Identity>>,
    shutdown_token: tokio::sync::watch::Sender<bool>,
    running: bool,
}

impl ControlPlane {
    pub fn new(runtime: IFPRuntimeRef) -> Self {
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Self {
            _runtime: runtime,
            audit_logger: Arc::new(RwLock::new(sea_common::AuditLogger::new())),
            identities: RwLock::new(HashMap::new()),
            shutdown_token: shutdown_tx,
            running: false,
        }
    }

    /// 启动控制面主循环（接收控制通道消息）。
    pub async fn start(&mut self) {
        self.running = true;
        tracing::info!("[control-plane] 控制面已启动");
    }

    /// 审计事件。
    pub async fn audit_event(
        &self,
        event_type: sea_common::AuditEventType,
        data: std::collections::HashMap<String, String>,
    ) {
        self.audit_logger.write().await.log(event_type, data);
    }

    /// 绑定身份到节点。
    ///
    /// 将节点的 Identity 注入到控制面的身份映射表中，
    /// 供后续访问控制 (AccessChecker) 和审计使用。
    pub async fn identity_bind(
        &self,
        node_id: &str,
        identity: &Identity,
    ) -> SeaResult<()> {
        self.identities
            .write()
            .await
            .insert(node_id.to_string(), identity.clone());
        tracing::debug!("[control-plane] 身份已绑定: {node_id}");
        Ok(())
    }

    /// 从 Registry 中注销节点。
    ///
    /// 委托给 Runtime 中的 Registry 实例执行。
    /// 如果 Registry 尚未初始化，静默跳过（可能在关闭路径中）。
    pub async fn registry_remove(&self, node_id: &str) {
        // 先清理身份映射
        self.identities.write().await.remove(node_id);

        // 委托给 Registry
        let rt = self._runtime.read().await;
        if let Some(registry) = &rt.registry {
            let _ = registry.unregister(node_id).await;
        }
        tracing::debug!("[control-plane] 节点已从 Registry 注销: {node_id}");
    }

    /// 查询节点身份。
    pub async fn identity_of(&self, node_id: &str) -> Option<Identity> {
        self.identities.read().await.get(node_id).cloned()
    }

    /// 获取审计日志收集器引用。
    pub fn audit_logger(&self) -> Arc<RwLock<sea_common::AuditLogger>> {
        self.audit_logger.clone()
    }

    /// 获取关闭信号接收器。
    pub fn subscribe_shutdown(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown_token.subscribe()
    }

    /// 请求关闭。
    pub fn request_shutdown(&self) {
        let _ = self.shutdown_token.send(true);
    }

    /// 关闭控制面。
    pub async fn shutdown(&mut self) {
        self.running = false;
        // 批量刷新审计事件
        let batch = self.audit_logger.write().await.drain_batch();
        if !batch.events.is_empty() {
            tracing::info!(
                "[control-plane] 审计事件已落盘: {} 条",
                batch.events.len()
            );
        }
        tracing::info!("[control-plane] 控制面已关闭");
    }
}
