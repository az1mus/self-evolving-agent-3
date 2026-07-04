use sea_common::{AccessChecker, AccessDecision, SeaResult};

/// 访问控制集成 —— 在消息路由前执行权限检查。
pub struct AccessController {
    checker: AccessChecker,
}

impl AccessController {
    pub fn new() -> Self {
        Self {
            checker: AccessChecker::new(),
        }
    }

    /// 检查通道消息发送权限。
    pub fn check_channel_send(
        &self,
        source_node: &str,
        source_port: &str,
        target_node: &str,
        target_port: &str,
    ) -> SeaResult<()> {
        match self.checker.check(source_node, source_port, target_node, target_port) {
            AccessDecision::Permit => Ok(()),
            AccessDecision::Deny(reason) => {
                tracing::warn!(
                    "[access] 权限拒绝: {source_node}:{source_port} -> {target_node}:{target_port}: {reason}"
                );
                Err(sea_common::SeaError::AccessDenied)
            }
        }
    }

    /// 检查节点是否持有指定权限。
    pub fn has_privilege(&self, node_id: &str, privilege: &str) -> bool {
        self.checker.has_privilege(node_id, privilege)
    }

    /// 从通道拓扑加载静态白名单。
    pub fn load_channels(&mut self, topology: &sea_common::ChannelTopology) {
        self.checker.load_channels(topology);
    }
}

impl Default for AccessController {
    fn default() -> Self {
        Self::new()
    }
}
