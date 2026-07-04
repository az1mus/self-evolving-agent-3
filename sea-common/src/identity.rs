use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 身份主体 —— 与节点绑定的安全身份。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Identity {
    /// 主体标识符，全局唯一。
    pub principal: String,

    /// 凭证集合 (token、证书、密钥引用)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<HashMap<String, String>>,

    /// 属性标签 (如 `role=worker`, `tier=backend`)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, String>>,
}

impl Identity {
    pub fn new(principal: &str) -> Self {
        Self {
            principal: principal.to_string(),
            credentials: None,
            attributes: None,
        }
    }

    pub fn with_attr(mut self, key: &str, value: &str) -> Self {
        self.attributes.get_or_insert_default().insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_credential(mut self, key: &str, value: &str) -> Self {
        self.credentials.get_or_insert_default().insert(key.to_string(), value.to_string());
        self
    }
}

/// 权限名称常量。
pub mod privileges {
    /// LLM 推理权限。
    pub const LLM_INFERENCE: &str = "llm_inference";
    /// 通道发送权限。
    pub const CHANNEL_SEND: &str = "channel_send";
    /// 工具调用权限 (LLM 调用子节点)。
    pub const TOOL_CALL: &str = "tool_call";
    /// 工具执行权限 (Python 节点执行)。
    pub const TOOL_EXEC: &str = "tool_exec";
    /// 动态 spawn 权限。
    pub const DYNAMIC_SPAWN: &str = "dynamic_spawn";

    /// 所有合法权限列表。
    pub const ALL: &[&str] = &[LLM_INFERENCE, CHANNEL_SEND, TOOL_CALL, TOOL_EXEC, DYNAMIC_SPAWN];
}

/// 能力掩码 —— 节点的权限集合，支持不可逆降权。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivilegeMask {
    held: HashMap<String, bool>,
}

impl PrivilegeMask {
    /// 从初始权限列表创建能力掩码。
    pub fn new(initial_privileges: &[String]) -> Self {
        let held: HashMap<String, bool> = privileges::ALL
            .iter()
            .map(|p| (p.to_string(), initial_privileges.iter().any(|ip| ip == p)))
            .collect();
        Self { held }
    }

    /// 创建持有所有权限的掩码。
    pub fn all() -> Self {
        let mut held = HashMap::new();
        for p in privileges::ALL {
            held.insert(p.to_string(), true);
        }
        Self { held }
    }

    /// 创建空权限掩码 (默认拒绝)。
    pub fn none() -> Self {
        Self {
            held: HashMap::new(),
        }
    }

    /// 检查是否持有指定权限。
    pub fn has(&self, privilege: &str) -> bool {
        *self.held.get(privilege).unwrap_or(&false)
    }

    /// 不可逆地放弃指定权限。
    pub fn drop(&mut self, privilege: &str) -> bool {
        if let Some(true) = self.held.get(privilege) {
            self.held.insert(privilege.to_string(), false);
            true
        } else {
            false
        }
    }

    /// 获取当前持有的权限列表。
    pub fn held_privileges(&self) -> Vec<String> {
        self.held
            .iter()
            .filter(|(_, held)| **held)
            .map(|(p, _)| p.clone())
            .collect()
    }

    /// 获取掩码的二进制表示 (用于审计)。
    pub fn mask_binary(&self) -> String {
        let mut bits = Vec::new();
        for p in privileges::ALL {
            if self.has(p) {
                bits.push('1');
            } else {
                bits.push('0');
            }
        }
        format!("0b{}", bits.iter().collect::<String>())
    }
}

/// HMAC 挑战-响应认证协议消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Challenge {
    pub nonce: String,
    pub auth_methods: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {
    pub method: String,
    pub principal: String,
    pub proof_data: String,
}

/// Bootstrap 绑定握手消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindMessage {
    pub service_id: String,
    pub control_addr: String,
    pub channels: Vec<ChannelBinding>,
    pub credential: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelBinding {
    pub name: String,
    pub role: String,
    pub addr: String,
}
