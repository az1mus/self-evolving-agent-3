//! # SEA Common
//!
//! SEA CLI 共享类型、数据结构与工具库。
//!
//! 遵循 SEA_CORE.md (统一 Node 模型) + IFP_CORE v3.1 协议规范。
//! 提供系统中所有节点间通信、节点定义、权限控制、配置解析等公共类型。
//!
//! ## 模块概览
//!
//! - [`error`] — 统一错误类型体系 (`SeaError` + `SeaResult<T>`)
//! - [`message`] — 核心消息类型 (`Message`, `ParentTrace`)
//! - [`node`] — 节点/服务定义 (`NodeDef`, `RuntimeDef`, `NodeInfo`, `PortDecl`)
//! - [`channel`] — 通道拓扑 (`ChannelTopology`, `ChannelId`, `ChannelStats`)
//! - [`signal`] — 信号枚举与状态机 (`Signal`, `NodeState`, `Backpressure`, `DeadLetter`)
//! - [`identity`] — 身份与权限 (`Identity`, `PrivilegeMask`)
//! - [`audit`] — 审计事件 (`AuditEvent`, `AuditBatch`, `AuditLogger`)
//! - [`access`] — 访问控制 (`AccessChecker`, `AccessDecision`, `WhiteListEntry`)
//! - [`config`] — 配置解析 (`GroupConfig`, `Proposal`, `ProposalResult`)

pub mod access;
pub mod audit;
pub mod channel;
pub mod config;
pub mod error;
pub mod identity;
pub mod llm;
pub mod message;
pub mod node;
pub mod signal;

// ─── 常用类型重新导出 ───

pub use error::{SeaError, SeaResult};
pub use message::Message;
pub use node::{
    IsolationMode, NodeDef, NodeInfo, PortDecl, RuntimeDef, RuntimeKind, MAX_NODES,
    MAX_LLM_NODES, MAX_PYTHON_NODES, PROTECTED_NODES,
};
pub use channel::{ChannelId, ChannelTopology, ChannelStats};
pub use signal::{Backpressure, BackpressureLevel, DeadLetter, DeadLetterReason, NodeState, Signal, SignalChannel};
pub use identity::{Identity, PrivilegeMask, Challenge, Proof, BindMessage};
pub use audit::{AuditEvent, AuditEventType, AuditBatch, AuditAck, AuditLogger};
pub use access::{AccessChecker, AccessDecision, WhiteListEntry};
pub use config::{GroupConfig, GroupMeta, Proposal, ProposalOperation, ProposalChanges, ProposalResult, ProposalStatus};
pub use llm::{
    ActionParseResult, ConversationEntry, LlmAction, LlmConfig, LlmMessage, LlmRequest,
    LlmResponse, parse_actions,
};

/// 库版本。
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// 生成一个新的 trace ID。
pub fn generate_trace_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// 计算 SHA256 哈希 (hex 编码)。
pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_trace_id() {
        let id1 = generate_trace_id();
        let id2 = generate_trace_id();
        assert_ne!(id1, id2);
        assert!(!id1.is_empty());
    }

    #[test]
    fn test_sha256_hex() {
        let hash = sha256_hex(b"hello");
        assert_eq!(hash.len(), 64);
        // known sha256 of "hello"
        assert_eq!(
            hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_message_default() {
        let msg = Message::default();
        assert_eq!(msg.priority, 128);
        assert_eq!(msg.content_type, "application/octet-stream");
        assert!(!msg.message_id.is_empty());
    }

    #[test]
    fn test_message_with_json() {
        let trace_id = generate_trace_id();
        let value = serde_json::json!({"action": "read", "path": "/tmp/test"});
        let msg = Message::with_json(trace_id.clone(), &value);
        assert_eq!(msg.trace_id, trace_id);
        assert_eq!(msg.content_type, "application/json");
        let parsed = msg.content_as_json().unwrap();
        assert_eq!(parsed["action"], "read");
    }

    #[test]
    fn test_sea_error_code() {
        let err = SeaError::NodeNotFound("test".to_string());
        assert_eq!(err.code(), "SERVICE_NOT_FOUND");
        assert_eq!(err.to_string(), "节点不存在: test");

        let err = SeaError::AccessDenied;
        assert_eq!(err.code(), "ACCESS_DENIED");
    }

    #[test]
    fn test_channel_topology() {
        let mut topo = ChannelTopology::new();
        topo.add_channel("ui:out", "agent-core:in");
        topo.add_channel("agent-core:call", "router:in");

        let targets = topo.targets_for("ui:out");
        assert_eq!(targets, vec!["agent-core:in"]);

        assert!(topo.references_node("agent-core"));
        assert!(topo.references_node("ui"));
        assert!(!topo.references_node("nonexistent"));

        topo.remove_all_for("agent-core");
        assert!(!topo.references_node("agent-core"));
    }

    #[test]
    fn test_privilege_mask() {
        let prvs = vec!["channel_send".to_string(), "tool_call".to_string()];
        let mut mask = PrivilegeMask::new(&prvs);
        assert!(mask.has("channel_send"));
        assert!(mask.has("tool_call"));
        assert!(!mask.has("llm_inference"));

        assert!(mask.drop("tool_call"));
        assert!(!mask.has("tool_call"));
        // 不可逆
        assert!(!mask.drop("tool_call"));
    }

    #[test]
    fn test_identity() {
        let id = Identity::new("agent-001")
            .with_attr("role", "worker")
            .with_credential("key", "abc123");
        assert_eq!(id.principal, "agent-001");
        assert_eq!(
            id.attributes.as_ref().unwrap().get("role").unwrap(),
            "worker"
        );
    }

    #[test]
    fn test_node_def_and_info() {
        let node =
            NodeDef::new("file_rw", "文件读写操作", RuntimeDef::python("python", vec!["file_rw.py".to_string()]));
        assert_eq!(node.id, "file_rw");
        assert_eq!(node.runtime.kind, RuntimeKind::Python);
        assert!(node.runtime.is_separate_process());

        let info = NodeInfo::new("file_rw", "文件读写操作");
        assert_eq!(info.id, "file_rw");
    }
}
