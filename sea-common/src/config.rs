use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::channel::ChannelTopology;
use crate::error::{SeaError, SeaResult};
use crate::node::{NodeDef, PortDecl, RuntimeDef, RuntimeKind, IsolationMode};

/// `.group.toml` 的根结构。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupConfig {
    #[serde(default)]
    pub group: GroupMeta,

    /// 节点定义: `node.<id>`。
    #[serde(default)]
    pub node: HashMap<String, NodeTomlDef>,

    /// 通道拓扑。
    #[serde(default)]
    pub channels: HashMap<String, Vec<String>>,
}

/// 组元信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMeta {
    #[serde(default)]
    pub name: String,

    #[serde(default = "default_version")]
    pub version: String,

    #[serde(default = "default_max_sub_depth")]
    pub max_sub_request_depth: u32,
}

fn default_version() -> String {
    "1.0".to_string()
}

const fn default_max_sub_depth() -> u32 {
    3
}

impl Default for GroupMeta {
    fn default() -> Self {
        Self {
            name: String::new(),
            version: default_version(),
            max_sub_request_depth: default_max_sub_depth(),
        }
    }
}

/// TOML 中的节点定义 (扁平结构，解析后转为 NodeDef)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTomlDef {
    #[serde(default)]
    pub description: String,

    /// 多行 API 文档 (Markdown)，描述该节点支持的动作、参数和返回值。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_docs: Option<String>,

    #[serde(default)]
    pub inputs: Vec<PortDecl>,

    #[serde(default)]
    pub outputs: Vec<PortDecl>,

    /// 内联的 `[node.X.runtime]` 段。
    #[serde(default)]
    pub runtime: Option<NodeRuntimeToml>,

    /// 通道声明 (可选，用于自演进场景)。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<HashMap<String, Vec<String>>>,

    /// 初始权限集。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privileges: Option<Vec<String>>,
}

/// TOML 中的运行时定义。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRuntimeToml {
    pub kind: String,

    // LLM
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,

    // Python
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_hash: Option<String>,

    // 隔离
    #[serde(default)]
    pub isolation: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_runtime_ms: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_limit_bytes: Option<u64>,

    // Builtin
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
}

impl GroupConfig {
    /// 从 TOML 字符串解析配置。
    pub fn from_toml(input: &str) -> SeaResult<Self> {
        let config: GroupConfig = toml::from_str(input)
            .map_err(|e| SeaError::ParseError(format!("TOML 解析失败: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    /// 校验配置的合法性。
    pub fn validate(&self) -> SeaResult<()> {
        for (id, node_def) in &self.node {
            if id.is_empty() {
                return Err(SeaError::ValidationError("节点 id 不能为空".to_string()));
            }

            if let Some(runtime) = &node_def.runtime {
                match runtime.kind.as_str() {
                    "builtin" | "llm" | "python" | "skill" => {}
                    other => {
                        return Err(SeaError::ValidationError(format!(
                            "未知 runtime.kind: {other} (节点: {id})"
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// 将 TOML 配置转换为内部的 NodeDef 列表。
    pub fn to_node_defs(&self) -> Vec<NodeDef> {
        self.node
            .iter()
            .map(|(id, toml_def)| {
                let runtime = toml_def
                    .runtime
                    .as_ref()
                    .map(|r| self.toml_runtime_to_def(r))
                    .unwrap_or_else(|| RuntimeDef::skill(""));

                let mut node = NodeDef {
                    id: id.clone(),
                    description: toml_def.description.clone(),
                    api_docs: toml_def.api_docs.clone(),
                    inputs: toml_def.inputs.clone(),
                    outputs: toml_def.outputs.clone(),
                    runtime,
                    channels: toml_def.channels.clone(),
                    privileges: toml_def.privileges.clone(),
                };

                // 如果节点没有声明 inputs，自动添加默认 in 端口
                if node.inputs.is_empty() && node.runtime.kind != RuntimeKind::Skill {
                    node.inputs.push(PortDecl::json("in"));
                }
                // 如果节点没有声明 outputs，自动添加默认 out 端口
                if node.outputs.is_empty() {
                    node.outputs.push(PortDecl::json("out"));
                }

                node
            })
            .collect()
    }

    /// 将 TOML 配置转换为 ChannelTopology。
    pub fn to_channel_topology(&self) -> ChannelTopology {
        let mut topology = ChannelTopology::new();
        for (source, targets) in &self.channels {
            for target in targets {
                topology.add_channel(source, target);
            }
        }
        topology
    }

    fn toml_runtime_to_def(&self, r: &NodeRuntimeToml) -> RuntimeDef {
        let kind = match r.kind.as_str() {
            "builtin" => RuntimeKind::Builtin,
            "llm" => RuntimeKind::Llm,
            "python" => RuntimeKind::Python,
            "skill" => RuntimeKind::Skill,
            _ => RuntimeKind::Skill,
        };

        let isolation = match r.isolation.as_deref() {
            Some("separate_process") => IsolationMode::SeparateProcess,
            _ => IsolationMode::InProcess,
        };

        RuntimeDef {
            kind,
            model: r.model.clone(),
            system_prompt: r.system_prompt.clone(),
            temperature: r.temperature,
            max_tokens: r.max_tokens,
            command: r.command.clone(),
            args: r.args.clone(),
            code: r.code.clone(),
            code_hash: r.code_hash.clone(),
            isolation,
            max_runtime_ms: r.max_runtime_ms,
            output_limit_bytes: r.output_limit_bytes,
            function: r.function.clone(),
        }
    }
}

/// 自演进提案格式。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proposal {
    pub proposal_id: String,
    pub proposed_by: String,
    pub operation: ProposalOperation,

    // add 操作
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<NodeDef>,

    // update 操作
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changes: Option<ProposalChanges>,

    // remove 操作
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// 提案操作类型。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProposalOperation {
    #[serde(rename = "add")]
    Add,
    #[serde(rename = "update")]
    Update,
    #[serde(rename = "remove")]
    Remove,
}

/// 提案变更内容 (update 操作)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalChanges {
    pub target: String,

    // description 更新
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_description: Option<String>,

    // prompt 更新
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_system_prompt: Option<String>,

    // code 更新
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_code: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_code_hash: Option<String>,

    // ports 更新
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_inputs: Option<Vec<PortDecl>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_outputs: Option<Vec<PortDecl>>,

    // channels 增量更新
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add_channels: Option<HashMap<String, Vec<String>>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove_channels: Option<Vec<(String, String)>>,
}

/// 提案处理结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalResult {
    pub proposal_id: String,
    pub status: ProposalStatus,
    pub node_id: Option<String>,
    pub error: Option<String>,
}

impl ProposalResult {
    pub fn accepted(proposal_id: &str, node_id: &str) -> Self {
        Self {
            proposal_id: proposal_id.to_string(),
            status: ProposalStatus::Accepted,
            node_id: Some(node_id.to_string()),
            error: None,
        }
    }

    pub fn rejected(proposal_id: &str, reason: &str) -> Self {
        Self {
            proposal_id: proposal_id.to_string(),
            status: ProposalStatus::Rejected,
            node_id: None,
            error: Some(reason.to_string()),
        }
    }

    pub fn error(proposal_id: &str, error: &str) -> Self {
        Self {
            proposal_id: proposal_id.to_string(),
            status: ProposalStatus::Error,
            node_id: None,
            error: Some(error.to_string()),
        }
    }
}

/// 提案状态。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProposalStatus {
    #[serde(rename = "accepted")]
    Accepted,
    #[serde(rename = "rejected")]
    Rejected,
    #[serde(rename = "error")]
    Error,
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            group: GroupMeta::default(),
            node: HashMap::new(),
            channels: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_group_toml() {
        let toml_str = r#"
[group]
name = "self-evolving"
version = "1.0"

[node.ui]
description = "终端交互"
inputs  = [{ port = "in",  format = "text/markdown" }]
outputs = [{ port = "out", format = "text/plain" }]
[node.ui.runtime]
kind = "builtin"
function = "ui_run"

[node.agent-core]
description = "核心推理引擎"
inputs  = [
    { port = "in",     format = "text/plain" },
    { port = "result", format = "application/json" },
]
outputs = [
    { port = "out",      format = "text/markdown" },
    { port = "call",     format = "application/json" },
    { port = "register", format = "application/json" },
]
[node.agent-core.runtime]
kind = "llm"
model = "gpt-4"
system_prompt = "你是 SEA CLI 的核心助手"

[channels]
"ui:out"                   = ["agent-core:in"]
"agent-core:out"           = ["ui:in"]
"agent-core:call"          = ["router:in"]
        "#;

        let config = GroupConfig::from_toml(toml_str).unwrap();
        assert_eq!(config.group.name, "self-evolving");
        assert_eq!(config.node.len(), 2);
        assert!(config.node.contains_key("ui"));
        assert!(config.node.contains_key("agent-core"));
        assert_eq!(config.channels.len(), 3);

        let defs = config.to_node_defs();
        assert_eq!(defs.len(), 2);

        let ui = defs.iter().find(|n| n.id == "ui").unwrap();
        assert_eq!(ui.runtime.kind, RuntimeKind::Builtin);
        assert_eq!(
            ui.runtime.function.as_deref(),
            Some("ui_run")
        );

        let agent = defs.iter().find(|n| n.id == "agent-core").unwrap();
        assert_eq!(agent.runtime.kind, RuntimeKind::Llm);
        assert_eq!(agent.inputs.len(), 2);
        assert_eq!(agent.outputs.len(), 3);
    }

    #[test]
    fn test_validate_invalid_kind() {
        let toml_str = r#"
[node.test]
description = "test"
[node.test.runtime]
kind = "invalid_kind"
        "#;

        let result = GroupConfig::from_toml(toml_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_api_docs() {
        let toml_str = r#"
[node.file_rw]
description = "文件读写操作"
api_docs = "支持的操作:\n  read  — 读取文件内容"
inputs  = [{ port = "in",  format = "application/json" }]
outputs = [{ port = "out", format = "application/json" }]
[node.file_rw.runtime]
kind = "python"
        "#;

        let config = GroupConfig::from_toml(toml_str).unwrap();
        let defs = config.to_node_defs();
        let f = defs.iter().find(|n| n.id == "file_rw").unwrap();
        assert!(f.api_docs.is_some());
        assert!(f.api_docs.as_ref().unwrap().contains("read"));
    }

    #[test]
    fn test_api_docs_optional() {
        let toml_str = r#"
[node.test]
description = "test without api_docs"
inputs  = [{ port = "in",  format = "application/json" }]
outputs = [{ port = "out", format = "application/json" }]
[node.test.runtime]
kind = "skill"
        "#;

        let config = GroupConfig::from_toml(toml_str).unwrap();
        let defs = config.to_node_defs();
        let t = defs.iter().find(|n| n.id == "test").unwrap();
        assert!(t.api_docs.is_none());
    }
}
