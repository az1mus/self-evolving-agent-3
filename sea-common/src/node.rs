use serde::{Deserialize, Serialize};

/// 端口声明 —— 描述节点的输入/输出端口。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PortDecl {
    /// 端口名 (如 `in`, `out`, `call`, `register`)。
    pub port: String,

    /// 端口接受的 MIME 类型 (如 `application/json`, `text/plain`)。
    #[serde(default = "default_port_format")]
    pub format: String,
}

fn default_port_format() -> String {
    "application/json".to_string()
}

impl PortDecl {
    pub fn new(port: &str, format: &str) -> Self {
        Self {
            port: port.to_string(),
            format: format.to_string(),
        }
    }

    /// 快速创建 JSON 端口。
    pub fn json(port: &str) -> Self {
        Self {
            port: port.to_string(),
            format: "application/json".to_string(),
        }
    }
}

/// 运行时定义 —— 节点的内部实现细节。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeDef {
    /// 运行时类型。
    pub kind: RuntimeKind,

    // ─── LLM 类型字段 ───
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,

    // ─── Python 类型字段 ───
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_hash: Option<String>,

    // ─── 隔离 & 安全 ───
    #[serde(default)]
    pub isolation: IsolationMode,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_runtime_ms: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_limit_bytes: Option<u64>,

    // ─── Builtin 类型字段 ───
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
}

impl RuntimeDef {
    pub fn builtin(function: &str) -> Self {
        Self {
            kind: RuntimeKind::Builtin,
            model: None,
            system_prompt: None,
            temperature: None,
            max_tokens: None,
            command: None,
            args: None,
            code: None,
            code_hash: None,
            isolation: IsolationMode::InProcess,
            max_runtime_ms: None,
            output_limit_bytes: None,
            function: Some(function.to_string()),
        }
    }

    pub fn llm(model: &str, system_prompt: &str) -> Self {
        Self {
            kind: RuntimeKind::Llm,
            model: Some(model.to_string()),
            system_prompt: Some(system_prompt.to_string()),
            temperature: None,
            max_tokens: None,
            command: None,
            args: None,
            code: None,
            code_hash: None,
            isolation: IsolationMode::InProcess,
            max_runtime_ms: None,
            output_limit_bytes: None,
            function: None,
        }
    }

    pub fn python(command: &str, args: Vec<String>) -> Self {
        Self {
            kind: RuntimeKind::Python,
            model: None,
            system_prompt: None,
            temperature: None,
            max_tokens: None,
            command: Some(command.to_string()),
            args: Some(args),
            code: None,
            code_hash: None,
            isolation: IsolationMode::SeparateProcess,
            max_runtime_ms: None,
            output_limit_bytes: None,
            function: None,
        }
    }

    pub fn skill(prompt: &str) -> Self {
        Self {
            kind: RuntimeKind::Skill,
            model: None,
            system_prompt: Some(prompt.to_string()),
            temperature: None,
            max_tokens: None,
            command: None,
            args: None,
            code: None,
            code_hash: None,
            isolation: IsolationMode::InProcess,
            max_runtime_ms: None,
            output_limit_bytes: None,
            function: None,
        }
    }

    pub fn kind_name(&self) -> &'static str {
        self.kind.as_str()
    }

    pub fn is_separate_process(&self) -> bool {
        self.isolation == IsolationMode::SeparateProcess
    }
}

/// 运行时类型。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RuntimeKind {
    /// Rust 内置逻辑，tokio async task，进程内直接执行。
    #[serde(rename = "builtin")]
    Builtin,

    /// LLM 推理，通过 reqwest 调用 API。
    #[serde(rename = "llm")]
    Llm,

    /// Python 脚本，独立子进程 + stdio JSON Lines。
    #[serde(rename = "python")]
    Python,

    /// 纯 prompt 模板，无运行时。
    #[serde(rename = "skill")]
    Skill,
}

impl RuntimeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Builtin => "builtin",
            Self::Llm => "llm",
            Self::Python => "python",
            Self::Skill => "skill",
        }
    }
}

/// 隔离模式。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IsolationMode {
    /// 进程内，tokio async task。
    #[serde(rename = "in_process")]
    InProcess,

    /// 独立 OS 子进程。
    #[serde(rename = "separate_process")]
    SeparateProcess,
}

impl Default for IsolationMode {
    fn default() -> Self {
        Self::InProcess
    }
}

/// 节点的外部视角信息 (Registry 存储)。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeInfo {
    pub id: String,
    pub description: String,
    pub inputs: Vec<PortDecl>,
    pub outputs: Vec<PortDecl>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl NodeInfo {
    pub fn new(id: &str, description: &str) -> Self {
        Self {
            id: id.to_string(),
            description: description.to_string(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            created_at: chrono::Utc::now(),
        }
    }

    pub fn with_ports(
        id: &str,
        description: &str,
        inputs: Vec<PortDecl>,
        outputs: Vec<PortDecl>,
    ) -> Self {
        Self {
            id: id.to_string(),
            description: description.to_string(),
            inputs,
            outputs,
            created_at: chrono::Utc::now(),
        }
    }
}

/// 完整节点定义 (引擎内部使用，含 runtime 细节)。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeDef {
    pub id: String,

    #[serde(default)]
    pub description: String,

    #[serde(default)]
    pub inputs: Vec<PortDecl>,

    #[serde(default)]
    pub outputs: Vec<PortDecl>,

    pub runtime: RuntimeDef,

    /// 通道拓扑声明: `"source_node:port" -> ["target_node:port", ...]`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<std::collections::HashMap<String, Vec<String>>>,

    /// 初始权限集。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privileges: Option<Vec<String>>,
}

impl NodeDef {
    pub fn new(id: &str, description: &str, runtime: RuntimeDef) -> Self {
        Self {
            id: id.to_string(),
            description: description.to_string(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            runtime,
            channels: None,
            privileges: None,
        }
    }

    pub fn with_ports(
        id: &str,
        description: &str,
        runtime: RuntimeDef,
        inputs: Vec<PortDecl>,
        outputs: Vec<PortDecl>,
    ) -> Self {
        Self {
            id: id.to_string(),
            description: description.to_string(),
            inputs,
            outputs,
            runtime,
            channels: None,
            privileges: None,
        }
    }
}

/// 受保护的核心节点列表 (不可增删)。
pub const PROTECTED_NODES: &[&str] = &["ui", "agent-core", "router", "admin", "registry"];

/// 系统级常量。
pub const MAX_NODES: u32 = 64;
pub const MAX_LLM_NODES: u32 = 10;
pub const MAX_PYTHON_NODES: u32 = 50;
