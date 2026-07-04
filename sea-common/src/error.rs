use thiserror::Error;

/// 顶层错误类型，涵盖所有 SEA CLI 操作中可能出现的错误。
#[derive(Error, Debug, Clone, PartialEq)]
pub enum SeaError {
    // ─── 通用 ───
    #[error("内部错误: {0}")]
    Internal(String),

    #[error("非法参数: {0}")]
    InvalidArg(String),

    #[error("超时: {0}")]
    Timeout(String),

    #[error("操作不被支持: {0}")]
    Unsupported(String),

    // ─── 节点/服务生命周期 ───
    #[error("节点已存在: {0}")]
    NodeAlreadyExists(String),

    #[error("节点不存在: {0}")]
    NodeNotFound(String),

    #[error("节点未停止, 不能 reap: {0}")]
    NodeNotStopped(String),

    #[error("节点总数超限 (max={0})")]
    MaxNodesExceeded(u32),

    #[error("LLM 节点已达上限 (max={0})")]
    MaxLlmNodesExceeded(u32),

    #[error("Python 节点已达上限 (max={0})")]
    MaxPythonNodesExceeded(u32),

    #[error("节点启动失败: {0}")]
    SpawnFailed(String),

    #[error("初始化超时: {0}")]
    InitTimeout(String),

    // ─── 通道 ───
    #[error("通道已存在: {0}")]
    ChannelAlreadyExists(String),

    #[error("通道不存在: {0}")]
    ChannelNotFound(String),

    #[error("通道缓冲已满: {0}")]
    ChannelFull(String),

    #[error("通道已断开: {0}")]
    ChannelBroken(String),

    #[error("通道冲突: {0}")]
    ChannelConflict(String),

    // ─── 消息 ───
    #[error("消息路由失败: {0}")]
    NoRoute(String),

    #[error("消息 TTL 过期")]
    TtlExpired,

    #[error("超过子请求最大嵌套深度 (max={0})")]
    MaxDepthExceeded(u32),

    // ─── 权限与安全 ───
    #[error("权限不足: ACCESS_DENIED")]
    AccessDenied,

    #[error("未持有该权限: {0}")]
    PrivNotHeld(String),

    #[error("非法权限名称: {0}")]
    InvalidPriv(String),

    #[error("身份验证失败: {0}")]
    AuthFailed(String),

    #[error("主体未找到: {0}")]
    PrincipalNotFound(String),

    // ─── 配置 ───
    #[error("配置解析错误: {0}")]
    ParseError(String),

    #[error("配置校验错误: {0}")]
    ValidationError(String),

    #[error("服务组已加载: {0}")]
    GroupAlreadyLoaded(String),

    #[error("服务组未找到: {0}")]
    GroupNotFound(String),

    // ─── 动态 spawn ───
    #[error("哈希校验不匹配")]
    HashMismatch,

    #[error("动态节点必须使用 separate_process 隔离模式")]
    IsolationRequired,

    #[error("不允许动态创建 builtin 核心节点")]
    BuiltinNotAllowed,

    // ─── 自演进 ───
    #[error("未知操作类型: {0}")]
    UnknownOperation(String),

    #[error("核心节点不可删除: {0}")]
    ProtectedNode(String),

    #[error("存在节点依赖, 无法删除: {0}")]
    DependencyExists(String),

    #[error("非法提案者: {0}")]
    InvalidProposer(String),

    #[error("人工审批未通过: {0}")]
    HumanRejected(String),

    #[error("端口变更不兼容: {0}")]
    PortConflict(String),

    #[error("prompt 更新仅适用于 LLM 节点")]
    NotLlmNode,
}

/// 结果类型别名，统一使用 `SeaError` 作为错误类型。
pub type SeaResult<T> = Result<T, SeaError>;

// ─── 错误码常量 (用于 JSON 序列化) ───

impl SeaError {
    /// 返回错误码字符串，与 IFP 工程规范附录 C 对齐。
    pub fn code(&self) -> &'static str {
        match self {
            Self::Internal(_) => "INTERNAL_ERROR",
            Self::InvalidArg(_) => "INVALID_ARG",
            Self::Timeout(_) => "TIMEOUT",
            Self::Unsupported(_) => "UNSUPPORTED",
            Self::NodeAlreadyExists(_) => "SERVICE_ALREADY_EXISTS",
            Self::NodeNotFound(_) => "SERVICE_NOT_FOUND",
            Self::NodeNotStopped(_) => "SERVICE_NOT_STOPPED",
            Self::MaxNodesExceeded(_) => "MAX_SERVICES_EXCEEDED",
            Self::MaxLlmNodesExceeded(_) => "MAX_LLM_SERVICES_EXCEEDED",
            Self::MaxPythonNodesExceeded(_) => "MAX_PYTHON_SERVICES_EXCEEDED",
            Self::SpawnFailed(_) => "SPAWN_FAILED",
            Self::InitTimeout(_) => "INIT_TIMEOUT",
            Self::ChannelAlreadyExists(_) => "CHANNEL_ALREADY_EXISTS",
            Self::ChannelNotFound(_) => "CHANNEL_NOT_FOUND",
            Self::ChannelFull(_) => "CHANNEL_FULL",
            Self::ChannelBroken(_) => "CHANNEL_BROKEN",
            Self::ChannelConflict(_) => "CHANNEL_CONFLICT",
            Self::NoRoute(_) => "NO_ROUTE",
            Self::TtlExpired => "TTL_EXPIRED",
            Self::MaxDepthExceeded(_) => "MAX_DEPTH_EXCEEDED",
            Self::AccessDenied => "ACCESS_DENIED",
            Self::PrivNotHeld(_) => "PRIV_NOT_HELD",
            Self::InvalidPriv(_) => "INVALID_PRIV",
            Self::AuthFailed(_) => "AUTH_FAILED",
            Self::PrincipalNotFound(_) => "PRINCIPAL_NOT_FOUND",
            Self::ParseError(_) => "PARSE_ERROR",
            Self::ValidationError(_) => "VALIDATION_ERROR",
            Self::GroupAlreadyLoaded(_) => "GROUP_ALREADY_LOADED",
            Self::GroupNotFound(_) => "GROUP_NOT_FOUND",
            Self::HashMismatch => "HASH_MISMATCH",
            Self::IsolationRequired => "ISOLATION_REQUIRED",
            Self::BuiltinNotAllowed => "BUILTIN_NOT_ALLOWED",
            Self::UnknownOperation(_) => "UNKNOWN_OPERATION",
            Self::ProtectedNode(_) => "PROTECTED_NODE",
            Self::DependencyExists(_) => "DEPENDENCY_EXISTS",
            Self::InvalidProposer(_) => "INVALID_PROPOSER",
            Self::HumanRejected(_) => "HUMAN_REJECTED",
            Self::PortConflict(_) => "PORT_CONFLICT",
            Self::NotLlmNode => "NOT_LLM_NODE",
        }
    }
}
