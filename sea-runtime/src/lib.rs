//! # SEA Runtime
//!
//! SEA CLI 运行时 —— 控制面、数据面、节点生命周期管理。
//!
//! 遵循 IFP v3.1 协议规范，实现 sea_instruct.md 中定义的各子系统。
//!
//! ## 模块概览
//!
//! - [`runtime`] — IFPRuntime 单例 & 启动引导 (`bootstrap_runtime`)
//! - [`control_plane`] — 控制面：生命周期管理、审计收集
//! - [`data_plane`] — 数据面：通道交换机、消息路由
//! - [`node_manager`] — 节点生命周期管理器：spawn/terminate/reap
//! - [`registry`] — 节点目录服务：注册、查询、模糊匹配
//! - [`router`] — 消息调度分发：按 target/capability 路由
//! - [`admin`] — 自演进管理：提案校验、审批、执行
//! - [`bridge`] — Python 子进程桥接：Rust ↔ Python JSON Lines
//! - [`ui`] — 终端交互循环：用户输入、Agent 回复展示
//! - [`reload`] — 增量热更新：ReloadGroup
//! - [`signal_handler`] — OS 信号处理：Ctrl+C / SIGTERM
//! - [`access`] — 访问控制集成：权限检查

#![allow(dead_code)]

pub mod access;
pub mod admin;
pub mod bridge;
pub mod control_plane;
pub mod data_plane;
pub mod llm_node;
pub mod node_manager;
pub mod registry;
pub mod reload;
pub mod router;
pub mod runtime;
pub mod signal_handler;
pub mod ui;

// ─── 常用类型重新导出 ───

pub use runtime::{IFPRuntime, IFPRuntimeRef};
pub use runtime::bootstrap_runtime;
pub use data_plane::DataPlane;
pub use control_plane::ControlPlane;
pub use node_manager::NodeManager;

/// 默认内嵌服务组配置（TOML 格式）。
pub const EMBEDDED_GROUP_TOML: &str = "";
