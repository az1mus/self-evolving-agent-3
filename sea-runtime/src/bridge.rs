//! BridgeTask — Python 子进程桥接（完整实现）
//!
//! 每个 `runtime.kind = "python"` 的节点对应一个 Bridge task。
//! 负责 Rust mpsc channel ↔ Python stdin/stdout JSON Lines 的双向转换。
//!
//! 对应 sea_instruct.md §7。

use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use sea_common::Message;

use crate::data_plane::DataPlane;

/// 桥接指标。
#[derive(Debug, Default)]
pub struct BridgeMetrics {
    pub messages_in: u64,
    pub messages_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub errors: u64,
    pub started_at: chrono::DateTime<chrono::Utc>,
}

/// BridgeTask — Python 子进程桥接。
///
/// 启动后维持 3 个并发循环：
/// - 循环 A: Rust → Python（读取 mpsc → 序列化 JSON Line → 写入 stdin）
/// - 循环 B: Python → Rust（读取 stdout → 反序列化 → 发送到 data_plane）
/// - 循环 C: 监控子进程退出
pub struct BridgeTask {
    node_id: String,
    data_plane: Arc<DataPlane>,

    /// Python 命令（如 "python" 或 "python3"）
    command: String,
    /// 命令参数（如 ["tools/file_rw.py"]）
    args: Vec<String>,
    /// 通道容量
    capacity: usize,
    /// 输出字节上限（0 = 不限制）
    output_limit: usize,
    /// 子进程最大运行时间（毫秒，0 = 不限制）
    max_runtime_ms: u64,
}

impl BridgeTask {
    /// 创建新的 Bridge task。
    pub fn new(node_id: &str, data_plane: &Arc<DataPlane>) -> Self {
        Self {
            node_id: node_id.to_string(),
            data_plane: data_plane.clone(),
            command: "python".to_string(),
            args: vec![],
            capacity: 256,
            output_limit: 10 * 1024 * 1024, // 10 MiB
            max_runtime_ms: 300_000, // 5 分钟
        }
    }

    /// 设置 Python 解释器命令。
    pub fn with_command(mut self, command: &str) -> Self {
        self.command = command.to_string();
        self
    }

    /// 设置命令参数（脚本路径等）。
    pub fn with_args(mut self, args: Vec<String>) -> Self {
        self.args = args;
        self
    }

    /// 设置输出字节上限。
    pub fn with_output_limit(mut self, limit: usize) -> Self {
        self.output_limit = limit;
        self
    }

    /// 设置最大运行时间。
    pub fn with_max_runtime(mut self, ms: u64) -> Self {
        self.max_runtime_ms = ms;
        self
    }

    /// 启动桥接：创建子进程并启动 3 个并发循环。
    ///
    /// 这一步会：
    /// 1. 注册 target_handler 拦截发送到 {node_id}:in 的消息
    /// 2. 创建 Python 子进程
    /// 3. 启动 3 个 tokio task
    pub async fn start(self) {
        let node_id = self.node_id.clone();
        tracing::info!(
            "[bridge:{}] 启动桥接 ({} {:?}) [cwd={}]",
            node_id,
            self.command,
            self.args,
            std::env::current_dir()
                .map(|d| d.display().to_string())
                .unwrap_or_default()
        );

        // ── 1. 注册输入拦截器（并重建通道，因为 handler 在 channel_create 后注册） ──
        let (tx_in, rx_in) = mpsc::channel(self.capacity);
        let target_port = format!("{node_id}:in");
        if let Err(e) = self
            .data_plane
            .register_handler_and_reconnect(&target_port, tx_in)
            .await
        {
            tracing::error!("[bridge:{node_id}] 无法注册输入处理器: {e}");
            return;
        }

        // ── 2. 创建 Python 子进程 ──
        let python_cmd = std::env::var("SEA_PYTHON").unwrap_or_else(|_| self.command.clone());

        // 解析工具脚本路径（相对路径 → 尝试多个查找位置）
        let resolved_args: Vec<String> = self
            .args
            .iter()
            .map(|a| resolve_tool_path(a))
            .collect();

        let mut child = match Command::new(&python_cmd)
            .args(&resolved_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
        {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("[bridge:{node_id}] 创建子进程失败: {e}");
                return;
            }
        };

        let child_pid = child.id().unwrap_or(0);
        tracing::info!("[bridge:{node_id}] 子进程已启动 (pid={child_pid})");

        // 获取 stdin / stdout 句柄
        let stdin = child.stdin.take().expect("stdin piped");
        let stdout = child.stdout.take().expect("stdout piped");

        // ── 共享指标 ──
        let metrics = Arc::new(std::sync::Mutex::new(BridgeMetrics {
            started_at: chrono::Utc::now(),
            ..Default::default()
        }));

        let running = Arc::new(AtomicBool::new(true));

        // ── 3. 循环 A: Rust → Python ──
        {
            let node_id = node_id.clone();
            let running = running.clone();
            let metrics = metrics.clone();
            tokio::spawn(Self::loop_rust_to_python(
                node_id, rx_in, stdin, running, metrics,
            ));
        }

        // ── 4. 循环 B: Python → Rust ──
        {
            let node_id = node_id.clone();
            let dp = self.data_plane.clone();
            let running = running.clone();
            let metrics = metrics.clone();
            let output_limit = self.output_limit;
            tokio::spawn(Self::loop_python_to_rust(
                node_id, stdout, dp, running, metrics, output_limit,
            ));
        }

        // ── 5. 循环 C: 监控子进程 ──
        {
            let node_id = node_id.clone();
            let dp = self.data_plane.clone();
            let running = running.clone();
            let max_runtime = self.max_runtime_ms;
            tokio::spawn(Self::watch_process(
                node_id, child, dp, running, max_runtime,
            ));
        }
    }

    // ── 循环 A: Rust mpsc → Python stdin ─────────────────────────────────

    async fn loop_rust_to_python(
        node_id: String,
        mut input_rx: mpsc::Receiver<Message>,
        mut stdin: tokio::process::ChildStdin,
        running: Arc<AtomicBool>,
        metrics: Arc<std::sync::Mutex<BridgeMetrics>>,
    ) {
        while let Some(message) = input_rx.recv().await {
            if !running.load(Ordering::Relaxed) {
                break;
            }

            // 构造 BridgeMessage
            let trace_id = message.trace_id.clone();
            let in_response_to = message.in_response_to.clone();

            // 提取 payload——优先 JSON，其次文本
            let payload = message
                .content_as_json()
                .unwrap_or_else(|_| serde_json::json!({"text": message.content_as_text().unwrap_or_default()}));

            let bridge_msg = serde_json::json!({
                "trace_id": trace_id,
                "in_response_to": in_response_to,
                "payload": payload,
                "content_type": message.content_type,
            });

            let json_line = format!("{}\n", bridge_msg);
            let json_bytes = json_line.as_bytes();

            // 写入子进程 stdin
            if let Err(e) = stdin.write_all(json_bytes).await {
                tracing::warn!("[bridge:{node_id}] stdin 写入失败: {e}");
                {
                    let mut m = metrics.lock().unwrap();
                    m.errors += 1;
                }
                break;
            }
            if let Err(e) = stdin.flush().await {
                tracing::warn!("[bridge:{node_id}] stdin flush 失败: {e}");
                break;
            }

            {
                let mut m = metrics.lock().unwrap();
                m.messages_in += 1;
                m.bytes_in += json_bytes.len() as u64;
            }

            tracing::info!("[bridge:{node_id}] Rust→Python: trace={trace_id}");
        }

        // stdin 关闭时子进程收到 EOF
        tracing::info!("[bridge:{node_id}] Rust→Python 循环结束");
    }

    // ── 循环 B: Python stdout → Rust mpsc → data_plane ───────────────────

    async fn loop_python_to_rust(
        node_id: String,
        stdout: tokio::process::ChildStdout,
        data_plane: Arc<DataPlane>,
        running: Arc<AtomicBool>,
        metrics: Arc<std::sync::Mutex<BridgeMetrics>>,
        output_limit: usize,
    ) {
        let reader = BufReader::new(stdout);
        let mut lines = reader.lines();
        let mut buffer = String::new();

        loop {
            if !running.load(Ordering::Relaxed) {
                break;
            }

            match lines.next_line().await {
                Ok(Some(line)) => {
                    buffer.push_str(&line);

                    // 安全上限检查
                    if output_limit > 0 && buffer.len() > output_limit {
                        tracing::warn!(
                            "[bridge:{node_id}] 输出超出上限: {} > {output_limit}",
                            buffer.len()
                        );
                        {
                            let mut m = metrics.lock().unwrap();
                            m.errors += 1;
                        }
                        // 发送一条错误通知
                        let err_msg = Message::with_json(
                            format!("bridge:{node_id}"),
                            &serde_json::json!({
                                "status": "error",
                                "error": format!("输出超出上限 {}", output_limit),
                                "node_id": node_id,
                            }),
                        );
                        let _ = data_plane.send(&format!("{node_id}:out"), err_msg).await;
                        buffer.clear();
                        continue;
                    }

                    // 非流式：一行一个完整的 JSON 消息
                    if let Some(bridge_msg) = Self::parse_bridge_message(&node_id, &line) {
                        let trace_id = bridge_msg.trace_id.clone();
                        let _ = data_plane
                            .send(&format!("{node_id}:out"), bridge_msg)
                            .await;

                        tracing::info!(
                            "[bridge:{node_id}] Python→Rust: trace={trace_id}"
                        );

                        {
                            let mut m = metrics.lock().unwrap();
                            m.messages_out += 1;
                            m.bytes_out += line.len() as u64;
                        }
                    }

                    buffer.clear();
                }
                Ok(None) => {
                    // stdout 关闭
                    tracing::info!("[bridge:{node_id}] Python stdout 已关闭");
                    break;
                }
                Err(e) => {
                    tracing::warn!("[bridge:{node_id}] stdout 读取错误: {e}");
                    {
                        let mut m = metrics.lock().unwrap();
                        m.errors += 1;
                    }
                    break;
                }
            }
        }

        tracing::info!("[bridge:{node_id}] Python→Rust 循环结束");
    }

    /// 解析一行 JSON，构造 Message。
    fn parse_bridge_message(node_id: &str, line: &str) -> Option<Message> {
        let line = line.trim();
        if line.is_empty() {
            return None;
        }

        let raw: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("[bridge:{node_id}] JSON 解析失败: {e} (line preview: {})", &line[..line.len().min(100)]);
                return None;
            }
        };

        let trace_id = raw
            .get("trace_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let in_response_to = raw.get("in_response_to").and_then(|v| v.as_str()).map(String::from);

        // payload 优先取 payload 字段，否则整条消息
        let payload = raw.get("payload").cloned().unwrap_or(raw.clone());

        let content_type = raw
            .get("content_type")
            .and_then(|v| v.as_str())
            .unwrap_or("application/json")
            .to_string();

        let mut msg = Message::with_json(trace_id, &payload);
        msg.content_type = content_type;
        if let Some(ref_id) = in_response_to {
            msg = msg.reply_to(&ref_id);
        }

        Some(msg)
    }

    // ── 循环 C: 监控子进程 ───────────────────────────────────────────────

    async fn watch_process(
        node_id: String,
        mut child: Child,
        data_plane: Arc<DataPlane>,
        running: Arc<AtomicBool>,
        max_runtime_ms: u64,
    ) {
        // 超时控制
        if max_runtime_ms > 0 {
            let node_id = node_id.clone();
            let running = running.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(max_runtime_ms)).await;
                if running.load(Ordering::Relaxed) {
                    tracing::warn!("[bridge:{node_id}] 子进程运行超时 (>{max_runtime_ms}ms)");
                    running.store(false, Ordering::Relaxed);
                }
            });
        }

        // 等待子进程退出
        let exit_status = child.wait().await;
        running.store(false, Ordering::Relaxed);

        match exit_status {
            Ok(status) => {
                if status.success() {
                    tracing::info!(
                        "[bridge:{node_id}] 子进程正常退出 (exit_code={})",
                        status.code().unwrap_or(0)
                    );
                } else {
                    tracing::warn!(
                        "[bridge:{node_id}] 子进程异常退出 (exit_code={})",
                        status.code().unwrap_or(-1)
                    );

                    // 发送审计事件
                    let audit_msg = Message::with_json(
                        format!("bridge:{node_id}"),
                        &serde_json::json!({
                            "status": "process_exited",
                            "node_id": node_id,
                            "exit_code": status.code().unwrap_or(-1),
                        }),
                    );
                    let _ = data_plane.send(&format!("{node_id}:out"), audit_msg).await;
                }
            }
            Err(e) => {
                tracing::error!("[bridge:{node_id}] 等待子进程退出失败: {e}");
            }
        }

        // 清理
        if let Err(e) = child.kill().await {
            tracing::debug!("[bridge:{node_id}] kill 子进程（已退出）: {e}");
        }

        // 注销输入处理器
        data_plane
            .unregister_target_handler(&format!("{node_id}:in"))
            .await;

        tracing::info!("[bridge:{node_id}] 桥接已关闭");
    }
}

// ── 路径解析 ───────────────────────────────────────────────────────────────────

/// 解析 Python 工具脚本的绝对路径。
///
/// 按优先级尝试：
/// 1. 如果是绝对路径，直接返回
/// 2. 相对于当前工作目录
/// 3. 相对于可执行文件所在目录的 `../..` (workspace root, 适配 `target/debug/`)
/// 4. 相对于可执行文件所在目录
fn resolve_tool_path(path: &str) -> String {
    use std::path::Path;

    // 绝对路径 → 直接返回
    if Path::new(path).is_absolute() {
        return path.to_string();
    }

    // 1. 相对于 cwd
    let candidate = Path::new(path);
    if candidate.exists() {
        return candidate.to_string_lossy().to_string();
    }

    // 2. 相对于二进制文件的 workspace root (target/debug/../../ = project root)
    if let Ok(exe) = std::env::current_exe() {
        if let Some(exe_dir) = exe.parent() {
            // target/debug/sea.exe → ../../ = project root
            let workspace_root = exe_dir.join("..").join("..");
            let candidate = workspace_root.join(path);
            if candidate.exists() {
                tracing::debug!(
                    "[bridge] 工具路径已解析: {path} → {}",
                    candidate.display()
                );
                return candidate.to_string_lossy().to_string();
            }

            // target/debug/sea.exe → ./ = tools/ 在 exe_dir 里？
            let candidate = exe_dir.join(path);
            if candidate.exists() {
                tracing::debug!(
                    "[bridge] 工具路径已解析 (exe_dir): {path} → {}",
                    candidate.display()
                );
                return candidate.to_string_lossy().to_string();
            }
        }
    }

    // 3. 都找不到，返回原始路径让 Python 报错
    tracing::warn!("[bridge] 工具脚本未找到: {path}（cwd={})", std::env::current_dir().map(|d| d.display().to_string()).unwrap_or_default());
    path.to_string()
}

impl Drop for BridgeTask {
    fn drop(&mut self) {
        tracing::debug!("[bridge:{}] BridgeTask dropped", self.node_id);
    }
}
