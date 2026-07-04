//! # SEA CLI — 统一二进制入口
//!
//! 遵循 sea_instruct.md §1 启动流程：
//!
//! 1. 解析 CLI 参数 → 加载 .group.toml
//! 2. 初始化 tracing 日志
//! 3. 初始化 IFP Runtime（控制面 + 数据面 + 通道 + 节点）
//! 4. 启动 TUI 事件循环（§8）
//! 5. TUI 停止 → 优雅关闭 Runtime
//! 6. 退出
//!
//! ## 项目结构（对齐 sea_instruct.md §13）
//!
//! ```text
//! sea/                     # workspace root
//! ├── Cargo.toml           # [workspace] 聚合 4 crate
//! ├── .group.toml          # 默认节点配置 (include_str!)
//! ├── sea-common/          # 共享数据类型
//! ├── sea-runtime/         # IFP Runtime 核心库
//! ├── sea-ui/              # TUI 终端交互
//! └── sea-bin/             # → sea 二进制 (this crate)
//! ```

use clap::Parser;
use sea_common::GroupConfig;
use sea_runtime::{
    bootstrap_runtime, IFPRuntimeRef,
};
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

// ── CLI 参数 ────────────────────────────────────────────────────────────────

/// SEA — 自演进对话式 CLI
#[derive(Parser, Debug)]
#[command(name = "sea", version, about, long_about = None)]
struct Cli {
    /// 自定义 .group.toml 路径（覆盖内置默认配置）
    #[arg(short = 'c', long = "config", value_name = "FILE")]
    config: Option<String>,

    /// 启用 TUI 模式（默认）
    #[arg(long)]
    tui: bool,

    /// 纯 CLI 模式（无 TUI，基本 REPL）
    #[arg(long)]
    cli: bool,

    /// 启用日志输出（默认关闭，仅输出错误）
    /// 开启后日志会写入临时文件并弹出独立终端窗口展示。
    #[arg(short = 'v', long)]
    verbose: bool,
}

// ── 内嵌配置 ────────────────────────────────────────────────────────────────

/// 编译时嵌入的默认 .group.toml。
const EMBEDDED_GROUP_TOML: &str = include_str!("../../.group.toml");

// ── main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 加载 .env 文件
    load_dotenv();

    // 2. 解析 CLI 参数
    let cli = Cli::parse();

    // 3. 初始化日志
    //    - 非 verbose：仅 stderr 输出 error 级别
    //    - verbose：stderr 仍仅 error（保护 TUI alternate screen），
    //      info+ 日志写入临时文件并在独立终端窗口 tail
    let mut verbose_log_path: Option<std::path::PathBuf> = None;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        if cli.verbose {
            EnvFilter::new("info")
        } else {
            EnvFilter::new("error")
        }
    });

    if cli.verbose {
        // ── verbose 模式 ──
        // 创建临时日志文件
        let pid = std::process::id();
        let log_path = std::env::temp_dir().join(format!("sea_logs_{pid}.txt"));
        verbose_log_path = Some(log_path.clone());

        // 预先创建日志文件并写入路径头，避免 Get-Content -Wait 因文件不存在而退出
        // 路径头确保用户在独立终端窗口第一行就能看到文件位置
        {
            use std::io::Write;
            let res = std::fs::File::create(&log_path)
                .and_then(|mut f| writeln!(f, "══ 日志文件: {} ══\n", log_path.display()));
            if let Err(e) = res {
                eprintln!("[sea] 无法创建日志文件 {}: {e}", log_path.display());
            }
        }

        // 文件 writer（每次打开追加，tracing fmt 自带缓冲）
        let file_writer = make_log_file_writer(log_path.clone());

        // 先初始化 tracing，确保日志文件有内容写入后再弹终端
        tracing_subscriber::registry()
            .with(
                // stderr 层：verbose 下仅保留 error 级别，避免 info 日志破坏 TUI alternate screen
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_filter(EnvFilter::new("error")),
            )
            .with(
                // 文件层：带时间戳、完整 format，输出 info 及以上
                tracing_subscriber::fmt::layer()
                    .with_writer(file_writer)
                    .with_ansi(false)
                    .with_filter(filter),
            )
            .init();

        // 日志系统就绪后再弹终端窗口 tail —— 此时文件已存在且有内容
        spawn_log_terminal(&log_path);
    } else {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_filter(filter),
            )
            .init();
    }

    tracing::info!("🌊 SEA CLI v{}", sea_common::VERSION);

    // 3. 加载 .group.toml
    let config = load_config(&cli)?;

    // 4. 初始化 IFP Runtime
    tracing::info!("[sea] 启动 IFP Runtime...");
    let runtime = bootstrap_runtime(config).await?;
    tracing::info!("[sea] IFP Runtime 就绪");

    // 5. 运行主循环（TUI 或 CLI）
    if cli.cli {
        run_cli_mode(runtime.clone()).await?;
    } else {
        run_tui_mode(runtime.clone()).await?;
    }

    // 6. 优雅关闭
    graceful_shutdown(runtime).await?;

    // TUI 已退出，stderr 恢复可见，再次确认日志文件位置
    if let Some(ref p) = verbose_log_path {
        eprintln!("[sea] 日志已保存: {}", p.display());
    }

    tracing::info!("sea: goodbye");
    Ok(())
}

// ── 环境加载 ────────────────────────────────────────────────────────────────

/// 按优先级从多处尝试加载 .env 文件。
fn load_dotenv() {
    use std::path::Path;

    let candidates: Vec<std::path::PathBuf> = {
        let mut v = Vec::new();

        if let Ok(cwd) = std::env::current_dir() {
            v.push(cwd.join(".env"));
        }

        if let Ok(exe) = std::env::current_exe() {
            if let Some(dir) = exe.parent() {
                v.push(dir.join(".env"));
            }
        }

        if let Ok(exe) = std::env::current_exe() {
            if let Some(dir) = exe.parent() {
                v.push(dir.join("..").join("..").join(".env"));
            }
        }

        v
    };

    for path in &candidates {
        if Path::new(path).exists() {
            match dotenvy::from_path(path) {
                Ok(()) => {
                    eprintln!("[sea] 已加载: {}", path.display());
                    return;
                }
                Err(e) => {
                    eprintln!("[sea] .env 加载警告 ({}) : {e}", path.display());
                }
            }
        }
    }
}

// ── 配置加载 ────────────────────────────────────────────────────────────────

fn load_config(cli: &Cli) -> anyhow::Result<GroupConfig> {
    match &cli.config {
        Some(path) => {
            tracing::info!("[sea] 加载自定义配置: {path}");
            let toml_str = std::fs::read_to_string(path)?;
            Ok(GroupConfig::from_toml(&toml_str)?)
        }
        None => {
            tracing::info!("[sea] 使用内置默认配置");
            if !EMBEDDED_GROUP_TOML.is_empty() {
                Ok(GroupConfig::from_toml(EMBEDDED_GROUP_TOML)?)
            } else {
                tracing::warn!("[sea] 未找到内嵌配置，使用默认空配置");
                Ok(GroupConfig::default())
            }
        }
    }
}

// ── TUI 模式 ────────────────────────────────────────────────────────────────

async fn run_tui_mode(
    runtime: IFPRuntimeRef,
) -> anyhow::Result<()> {
    tracing::info!("[sea] 启动 TUI 模式");

    let dp = {
        let rt = runtime.read().await;
        rt.data_plane
            .clone()
            .ok_or_else(|| anyhow::anyhow!("DataPlane 未初始化"))?
    };

    let (tx_ui_out, mut rx_ui_out) = tokio::sync::mpsc::channel::<sea_common::Message>(256);
    let (tx_ui_in, rx_ui_in) = tokio::sync::mpsc::channel::<sea_common::Message>(256);

    // 注册 ui:in 处理器
    if let Err(e) = dp.register_handler_and_reconnect("ui:in", tx_ui_in).await {
        tracing::error!("[sea] 无法注册 ui:in 处理器: {e}");
        return Err(anyhow::anyhow!("ui:in 处理器注册失败: {e}"));
    }

    // 启动转发任务：TUI 输出 → data_plane "ui:out"
    let dp_forward = dp.clone();
    tokio::spawn(async move {
        while let Some(msg) = rx_ui_out.recv().await {
            if let Err(e) = dp_forward.send("ui:out", msg).await {
                tracing::warn!("[sea] TUI → data_plane 转发失败: {e}");
            }
        }
        tracing::info!("[sea] TUI 输出转发已停止");
    });

    // 运行 TUI（不再接收日志通道，verbose 日志走独立终端）
    let tui_result = sea_ui::run_tui_with_agent(tx_ui_out, rx_ui_in).await;

    // 清理
    dp.unregister_target_handler("ui:in").await;
    tracing::info!("[sea] TUI 已退出");

    {
        let rt = runtime.read().await;
        if let Some(cp) = &rt.control_plane {
            cp.request_shutdown();
        }
    }

    tui_result.map_err(|e| anyhow::anyhow!("TUI 运行错误: {e}"))
}

// ── CLI 模式 ────────────────────────────────────────────────────────────────

async fn run_cli_mode(runtime: IFPRuntimeRef) -> anyhow::Result<()> {
    tracing::info!("[sea] 启动 CLI 模式");

    let dp = {
        let rt = runtime.read().await;
        rt.data_plane.clone()
    };

    let _dp = dp.ok_or_else(|| {
        anyhow::anyhow!("DataPlane 未初始化")
    })?;

    println!("🌊 SEA CLI v{} — Type /help for commands", sea_common::VERSION);

    loop {
        print!("sea> ");
        use std::io::Write;
        std::io::stdout().flush()?;

        let mut line = String::new();
        match std::io::stdin().read_line(&mut line) {
            Ok(0) => {
                println!();
                break;
            }
            Ok(_) => {
                let trimmed = line.trim().to_string();
                if trimmed.is_empty() {
                    continue;
                }

                if trimmed == "/exit" || trimmed == "/quit" {
                    break;
                }

                if trimmed == "/help" {
                    println!("可用命令:");
                    println!("  /exit      退出");
                    println!("  /help      显示此帮助");
                    println!("  /clear     清屏");
                    println!("  /nodes     查看可用节点");
                    println!("  /status    查看状态");
                    continue;
                }

                if trimmed == "/clear" {
                    print!("\x1B[2J\x1B[H");
                    std::io::stdout().flush()?;
                    continue;
                }

                if trimmed == "/nodes" || trimmed == "/status" {
                    let rt = runtime.read().await;
                    if let Some(registry) = &rt.registry {
                        let nodes = registry.list_all().await;
                        println!("节点 ({}) :", nodes.len());
                        for node_info in &nodes {
                            println!("  - {}: {}", node_info.id, node_info.description);
                        }
                    }
                    if let Some(nm) = &rt.node_manager {
                        println!("节点管理器: {} 节点活跃", nm.node_count().await);
                    }
                    continue;
                }

                println!("  [模拟回复] 收到: {trimmed}");
            }
            Err(e) => {
                eprintln!("读取输入错误: {e}");
                break;
            }
        }
    }

    Ok(())
}

// ── 优雅关闭 ────────────────────────────────────────────────────────────────

async fn graceful_shutdown(runtime: IFPRuntimeRef) -> anyhow::Result<()> {
    tracing::info!("[sea] 开始优雅关闭...");

    let mut rt = runtime.write().await;

    if let Some(nm) = &rt.node_manager {
        nm.shutdown_all().await;
    }

    if let Some(dp) = &rt.data_plane {
        dp.shutdown().await;
    }

    if let Some(cp) = &mut rt.control_plane {
        cp.shutdown().await;
    }

    rt.shutdown().await;

    tracing::info!("[sea] 优雅关闭完成");
    Ok(())
}

// ── Verbose 辅助函数 ────────────────────────────────────────────────────────

/// 为 verbose 模式创建一个 MakeWriter，每次调用追加写入日志文件。
fn make_log_file_writer(path: std::path::PathBuf) -> impl Fn() -> std::fs::File + Clone {
    move || {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("无法打开日志文件")
    }
}

/// 弹出一个新终端窗口，实时 tail 日志文件。
fn spawn_log_terminal(path: &std::path::Path) {
    #[cfg(windows)]
    {
        // 把包装脚本写到临时文件，完全规避命令行嵌套引号问题
        let ps_script = format!(
            "chcp 65001 >$null\n\
             $w=[Console]::WindowWidth; if ($w -le 0) {{ $w = 80 }}\n\
             function Wrap([string]$t) {{\n\
               if ($t.Length -le $w) {{ $t; return }}\n\
               $line=''; $col=0\n\
               for ($i=0; $i -lt $t.Length; $i++) {{\n\
                 $c=$t[$i]; $cw=if($c-gt0x7F){{2}}else{{1}}\n\
                 if ($col -gt 0 -and $col+$cw -gt $w) {{ $line; $line=$c; $col=$cw }}\n\
                 else {{ $line+=$c; $col+=$cw }}\n\
               }}\n\
               if ($line) {{ $line }}\n\
             }}\n\
             Get-Content -LiteralPath '{path}' -Tail 50 -Encoding UTF8 | ForEach-Object {{ Wrap $_ }}\n\
             Get-Content -LiteralPath '{path}' -Wait -Encoding UTF8 | ForEach-Object {{ Wrap $_ }}",
            path = path.to_string_lossy().replace('\'', "''"),
        );
        let ps_path = std::env::temp_dir().join("sea_log_tail.ps1");
        let _ = std::fs::write(&ps_path, &ps_script);

        // 在新窗口中执行包装脚本
        let result = std::process::Command::new("powershell")
            .args([
                "-NoLogo",
                "-NoProfile",
                "-Command",
                &format!(
                    "Start-Process powershell -ArgumentList '-NoLogo','-NoProfile','-NoExit','-ExecutionPolicy','Bypass','-File',\"{ps}\"",
                    ps = ps_path.to_string_lossy()
                ),
            ])
            .spawn();
        match &result {
            Ok(child) => {
                eprintln!("[sea] 日志终端已启动 (pid={})，日志文件: {}", child.id(), path.to_string_lossy());
            }
            Err(e) => {
                eprintln!("[sea] 无法启动日志终端: {e}");
                eprintln!("[sea] 日志仍写入: {}", path.to_string_lossy());
            }
        }
    }

    #[cfg(unix)]
    {
        let path_str = path.to_str().unwrap_or("/tmp/sea_logs.txt");
        // 尝试 xterm
        if std::process::Command::new("xterm")
            .args(["-T", "Sea Logs", "-e", "tail", "-f", path_str])
            .spawn()
            .is_err()
        {
            // fallback: gnome-terminal
            let _ = std::process::Command::new("gnome-terminal")
                .args(["--", "tail", "-f", path_str])
                .spawn();
        }
    }
}
