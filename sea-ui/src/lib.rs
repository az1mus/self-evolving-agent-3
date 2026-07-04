//! # SEA TUI
//!
//! SEA CLI 终端交互界面。外观与部分功能参照 Peri 设计语言。
//!
//! ## 设计
//!
//! - 色板：中性灰层级 + Claude 暖橙品牌色（配合 [`theme`] 模块）
//! - 布局：消息流 / 输入区 / 状态栏（3 段垂直布局）
//! - 命令：`/exit` `/help` `/clear` 等
//!
//! ## 双模式
//!
//! - [`run_tui`] — 独立开发模式，使用模拟回复（不依赖 Runtime）
//! - [`run_tui_with_agent`] — 生产模式，通过 mpsc 通道与 AgentCore 通信

pub mod app;
pub mod theme;
pub mod ui;

use std::io;
use std::time::Duration;

use anyhow::Result;
use crossterm::{
    event::{
        DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste,
        EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
        MouseEventKind,
    },
    execute,
    terminal::{
        disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
    },
};
use futures::StreamExt;
use ratatui::{backend::CrosstermBackend, Terminal};
use tokio::sync::mpsc;

use crate::app::{App, UiMode};

/// TUI 帧率限制 (约 30 FPS)。
const TARGET_FRAME_INTERVAL: Duration = Duration::from_millis(33);

// ── 独立开发模式 ─────────────────────────────────────────────────────────────

/// 运行独立 TUI（不连接 Runtime，使用模拟回复）。
///
/// 适用于 UI 开发调试。
pub fn run_tui() -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let (tx_out, mut rx_out) = mpsc::channel::<sea_common::Message>(256);
        let (tx_sim, rx_in) = mpsc::channel::<sea_common::Message>(256);

        // 模拟回复 task：收到用户消息后生成模拟回复
        tokio::spawn(async move {
            while let Some(msg) = rx_out.recv().await {
                let text = msg.content_as_text().unwrap_or_default();
                let reply = simulate_reply(&text);
                let reply_msg = sea_common::Message::with_text(
                    sea_common::generate_trace_id(),
                    &reply,
                );
                let _ = tx_sim.send(reply_msg).await;
            }
        });

        run_tui_loop(tx_out, Some(rx_in)).await
    })
}

/// 模拟 AI 回复（仅独立开发模式使用）。

// ── 生产模式 ─────────────────────────────────────────────────────────────────

/// 运行 TUI，接入真实 AgentCore。
///
/// # 参数
/// - `tx_out`: 发送端，TUI 产生消息后发送到此通道。
///             wiring 侧负责转发到 `data_plane.send("ui:out", msg)`。
/// - `rx_in`: 接收端，AgentCore 的回复消息由此通道送入 TUI。
///
/// # Wiring 示例（在 sea-bin 中完成）
///
/// ```ignore
/// let (tx_ui_out, mut rx_ui_out) = mpsc::channel(256);
/// let (tx_ui_in, rx_ui_in) = mpsc::channel(256);
///
/// dp.register_target_handler("ui:in", tx_ui_in).await?;
///
/// tokio::spawn(async move {
///     while let Some(msg) = rx_ui_out.recv().await {
///         let _ = dp.send("ui:out", msg).await;
///     }
/// });
///
/// sea_ui::run_tui_with_agent(tx_ui_out, rx_ui_in).await?;
/// ```
pub async fn run_tui_with_agent(
    tx_out: mpsc::Sender<sea_common::Message>,
    rx_in: mpsc::Receiver<sea_common::Message>,
) -> Result<()> {
    run_tui_loop(tx_out, Some(rx_in)).await
}

// ── 核心事件循环 ─────────────────────────────────────────────────────────────

async fn run_tui_loop(
    tx_out: mpsc::Sender<sea_common::Message>,
    rx_in: Option<mpsc::Receiver<sea_common::Message>>,
) -> Result<()> {
    // 初始化终端
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(
        stdout,
        EnterAlternateScreen,
        EnableMouseCapture,
        EnableBracketedPaste,
    )?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let result = run_app_with_agent(&mut terminal, tx_out, rx_in).await;

    // 恢复终端
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture,
        DisableBracketedPaste,
    )?;
    terminal.show_cursor()?;

    if let Err(ref e) = result {
        eprintln!("Error: {e:#}");
    }

    result
}

/// 应用主循环：同时监听键盘事件和数据面消息。
async fn run_app_with_agent(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    tx_out: mpsc::Sender<sea_common::Message>,
    rx_in: Option<mpsc::Receiver<sea_common::Message>>,
) -> Result<()> {
    let mut app = App::new();

    terminal.draw(|f| ui::render(f, &mut app))?;

    // crossterm 异步事件流
    let mut event_stream = crossterm::event::EventStream::new();

    // 数据面消息接收端（可选）
    let mut rx_in = rx_in;

    loop {
        if let Ok(size) = crossterm::terminal::size() {
            app.terminal_width = size.0;
            app.terminal_height = size.1;
        }

        if app.quit {
            break;
        }

        let event_fut = event_stream.next();
        let msg_fut = async {
            match &mut rx_in {
                Some(rx) => rx.recv().await,
                None => std::future::pending::<Option<sea_common::Message>>().await,
            }
        };

        tokio::select! {
            // 分支 1：键盘事件
            maybe_event = event_fut => {
                match maybe_event {
                    Some(Ok(event)) => {
                        handle_input_event(&mut app, event, &tx_out);
                    }
                    Some(Err(e)) => {
                        tracing::warn!("[tui] 事件读取错误: {e}");
                    }
                    None => {
                        // EventStream 关闭（终端断开）
                        app.quit = true;
                    }
                }
            }

            // 分支 2：AgentCore 回复
            maybe_msg = msg_fut => {
                if let Some(msg) = maybe_msg {
                    handle_agent_message(&mut app, msg);
                }
                // None = channel closed, continue without quitting
            }

            // 分支 3：渲染 tick（更新 spinner 帧）
            _ = tokio::time::sleep(TARGET_FRAME_INTERVAL) => {
                if matches!(app.mode, UiMode::Loading) {
                    app.tick_spinner();
                }
            }
        }

        terminal.draw(|f| ui::render(f, &mut app))?;
    }

    Ok(())
}

/// 模拟 AI 回复（仅独立开发模式使用）。
fn simulate_reply(input: &str) -> String {
    let lower = input.to_lowercase();
    if lower.contains("hello") || lower.contains("hi") || lower.contains("你好") {
        return "你好！我是 SEA CLI。有什么我可以帮你的吗？".to_string();
    }
    if lower.contains("who") || lower.contains("你是") {
        return "我是 **SEA** — Self-Evolving Agent 框架的命令行界面。\n\n我基于 IFP v3.1 协议运行，支持控制面/数据面/节点管理等特性。".to_string();
    }
    if lower.contains("time") || lower.contains("时间") {
        let now = chrono::Local::now();
        return format!("当前时间: **{}**", now.format("%Y-%m-%d %H:%M:%S"));
    }
    format!(
        "收到: 「{input}」\n\n这是 SEA TUI 的占位回复。完整 Agent 功能接入中。"
    )
}

// ── 事件处理 ─────────────────────────────────────────────────────────────────

/// 处理键盘事件。
fn handle_input_event(
    app: &mut App,
    event: Event,
    tx_out: &mpsc::Sender<sea_common::Message>,
) {
    match event {
        Event::Key(key) => {
            if key.kind != KeyEventKind::Press {
                return;
            }

            match app.mode {
                UiMode::Loading => {
                    // Loading 时只处理退出
                    if key.code == KeyCode::Esc
                        || (key.code == KeyCode::Char('c')
                            && key.modifiers == KeyModifiers::CONTROL)
                    {
                        app.quit = true;
                    }
                }
                UiMode::Idle => match key.code {
                    KeyCode::Esc => {
                        app.quit = true;
                    }
                    KeyCode::Enter => {
                        if key.modifiers == KeyModifiers::ALT {
                            app.insert_char('\n');
                        } else {
                            handle_submit(app, tx_out);
                        }
                    }
                    KeyCode::Char(c) => app.insert_char(c),
                    KeyCode::Backspace => app.backspace(),
                    KeyCode::Delete => app.delete(),
                    KeyCode::Left => app.cursor_left(),
                    KeyCode::Right => app.cursor_right(),
                    KeyCode::Home => app.cursor_home(),
                    KeyCode::End => app.cursor_end(),
                    KeyCode::Up => app.history_up(),
                    KeyCode::Down => app.history_down(),
                    KeyCode::PageUp => {
                        for _ in 0..5 {
                            app.scroll_up();
                        }
                    }
                    KeyCode::PageDown => {
                        for _ in 0..5 {
                            app.scroll_down();
                        }
                    }
                    KeyCode::Tab => app.insert_char('\t'),
                    _ => {}
                },
            }
        }
        Event::Resize(w, h) => {
            app.terminal_width = w;
            app.terminal_height = h;
        }
        Event::Mouse(mouse) => match mouse.kind {
            MouseEventKind::ScrollDown => {
                app.scroll_down();
            }
            MouseEventKind::ScrollUp => {
                app.scroll_up();
            }
            _ => {}
        },
        _ => {}
    }
}

/// 处理用户提交的输入。
fn handle_submit(app: &mut App, tx_out: &mpsc::Sender<sea_common::Message>) {
    let Some(text) = app.submit() else {
        return;
    };

    if text.starts_with('/') {
        handle_command(app, &text);
        return;
    }

    // 添加用户消息
    app.push_user(&text);
    app.mode = UiMode::Loading;

    // 构造 Message 并通过通道发送
    let msg = sea_common::Message::with_text(
        sea_common::generate_trace_id(),
        &text,
    );

    let tx = tx_out.clone();
    tokio::task::spawn(async move {
        if let Err(e) = tx.send(msg).await {
            tracing::warn!("[tui] 发送消息失败: {e}");
        }
    });

    tracing::debug!("[tui] 消息已发送: {text}");
}

/// 处理从 AgentCore 收到的回复（含 verbose 模式下的日志行）。
fn handle_agent_message(app: &mut App, msg: sea_common::Message) {
    let text = msg.content_as_text().unwrap_or_else(|_| {
        msg.content_as_json()
            .map(|j| j.to_string())
            .unwrap_or_else(|_| "(无法解析的消息)".to_string())
    });

    if text.is_empty() {
        return;
    }

    // ── 日志行（verbose 模式）──
    // 作为 System 气泡渲染。不调用 scroll_to_bottom — 由渲染函数根据 scroll_follow 状态
    // 自动决定是否跟随底部（from-top 语义下不会因新日志追加而产生偏移漂移）。
    if msg.content_type == "text/x-log" {
        app.bubbles.push_back(crate::app::Bubble {
            kind: crate::app::BubbleKind::System,
            content: text,
        });
        return;
    }

    // ── 正常消息：根据内容类型决定气泡类型 ──
    let bubble = match msg.content_type.as_str() {
        "text/markdown" => crate::app::Bubble {
            kind: crate::app::BubbleKind::Assistant,
            content: text,
        },
        "text/plain" => crate::app::Bubble {
            kind: crate::app::BubbleKind::Assistant,
            content: text,
        },
        ct if ct.contains("json") => {
            // JSON 内容 → 尝试格式化显示
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                let pretty = serde_json::to_string_pretty(&parsed)
                    .unwrap_or_else(|_| text);
                crate::app::Bubble {
                    kind: crate::app::BubbleKind::Tool,
                    content: format!("```json\n{pretty}\n```"),
                }
            } else {
                crate::app::Bubble {
                    kind: crate::app::BubbleKind::System,
                    content: text,
                }
            }
        }
        _ => crate::app::Bubble {
            kind: crate::app::BubbleKind::Assistant,
            content: text,
        },
    };

    app.bubbles.push_back(bubble);
    app.scroll_to_bottom();
    app.mode = UiMode::Idle;

    tracing::debug!(
        "[tui] 收到消息: trace={}, len={}",
        msg.trace_id,
        msg.content.len()
    );
}

/// 处理内置命令。
fn handle_command(app: &mut App, cmd: &str) {
    match cmd.trim() {
        "/exit" | "/quit" => {
            app.quit = true;
        }
        "/help" => {
            let help_text = "\
# 可用命令

## 基本
`/exit` 或 `/quit`  — 退出程序
`/help`            — 显示此帮助
`/clear`           — 清屏消息
`/count`           — 显示消息计数

## 操作
`Enter`            — 发送消息
`Alt+Enter`        — 换行
`↑/↓`              — 输入历史
`Esc`              — 退出";
            app.push_assistant(help_text);
        }
        "/clear" => {
            app.bubbles.clear();
            app.scroll_offset = 0;
            app.scroll_follow = true;
        }
        "/count" => {
            let user_count = app
                .bubbles
                .iter()
                .filter(|b| matches!(b.kind, crate::app::BubbleKind::User))
                .count();
            let ai_count = app
                .bubbles
                .iter()
                .filter(|b| matches!(b.kind, crate::app::BubbleKind::Assistant))
                .count();
            app.push_system(&format!(
                "消息统计: {} 用户 · {} AI · {} 总计",
                user_count,
                ai_count,
                app.bubbles.len()
            ));
        }
        _ => {
            if cmd.starts_with('/') {
                app.push_system(&format!(
                    "未知命令: {cmd}。输入 /help 查看可用命令。"
                ));
            }
        }
    }
}
