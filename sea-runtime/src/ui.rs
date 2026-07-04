use std::sync::Arc;
use tokio::sync::mpsc;

use sea_common::{Message, SeaResult};

use crate::data_plane::DataPlane;

/// UI — 终端交互循环。
///
/// 用户交互边界。抢占终端，循环读取 stdin，显示 markdown 渲染输出到 stdout。
/// 内置节点，无 Python 子进程。
pub struct Ui {
    data_plane: Arc<DataPlane>,
    /// Agent 回复接收端。
    rx: Option<mpsc::Receiver<Message>>,
    running: bool,
}

impl Ui {
    pub fn new(data_plane: Arc<DataPlane>) -> Self {
        Self {
            data_plane,
            rx: None,
            running: false,
        }
    }

    /// 绑定输入通道（接收 agent-core 回复）。
    pub fn bind_rx(&mut self, rx: mpsc::Receiver<Message>) {
        self.rx = Some(rx);
    }

    /// 启动 UI 主循环。
    pub async fn run(&mut self) -> SeaResult<()> {
        let mut rx = self.rx.take().expect("UI: 未绑定输入通道");
        self.running = true;

        println!("🌊 SEA CLI — Self-Evolving Agent");

        // 并行等待：用户输入 或 Agent 推送
        loop {
            tokio::select! {
                // 用户输入（简化版：实际应使用 terminal raw mode）
                input = read_user_input() => {
                    match input {
                        Some(line) => {
                            if let Some(cmd) = self.handle_special_command(&line).await {
                                // 特殊命令已处理
                                if cmd == "exit" {
                                    break;
                                }
                                continue;
                            }

                            // 构造用户消息
                            let msg = Message::with_text(
                                sea_common::generate_trace_id(),
                                &line,
                            );

                            // 发送给 agent-core
                            if let Err(e) = self.data_plane.send("ui:out", msg).await {
                                tracing::error!("[UI] 发送失败: {e}");
                            }
                        }
                        None => break,
                    }
                }

                // Agent 回复
                Some(agent_msg) = rx.recv() => {
                    self.handle_agent_response(&agent_msg);
                }
            }
        }

        self.running = false;
        println!("\n👋 SEA CLI 已退出");
        Ok(())
    }

    /// 处理 Agent 回复。
    fn handle_agent_response(&self, msg: &Message) {
        if let Ok(text) = msg.content_as_text() {
            println!("\n{}", text);
        }
    }

    /// 处理特殊命令。
    async fn handle_special_command(&self, cmd: &str) -> Option<&'static str> {
        match cmd.trim() {
            "/exit" | "/quit" => Some("exit"),
            "/help" => {
                println!("可用命令:");
                println!("  /exit     退出");
                println!("  /help     显示帮助");
                println!("  /clear    清屏");
                Some("help")
            }
            "/clear" => {
                // 简单清屏
                print!("\x1B[2J\x1B[1;1H");
                Some("clear")
            }
            _ => None,
        }
    }
}

/// 读取用户输入（简化版）。
async fn read_user_input() -> Option<String> {
    use tokio::io::AsyncBufReadExt;
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut line = String::new();
    stdin.read_line(&mut line).await.ok()?;
    let trimmed = line.trim().to_string();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed)
}

/// 生成默认终端提示符。
pub const PROMPT: &str = "sea> ";
