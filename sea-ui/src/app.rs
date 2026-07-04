//! SEA TUI 应用状态
//!
//! 设计对齐 Peri，支持：
//! - 滚动位置跟踪 + 自动滚动
//! - 多行输入
//! - 粘性消息头
//! - 后台节点列表

use std::collections::VecDeque;
use std::time::Instant;

/// UI 模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiMode {
    Idle,
    Loading,
}

/// 消息气泡类型。
#[derive(Debug, Clone)]
pub enum BubbleKind {
    User,
    Assistant,
    System,
    Tool,
}

/// 单条渲染气泡。
#[derive(Debug, Clone)]
pub struct Bubble {
    pub kind: BubbleKind,
    pub content: String,
}

/// 后台节点信息（Python 工具等）。
#[derive(Debug, Clone)]
pub struct BgAgentInfo {
    pub name: String,
    pub color_idx: usize,
    pub messages_in: u64,
    pub messages_out: u64,
    pub pid: Option<u32>,
}

/// App 全局状态。
pub struct App {
    // ── 模式 ──
    pub mode: UiMode,
    pub quit: bool,

    // ── 消息 ──
    pub bubbles: VecDeque<Bubble>,

    // ── 滚动 ──
    /// 当前滚动偏移（从顶部数起的行数，0=最顶部）。
    /// 对齐 Peri: scroll_offset 是 from-top 语义，新内容追加到底部不会导致偏移漂移。
    pub scroll_offset: usize,
    /// 是否自动跟随底部（对齐 Peri scroll_follow）。
    pub scroll_follow: bool,
    /// 最后一条人类消息的内容（供 sticky header 使用）。
    pub last_user_message: Option<String>,

    // ── 输入 ──
    pub input: String,
    pub cursor: usize,
    /// Alt+Enter 换行的行号集合（不用于光标跳转，仅渲染用）。
    pub input_line_breaks: Vec<usize>,
    /// 输入历史。
    pub history: VecDeque<String>,
    pub history_index: Option<usize>,
    /// 上次键盘活动时间（用于闪烁提示）。
    pub last_activity: Instant,

    // ── 终端 ──
    pub terminal_width: u16,
    pub terminal_height: u16,

    // ── Loading 动画 ──
    pub spinner_frame: u8,
    pub spinner_updated: bool,

    // ── 后台节点 ──
    pub bg_agents: Vec<BgAgentInfo>,

    // ── 运行时信息 ──
    pub node_count: usize,
    pub channel_count: usize,
    pub session_model: String,
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

impl App {
    pub fn new() -> Self {
        Self {
            mode: UiMode::Idle,
            quit: false,
            bubbles: VecDeque::new(),
            scroll_offset: 0,
            scroll_follow: true,
            last_user_message: None,
            input: String::new(),
            cursor: 0,
            input_line_breaks: Vec::new(),
            history: VecDeque::new(),
            history_index: None,
            last_activity: Instant::now(),
            terminal_width: 80,
            terminal_height: 24,
            spinner_frame: 0,
            spinner_updated: false,
            bg_agents: Vec::new(),
            node_count: 0,
            channel_count: 0,
            session_model: String::from("none"),
        }
    }

    // ── 消息管理 ──

    pub fn push_user(&mut self, text: &str) {
        self.last_user_message = Some(text.to_string());
        self.bubbles.push_back(Bubble {
            kind: BubbleKind::User,
            content: text.to_string(),
        });
        self.scroll_to_bottom();
    }

    pub fn push_assistant(&mut self, text: &str) {
        self.bubbles.push_back(Bubble {
            kind: BubbleKind::Assistant,
            content: text.to_string(),
        });
        self.scroll_to_bottom();
    }

    pub fn push_system(&mut self, text: &str) {
        self.bubbles.push_back(Bubble {
            kind: BubbleKind::System,
            content: text.to_string(),
        });
        self.scroll_to_bottom();
    }

    pub fn push_tool(&mut self, text: &str) {
        self.bubbles.push_back(Bubble {
            kind: BubbleKind::Tool,
            content: text.to_string(),
        });
    }

    pub fn scroll_to_bottom(&mut self) {
        self.scroll_follow = true;
    }

    /// 手动上滚（from-top 语义: 减少 scroll_offset → 向顶部移动）。
    pub fn scroll_up(&mut self) {
        self.scroll_follow = false;
        self.scroll_offset = self.scroll_offset.saturating_sub(1);
    }

    /// 手动下滚（from-top 语义: 增加 scroll_offset → 向底部移动）。
    pub fn scroll_down(&mut self) {
        self.scroll_offset += 1;
        // 渲染时会自动检测 offset >= max_scroll 并恢复 scroll_follow
    }

    // ── 输入管理 ──

    pub fn insert_char(&mut self, c: char) {
        self.last_activity = Instant::now();
        self.input.insert(self.cursor, c);
        self.cursor += c.len_utf8();
    }

    pub fn backspace(&mut self) {
        self.last_activity = Instant::now();
        if self.cursor > 0 {
            let prev = self.input[..self.cursor].chars().rev().next().unwrap();
            let len = prev.len_utf8();
            self.input.drain((self.cursor - len)..self.cursor);
            self.cursor -= len;
        }
    }

    pub fn delete(&mut self) {
        self.last_activity = Instant::now();
        if self.cursor < self.input.len() {
            let next = self.input[self.cursor..].chars().next().unwrap();
            let len = next.len_utf8();
            self.input.drain(self.cursor..(self.cursor + len));
        }
    }

    pub fn cursor_left(&mut self) {
        self.last_activity = Instant::now();
        if self.cursor > 0 {
            let prev = self.input[..self.cursor].chars().rev().next().unwrap();
            self.cursor -= prev.len_utf8();
        }
    }

    pub fn cursor_right(&mut self) {
        self.last_activity = Instant::now();
        if self.cursor < self.input.len() {
            let next = self.input[self.cursor..].chars().next().unwrap();
            self.cursor += next.len_utf8();
        }
    }

    pub fn cursor_home(&mut self) {
        self.cursor = 0;
    }

    pub fn cursor_end(&mut self) {
        self.cursor = self.input.len();
    }

    /// 提交输入 → 返回文本，或 None（空输入）。
    pub fn submit(&mut self) -> Option<String> {
        let trimmed = self.input.trim().to_string();
        if trimmed.is_empty() {
            return None;
        }

        // 保存历史
        const MAX_HISTORY: usize = 100;
        if self.history.len() >= MAX_HISTORY {
            self.history.pop_front();
        }
        self.history.push_back(trimmed.clone());
        self.history_index = None;

        self.input.clear();
        self.cursor = 0;
        self.input_line_breaks.clear();
        self.last_activity = Instant::now();
        self.scroll_follow = true;

        Some(trimmed)
    }

    pub fn history_up(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let idx = match self.history_index {
            Some(i) if i + 1 < self.history.len() => i + 1,
            Some(_) => return,
            None => 0,
        };
        self.history_index = Some(idx);
        self.input = self.history[self.history.len() - 1 - idx].clone();
        self.cursor = self.input.len();
        self.input_line_breaks.clear();
    }

    pub fn history_down(&mut self) {
        match self.history_index {
            Some(0) => {
                self.history_index = None;
                self.input.clear();
                self.cursor = 0;
                self.input_line_breaks.clear();
            }
            Some(i) => {
                self.history_index = Some(i - 1);
                self.input = self.history[self.history.len() - 1 - (i - 1)].clone();
                self.cursor = self.input.len();
                self.input_line_breaks.clear();
            }
            None => {}
        }
    }

    /// 估算输入框需要的行数。
    pub fn estimate_input_lines(&self) -> usize {
        let width = self.terminal_width.max(10) as usize;
        let text = if self.input.is_empty() {
            " "
        } else {
            &self.input
        };
        let char_count = text.chars().count();
        // 粗糙估算：每行可容纳 (width - 3) 个字符（减 3 是前缀和边距）
        let chars_per_line = (width.saturating_sub(4)).max(1);
        1 + char_count / chars_per_line
    }

    /// 更新 spinner 帧。
    pub fn tick_spinner(&mut self) {
        self.spinner_frame = self.spinner_frame.wrapping_add(1);
        self.spinner_updated = true;
    }
}
