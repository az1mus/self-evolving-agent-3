//! 主 UI 布局 — 对齐 Peri 的垂直布局。
//!
//! ```text
//! ┌────────────────────────────────────┐
//! │ [0] Sticky Header  (0~3 rows)    │  ← 滚动时显示最后用户消息
//! ├────────────────────────────────────┤
//! │ [1] Messages Area  (剩余空间)     │  ← 视口裁剪 + 滚动条 + spinner
//! ├────────────────────────────────────┤
//! │ [2] Input Area     (3~8 rows)    │  ← 多行输入 + ❯ 前缀
//! ├────────────────────────────────────┤
//! │ [3] Status Bar     (3 rows)      │  ← 模式/节点/快捷键
//! ├────────────────────────────────────┤
//! │ [4] BG Agents      (0~6 rows)    │  ← Python 工具后台列表
//! └────────────────────────────────────┘
//! ```
//!
//! Peri 的关键设计：sticky header 和 message area 一起渲染，
//! 由 message_area::render_messages 内部控制是否显示 sticky header。

pub mod bg_agent_bar;
pub mod input_area;
pub mod message_area;
pub mod status_bar;
pub mod sticky_header;
pub mod welcome;

use ratatui::{
    layout::{Constraint, Direction, Layout},
    Frame,
};

use crate::app::App;

/// 主渲染入口。
pub fn render(f: &mut Frame, app: &mut App) {
    let area = f.area();

    let sticky_height = sticky_header::estimate_sticky_header_height(app);
    let input_height = input_area::estimate_input_height(app);
    let bg_bar_height = bg_agent_bar::estimate_bg_bar_height(app);

    // 显式计算消息区高度，避免 ratatui 0.30 Flex::Start 下
    // Fill/Min 约束无法正确拉伸的问题。
    let fixed_height = sticky_height + input_height + 3 + bg_bar_height;
    let message_height = area.height.saturating_sub(fixed_height).max(1);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(sticky_height),   // [0] Sticky Header (在 messages 内渲染)
            Constraint::Length(message_height),  // [1] Messages Area — 显式高度
            Constraint::Length(input_height),     // [2] Input Area
            Constraint::Length(3),               // [3] Status Bar
            Constraint::Length(bg_bar_height),   // [4] BG Agents
        ])
        .split(area);

    // [0] + [1] 一起渲染：由 message_area 控制 sticky header 的显示
    message_area::render_messages(f, app, chunks[0], chunks[1]);

    // [2] 输入区
    input_area::render_input(f, app, chunks[2]);

    // [3] 状态栏
    status_bar::render_status_bar(f, app, chunks[3]);

    // [4] 后台节点
    if bg_bar_height > 0 {
        bg_agent_bar::render_bg_agent_bar(f, app, chunks[4]);
    }
}
