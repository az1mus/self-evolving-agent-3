//! 状态栏渲染 — 3 行固定高度。
//!
//! 对齐 Peri status_bar：
//! - 第 1 行：模式 · 节点数 · 通道数 · 模型名 · 内存用量
//! - 第 2 行：后台节点数 · 快捷键提示
//! - 第 3 行：留空

use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use super::App;
use crate::app::UiMode;
use crate::theme;

/// 渲染 3 行状态栏。
pub fn render_status_bar(f: &mut Frame, app: &App, area: Rect) {
    if area.width < 4 || area.height < 3 {
        return;
    }

    // 第 1 行：系统状态
    let row1 = render_first_row(app, area.width);
    f.render_widget(row1, Rect { y: area.y, height: 1, ..area });

    // 第 2 行：事件 + 快捷键
    let row2 = render_second_row(app, area.width);
    f.render_widget(row2, Rect { y: area.y + 1, height: 1, ..area });

    // 第 3 行：留空（视觉缓冲）
    f.render_widget(
        Paragraph::new(""),
        Rect { y: area.y + 2, height: 1, ..area },
    );
}

/// 第 1 行：模式 · 节点数 · 通道数 · 模型名。
fn render_first_row(app: &App, _width: u16) -> Paragraph<'_> {
    let mut spans = Vec::new();

    // 模式指示
    let (mode_icon, mode_text, mode_color) = match app.mode {
        UiMode::Idle => ("●", "Idle",  theme::MUTED),
        UiMode::Loading => ("⏺", "Running", theme::SAGE),
    };

    spans.push(Span::styled(
        format!("{mode_icon} {mode_text}"),
        Style::default().fg(mode_color).add_modifier(Modifier::BOLD),
    ));
    spans.push(divider());

    // 节点数
    spans.push(Span::styled(
        format!("{} 节点", app.node_count),
        Style::default().fg(theme::TEXT),
    ));
    spans.push(divider());

    // 通道数
    spans.push(Span::styled(
        format!("{} 通道", app.channel_count),
        Style::default().fg(theme::TEXT),
    ));
    spans.push(divider());

    // 模型名
    if !app.session_model.is_empty() && app.session_model != "none" {
        spans.push(Span::styled(
            &app.session_model,
            Style::default().fg(theme::MODEL_INFO),
        ));
        spans.push(divider());
    }

    // 消息计数
    let user_msgs = app.bubbles.iter().filter(|b| matches!(b.kind, crate::app::BubbleKind::User)).count();
    spans.push(Span::styled(
        format!("{} 条消息", app.bubbles.len()),
        Style::default().fg(theme::MUTED),
    ));
    if user_msgs > 0 {
        spans.push(Span::styled(
            format!(" ({} 用户)", user_msgs),
            Style::default().fg(theme::DIM),
        ));
    }

    Paragraph::new(Line::from(spans))
}

/// 第 2 行：后台节点 + 快捷键（右侧对齐）。
fn render_second_row(app: &App, width: u16) -> Paragraph<'_> {
    let mut left_spans = Vec::new();

    // 后台节点数
    if !app.bg_agents.is_empty() {
        left_spans.push(Span::styled(
            format!("{} 后台节点", app.bg_agents.len()),
            Style::default().fg(theme::MUTED),
        ));
    }

    // 快捷键（右侧对齐）
    let hint_str = match app.mode {
        UiMode::Loading => " Esc 中止 · ↑↓ 历史".to_string(),
        UiMode::Idle => " Enter 发送 · Alt+Enter 换行 · ↑↓ 历史 · Esc 退出".to_string(),
    };

    let hint_parts: Vec<String> = hint_str.split(" · ").map(String::from).collect();

    let hint_width = hint_str.len() as u16;
    let padding = width.saturating_sub(hint_width).saturating_sub(2);

    let mut all_spans = left_spans;

    if padding > 0 {
        all_spans.push(Span::styled(" ".repeat(padding as usize), Style::default()));
    } else if !all_spans.is_empty() {
        all_spans.push(Span::styled(" ", Style::default()));
    }

    // 快捷键分段着色
    for (i, part) in hint_parts.iter().enumerate() {
        if i > 0 || !all_spans.is_empty() {
            all_spans.push(Span::styled(" · ", Style::default().fg(theme::DIM)));
        }
        if let Some((key, desc)) = part.split_once(' ') {
            all_spans.push(Span::styled(
                key.to_string(),
                Style::default().fg(theme::MUTED).add_modifier(Modifier::BOLD),
            ));
            all_spans.push(Span::styled(
                format!(" {desc}"),
                Style::default().fg(theme::DIM),
            ));
        } else {
            all_spans.push(Span::styled(part.to_string(), Style::default().fg(theme::DIM)));
        }
    }

    Paragraph::new(Line::from(all_spans))
}

fn divider() -> Span<'static> {
    Span::styled(" │ ", Style::default().fg(theme::DIM))
}
