//! 粘性消息头 — 滚动时显示最后一条用户消息。
//!
//! 对齐 Peri sticky_header：Paragraph + USER_BG 背景 + ❯ 前缀。

use ratatui::{
    layout::{Alignment, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Padding, Wrap},
    Frame,
};

use super::App;
use crate::theme;

/// 渲染粘性消息头。仅在 `!scroll_follow` 且有用户消息时显示。
pub fn render_sticky_header(f: &mut Frame, app: &App, area: Rect) {
    let Some(ref msg) = app.last_user_message else {
        return;
    };

    if area.height == 0 || area.width < 4 {
        return;
    }

    let block = Block::default()
        .borders(Borders::BOTTOM)
        .border_style(Style::default().fg(theme::DIM))
        .style(Style::default().bg(theme::USER_BG))
        .padding(Padding::new(2, 1, 0, 0));
    let inner = block.inner(area);
    f.render_widget(&block, area);

    // 截取到可视宽度
    let max_chars = (inner.width.saturating_sub(2) * 2) as usize; // 2x 宽度 = 大概字符数
    let text: String = msg
        .chars()
        .take(max_chars)
        .chain(if msg.len() > max_chars { Some('…') } else { None })
        .collect();

    let spans = vec![Span::styled(
        format!("❯ {text}"),
        Style::default()
            .fg(theme::TEXT)
            .add_modifier(Modifier::BOLD),
    )];

    let paragraph = Paragraph::new(Line::from(spans))
        .wrap(Wrap { trim: true })
        .alignment(Alignment::Left);

    f.render_widget(paragraph, inner);
}

/// 估算粘性消息头的行数。
pub fn estimate_sticky_header_height(app: &App) -> u16 {
    let Some(ref msg) = app.last_user_message else {
        return 0;
    };
    if app.scroll_follow {
        return 0; // 在底部时不显示
    }

    let width = app.terminal_width.max(10) as usize;
    let char_count = msg.chars().count();
    let chars_per_line = (width.saturating_sub(4)).max(1);
    let lines = (char_count / chars_per_line) + 1;
    lines.clamp(1, 3) as u16
}
