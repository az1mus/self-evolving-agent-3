//! 输入区渲染 — 多行输入 + 行数自适应 + ❯ 前缀。
//!
//! 对齐 Peri input_area：Borders::TOP | Borders::BOTTOM + 多行 Paragraph + cursor。

use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Padding},
    Frame,
};

use super::App;
use crate::app::UiMode;
use crate::theme;

/// 渲染输入区域。
pub fn render_input(f: &mut Frame, app: &App, area: Rect) {
    if area.width < 6 || area.height < 1 {
        return;
    }

    let loading = matches!(app.mode, UiMode::Loading);

    let border_color = if loading { theme::DIM } else { theme::MUTED };
    let block = Block::default()
        .borders(Borders::TOP | Borders::BOTTOM)
        .border_style(Style::default().fg(border_color))
        .padding(Padding::horizontal(3));
    let inner = block.inner(area);
    f.render_widget(&block, area);

    // ❯ 前缀
    render_prompt(f, area, loading);

    // 输入文本
    render_input_text(f, app, loading, inner);
}

fn render_prompt(f: &mut Frame, area: Rect, loading: bool) {
    let x = area.x + 1;
    let y = area.y + 1;
    let color = if loading { theme::MUTED } else { theme::ACCENT };
    f.render_widget(
        Paragraph::new(Span::styled("❯", Style::default().fg(color).add_modifier(Modifier::BOLD))),
        Rect { x, y, width: 2, height: 1 },
    );
}

/// 渲染输入文本并设置光标。
///
/// 算法：
/// 1. 将输入按硬换行 (`\n`) 拆段
/// 2. 每段按 `inner.width` 拆分显示行，记录每行的 (start_byte, end_byte)
/// 3. 找到光标所在的显示行并计算 column offset（**display width**，非 char count）
/// 4. 裁剪到可视高度，计算 visible cursor position
fn render_input_text(f: &mut Frame, app: &App, loading: bool, inner: Rect) {
    if loading {
        f.render_widget(
            Paragraph::new(Span::styled("⏺ 等待 Agent 回复…", Style::default().fg(theme::MUTED))),
            inner,
        );
        return;
    }

    if app.input.is_empty() {
        f.render_widget(
            Paragraph::new(Span::styled("输入消息…", Style::default().fg(theme::DIM))),
            inner,
        );
        return;
    }

    let max_chars = (inner.width.max(1)) as usize;

    // ── 1. 构建显示行范围列表 ──
    struct DisplayLine { start: usize, end: usize, content: String }

    let mut lines: Vec<DisplayLine> = Vec::new();
    let mut byte_pos = 0usize;
    let text = &app.input;

    for raw_line in text.lines() {
        let raw_bytes = raw_line.as_bytes();
        let mut seg_start = 0usize;

        loop {
            if seg_start >= raw_bytes.len() {
                break;
            }
            let mut seg_end = seg_start;
            let mut count = 0usize;
            while seg_end < raw_bytes.len() {
                let next_char = raw_line[seg_end..].chars().next();
                let Some(c) = next_char else { break };
                if count >= max_chars { break; }
                count += 1;
                seg_end += c.len_utf8();
            }
            let abs_start = byte_pos + seg_start;
            let abs_end = byte_pos + seg_end;
            let content = &text[abs_start..abs_end];

            lines.push(DisplayLine { start: abs_start, end: abs_end, content: content.to_string() });

            seg_start = seg_end;
        }

        // 硬换行：新行始终需要一行（即使是空的）
        if raw_bytes.is_empty() {
            let abs_start = byte_pos;
            let abs_end = byte_pos;
            lines.push(DisplayLine { start: abs_start, end: abs_end, content: String::new() });
        }

        byte_pos += raw_bytes.len() + 1; // +1 for \n
    }

    // 特殊处理：输入末尾有空行
    if text.ends_with('\n') {
        lines.push(DisplayLine {
            start: text.len(),
            end: text.len(),
            content: String::new(),
        });
    }

    // ── 2. 找到光标所在行和列 ──
    let mut cursor_line_idx = 0usize;
    let mut cursor_col_width = 0usize;

    for (i, dl) in lines.iter().enumerate() {
        if app.cursor >= dl.start && app.cursor <= dl.end {
            cursor_line_idx = i;
            // 计算光标前文本的 display width
            let before_cursor = &text[dl.start..app.cursor];
            cursor_col_width = unicode_width::UnicodeWidthStr::width(before_cursor);
            break;
        }
        // 如果光标在当前行末尾的换行符位置（app.cursor == end + 1 for \n）
        if app.cursor == dl.end + 1 && dl.end < text.len() {
            cursor_line_idx = i;
            cursor_col_width = unicode_width::UnicodeWidthStr::width(&text[dl.start..dl.end]);
            break;
        }
    }

    // ── 3. 裁剪到可视高度 ──
    let visible_height = inner.height as usize;
    let skip = if lines.len() > visible_height {
        lines.len() - visible_height
    } else {
        0
    };
    let visible_lines: Vec<&DisplayLine> = lines.iter().skip(skip).collect();

    // ── 4. 渲染 ──
    let display_lines: Vec<Line> = visible_lines
        .iter()
        .map(|dl| {
            if dl.content.is_empty() {
                Line::from("")
            } else {
                Line::from(Span::styled(&dl.content, Style::default().fg(theme::TEXT)))
            }
        })
        .collect();

    f.render_widget(Paragraph::new(display_lines), inner);

    // ── 5. 光标定位 ──
    let cursor_visible_row = cursor_line_idx.saturating_sub(skip).min(visible_height.saturating_sub(1));
    let cursor_visible_row_u16 = cursor_visible_row as u16;
    let cursor_x = inner.x + cursor_col_width as u16;
    let cursor_y = inner.y + cursor_visible_row_u16;

    f.set_cursor_position((cursor_x, cursor_y));
}

pub fn estimate_input_height(app: &App) -> u16 {
    let line_count = app.estimate_input_lines();
    let max_by_area = (app.terminal_height as usize * 2 / 5).max(3);
    line_count.clamp(3, max_by_area).min(8) as u16
}
