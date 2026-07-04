//! 消息区渲染 — 逐行容器化渲染 + 滚动条 + spinner 动画。
//!
//! 设计原则：
//! - 逐行容器化：每行作为一个独立的 Paragraph 渲染在 1 行高的 Rect 内，
//!   由 ratatui 的 buffer 自然裁剪，杜绝任何跨行/跨区溢出。
//! - 预换行 + 无 Wrap：在构建行时按宽度拆行确保每行不超宽，
//!   Paragraph 不设 Wrap → 每行严格占 1 行高。
//! - 粘性消息头（sticky header）仅在用户不在底部时显示。

use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use super::sticky_header;
use super::welcome;
use crate::app::App;
use crate::app::{BubbleKind, UiMode};
use crate::theme;

/// Spinner 动画帧。
const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// 渲染消息区域。
///
/// `header_area` — sticky header 所在区域（从 Layout chunk[0] 传入）
/// `messages_area` — 消息列表所在区域（从 Layout chunk[1] 传入），同时也是容器边界。
pub fn render_messages(f: &mut Frame, app: &mut App, header_area: Rect, messages_area: Rect) {
    if messages_area.width < 4 || messages_area.height < 1 {
        return;
    }

    // Welcome 屏幕
    if app.bubbles.is_empty() && !matches!(app.mode, UiMode::Loading) {
        welcome::render_welcome(f, messages_area);
        return;
    }

    // 读取当前滚动状态（在构建消息行之前复制出来，避免借用冲突）
    let scroll_follow = app.scroll_follow;
    let scroll_offset = app.scroll_offset;

    let (new_scroll_follow, new_scroll_offset) = {
        // ── 构建全部消息行（预换行）──
        let all_lines = build_message_lines(app, messages_area.width);
        let total_visual = all_lines.len().max(1);
        let visible_height = messages_area.height as usize;

        // 计算可滚动的最大行数
        let max_scroll = total_visual.saturating_sub(visible_height);

        // ── 滚动偏移（对齐 Peri from-top 语义）──
        let (offset, new_scroll_follow, new_scroll_offset) = if scroll_follow {
            (max_scroll, true, max_scroll)
        } else {
            let off = scroll_offset.min(max_scroll);
            let new_follow = off >= max_scroll;
            (off, new_follow, off)
        };

        // ── 粘性消息头 ──
        let show_sticky = offset < max_scroll && header_area.height > 0;
        if show_sticky {
            sticky_header::render_sticky_header(f, app, header_area);
        }

        // ── 容器化渲染（逐行渲染）──
        // 每行作为一个独立的 Paragraph，渲染在 1 行高的 Rect 内。
        // Paragraph::render() 写入 Frame buffer 时以 Rect 为界，自然杜绝溢出。
        let text_x = messages_area.x;
        let text_width = messages_area.width.saturating_sub(1);
        for (i, line) in all_lines.iter().skip(offset).take(visible_height).enumerate() {
            let row = Rect {
                x: text_x,
                y: messages_area.y + i as u16,
                width: text_width,
                height: 1,
            };
            f.render_widget(Paragraph::new(line.clone()), row);
        }

        // ── 滚动条 ──
        if max_scroll > 0 {
            render_scrollbar(f, messages_area, offset as u16, max_scroll as u16);
        }

        (new_scroll_follow, new_scroll_offset)
    }; // all_lines / paragraph 在此释放 → app 的不可变借用结束

    // ── 写回滚动状态 ──
    app.scroll_follow = new_scroll_follow;
    app.scroll_offset = new_scroll_offset;
}

/// 构建所有消息行（含 spinner，含预换行）。
fn build_message_lines<'a>(app: &'a App, width: u16) -> Vec<Line<'a>> {
    let mut lines: Vec<Line<'a>> = Vec::new();
    // 每行可用显示宽度 = 区域宽度 - 2（前缀如 "❯ "）- 1（滚动条）
    let content_width = (width as usize).saturating_sub(3);

    for bubble in &app.bubbles {
        let prefix = match bubble.kind {
            BubbleKind::User => "❯ ",
            BubbleKind::Assistant => "",
            BubbleKind::System => "· ",
            BubbleKind::Tool => "⏺ ",
        };
        let (prefix_color, text_color) = bubble_style(&bubble.kind);

        let content = &bubble.content;
        for raw_line in content.lines() {
            let line_width = UnicodeWidthStr::width(raw_line);
            if raw_line.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled(prefix, Style::default().fg(prefix_color).add_modifier(Modifier::BOLD)),
                    Span::styled("", Style::default().fg(text_color)),
                ]));
            } else if line_width <= content_width {
                lines.push(Line::from(vec![
                    Span::styled(prefix, Style::default().fg(prefix_color).add_modifier(Modifier::BOLD)),
                    Span::styled(raw_line, Style::default().fg(text_color)),
                ]));
            } else {
                // 计算前缀在每行占用的宽度，传递给 wrap_line 作为每行可用的显示宽度
                // 即每行最大显示宽度 = content_width - prefix_width（实际上 prefix 已经在行首，
                // 所以每行独立渲染时 prefix + wrapped 总宽度 = prefix_width + wrapped_width
                // 需要 <= width - 1（滚动条），即 wrapped_width <= content_width）
                for wrapped in wrap_line_by_width(raw_line, content_width) {
                    lines.push(Line::from(vec![
                        Span::styled(prefix, Style::default().fg(prefix_color).add_modifier(Modifier::BOLD)),
                        Span::styled(wrapped, Style::default().fg(text_color)),
                    ]));
                }
            }
        }
    }

    // Loading spinner
    if matches!(app.mode, UiMode::Loading) {
        let spinner = SPINNER_FRAMES[app.spinner_frame as usize % SPINNER_FRAMES.len()];
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled(spinner, Style::default().fg(theme::LOADING).add_modifier(Modifier::BOLD)),
            Span::styled("  Agent 正在思考…", Style::default().fg(theme::MUTED)),
        ]));
    }

    lines.push(Line::from(""));
    lines
}

/// 将字符串按显示宽度折行（CJK 感知），尽量在单词边界断开。
///
/// 算法对齐 peri `wrap_cell_text`：
/// 1. 逐字符累加显示宽度
/// 2. 超过 `max_width` 时，向回查找到最后一个空白字符
/// 3. 在空白处断开，跳过尾部空白
fn wrap_line_by_width(line: &str, max_width: usize) -> Vec<String> {
    if max_width == 0 || line.is_empty() {
        return vec![line.to_string()];
    }
    let total_width = UnicodeWidthStr::width(line);
    if total_width <= max_width {
        return vec![line.to_string()];
    }

    let mut result = Vec::new();
    let text = line;
    let mut byte_pos = 0;

    while byte_pos < text.len() {
        let mut cur_width = 0usize;
        let mut content_end = byte_pos;

        for (i, c) in text[byte_pos..].char_indices() {
            let cw = UnicodeWidthChar::width(c).unwrap_or(0);
            // 至少推进一个字符，防止 CJK 超宽时的死循环
            if content_end > byte_pos && cur_width + cw > max_width {
                break;
            }
            cur_width += cw;
            content_end = byte_pos + i + c.len_utf8();
        }

        // 尝试在单词边界断开：从 content_end 向回退扫描空格
        let mut break_at = content_end;
        for (i, c) in text[byte_pos..content_end].char_indices().rev() {
            if c.is_whitespace() {
                break_at = byte_pos + i;
                break;
            }
        }

        let piece = text[byte_pos..break_at].trim();
        if !piece.is_empty() {
            result.push(piece.to_string());
        }

        byte_pos = break_at;
        // 跳过后续空白
        while byte_pos < text.len() {
            let c = text[byte_pos..].chars().next().unwrap();
            if c.is_whitespace() {
                byte_pos += c.len_utf8();
            } else {
                break;
            }
        }
    }

    if result.is_empty() {
        result.push(String::new());
    }
    result
}

fn bubble_style(kind: &BubbleKind) -> (ratatui::style::Color, ratatui::style::Color) {
    match kind {
        BubbleKind::User => (theme::ACCENT, theme::TEXT),
        BubbleKind::Assistant => (theme::TEXT, theme::TEXT),
        BubbleKind::System => (theme::DIM, theme::MUTED),
        BubbleKind::Tool => (theme::SAGE, theme::TEXT),
    }
}

/// 渲染垂直滚动条。
fn render_scrollbar(f: &mut Frame, area: Rect, offset: u16, max: u16) {
    if max == 0 || area.height < 2 {
        return;
    }
    let bar_height = ((area.height as f64 / (max + area.height) as f64) * area.height as f64)
        .max(1.0) as u16;
    let bar_top = ((offset as f64 / max as f64) * (area.height - bar_height) as f64) as u16;

    let x = area.x + area.width.saturating_sub(1);
    if x == 0 {
        return;
    }

    for y in 0..area.height {
        let is_bar = y >= bar_top && y < bar_top + bar_height;
        let is_arrow_up = offset < max && y == 0;
        let is_arrow_down = offset > 0 && y == area.height - 1;

        let ch = if is_arrow_up {
            "▲"
        } else if is_arrow_down {
            "▼"
        } else if is_bar {
            "┃"
        } else {
            " "
        };
        let color = if is_bar || is_arrow_up || is_arrow_down {
            theme::MUTED
        } else {
            theme::DIM
        };

        f.render_widget(
            Paragraph::new(Span::styled(ch, Style::default().fg(color))),
            Rect {
                x,
                y: area.y + y,
                width: 1,
                height: 1,
            },
        );
    }
}
