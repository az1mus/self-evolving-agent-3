//! Welcome Card — 空消息时垂直+水平居中显示。

use ratatui::{
    layout::{Alignment, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use crate::theme;

/// 渲染 Welcome Card。
pub fn render_welcome(f: &mut Frame, area: Rect) {
    let logo_lines = [
        "███████╗███████╗ █████╗ ",
        "██╔════╝██╔════╝██╔══██╗",
        "███████╗█████╗  ███████║",
        "╚════██║██╔══╝  ██╔══██║",
        "███████║███████╗██║  ██║",
        "╚══════╝╚══════╝╚═╝  ╚═╝",
    ];

    let mut lines = Vec::new();

    // Logo (ACCENT + BOLD)
    for logo_line in &logo_lines {
        lines.push(Line::from(Span::styled(
            *logo_line,
            Style::default()
                .fg(theme::ACCENT)
                .add_modifier(Modifier::BOLD),
        )));
    }

    // 副标题
    lines.push(Line::from(Span::styled(
        "  Self-Evolving Agent CLI",
        Style::default().fg(theme::MUTED),
    )));

    // 分隔线
    lines.push(Line::from(Span::styled(
        "────── What can I do? ──────",
        Style::default().fg(theme::DIM),
    )));
    lines.push(Line::from(""));

    // 功能亮点
    let highlights = [
        " •  IFP Runtime — 控制面/数据面/节点管理",
        " •  多 Agent 协作 — 自演进架构",
        " •  Markdown 渲染输出",
        " •  命令系统 — /help 查看全部",
    ];
    for h in &highlights {
        lines.push(Line::from(vec![
            Span::styled(" • ", Style::default().fg(theme::ACCENT)),
            Span::styled(*h, Style::default().fg(theme::TEXT)),
        ]));
    }

    lines.push(Line::from(""));

    // 命令提示
    let cmd_hints = [
        Span::styled("/help", Style::default().fg(theme::WARNING)),
        Span::styled("  ", Style::default().fg(theme::MUTED)),
        Span::styled("查看全部命令", Style::default().fg(theme::MUTED)),
    ];
    lines.push(Line::from(Vec::from(cmd_hints)));

    lines.push(Line::from(""));

    // 快捷键提示 (DIM)
    let hints = [
        Span::styled("Enter 发送 · ↑↓ 历史 · Alt+Enter 换行 · Esc退出",
            Style::default().fg(theme::DIM)),
    ];
    lines.push(Line::from(Vec::from(hints)));

    let text_height = lines.len() as u16;

    let paragraph = Paragraph::new(lines)
        .alignment(Alignment::Center)
        .style(Style::default());

    // 垂直居中
    let v_offset = area.height.saturating_sub(text_height) / 2;
    let centered_area = Rect {
        x: area.x,
        y: area.y + v_offset,
        width: area.width,
        height: text_height,
    };

    f.render_widget(paragraph, centered_area);
}
