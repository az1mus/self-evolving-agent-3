//! 后台节点栏 — 显示正在运行的 Python 工具节点。
//!
//! 对齐 Peri bg_agent_bar：List widget + 彩色圆点 + 消息计数 + 运行时间。

use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Padding},
    Frame,
};

use super::App;
use crate::theme;

/// 8 色调色板循环。
const AGENT_COLORS: [ratatui::style::Color; 8] = [
    theme::SAGE,           // Green
    theme::THINKING,       // Purple
    theme::WARNING,        // Amber
    theme::ERROR,          // Red
    theme::LOADING,        // Blue
    theme::BASH_BORDER,    // Pink
    theme::MODEL_INFO,     // Brown
    theme::ACCENT,         // Orange
];

/// 渲染后台节点列表。
pub fn render_bg_agent_bar(f: &mut Frame, app: &App, area: Rect) {
    if app.bg_agents.is_empty() || area.height < 2 || area.width < 4 {
        return;
    }

    let block = Block::default()
        .borders(Borders::TOP)
        .border_style(Style::default().fg(theme::DIM))
        .padding(Padding::new(2, 0, 0, 0));
    let inner = block.inner(area);
    f.render_widget(&block, area);

    let max_display = (inner.height as usize).min(4).min(app.bg_agents.len());
    let total = app.bg_agents.len();

    let mut items: Vec<ListItem> = Vec::with_capacity(max_display + 1);

    // 标题行
    items.push(ListItem::from(Line::from(vec![
        Span::styled("■", Style::default().fg(theme::SAGE).add_modifier(Modifier::BOLD)),
        Span::styled(
            format!(" 后台节点 ({total})",),
            Style::default().fg(theme::MUTED),
        ),
    ])));

    for i in 0..max_display {
        let agent = &app.bg_agents[i];
        let color = AGENT_COLORS[agent.color_idx % AGENT_COLORS.len()];

        let name = if agent.name.len() > 18 {
            format!("{:.17}…", agent.name)
        } else {
            agent.name.clone()
        };

        let pid_str = agent
            .pid
            .map(|p| format!("pid={p}"))
            .unwrap_or_default();

        let line = Line::from(vec![
            Span::styled("● ", Style::default().fg(color)),
            Span::styled(name, Style::default().fg(theme::TEXT)),
            Span::styled(
                format!("  in={} out={}", agent.messages_in, agent.messages_out),
                Style::default().fg(theme::MUTED),
            ),
            Span::styled(
                if !pid_str.is_empty() {
                    format!("  {pid_str}")
                } else {
                    String::new()
                },
                Style::default().fg(theme::DIM),
            ),
        ]);

        items.push(ListItem::from(line));
    }

    // 如果还有更多
    if total > max_display {
        items.push(ListItem::from(Line::from(Span::styled(
            format!("  … +{} 更多", total - max_display),
            Style::default().fg(theme::DIM),
        ))));
    }

    let list = List::new(items);
    f.render_widget(list, inner);
}

/// 估算后台节点栏的高度。
pub fn estimate_bg_bar_height(app: &App) -> u16 {
    if app.bg_agents.is_empty() {
        return 0;
    }
    let count = app.bg_agents.len().min(4);
    (1 + count + 1).min(6) as u16 // 标题 + 节点 + 更多行
}
