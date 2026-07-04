use tokio::signal;

/// 信号处理 —— 等待 OS 终止信号，转发到控制面。
pub struct SignalHandler;

impl SignalHandler {
    /// 等待终止信号（Ctrl+C / SIGTERM）。
    ///
    /// 返回后，调用方应执行优雅关闭流程。
    pub async fn wait_for_shutdown() {
        tracing::info!("[signal] 等待终止信号 (Ctrl+C)...");

        #[cfg(unix)]
        {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("无法注册 SIGTERM 处理器");

            tokio::select! {
                _ = signal::ctrl_c() => {
                    tracing::info!("[signal] 收到 Ctrl+C");
                }
                _ = sigterm.recv() => {
                    tracing::info!("[signal] 收到 SIGTERM");
                }
            }
        }

        #[cfg(not(unix))]
        {
            // Windows: 仅支持 Ctrl+C
            signal::ctrl_c().await.expect("无法注册 Ctrl+C 处理器");
            tracing::info!("[signal] 收到 Ctrl+C");
        }
    }
}
