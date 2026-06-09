mod config;
mod proxy;
mod state;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    routing::{any, get},
    Router,
};
use tracing_subscriber::{fmt::time::UtcTime, EnvFilter};

use crate::config::Config;
use crate::state::AppState;

/// Глобальный переиспользуемый HTTP-клиент (пул соединений к upstream).
static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

pub fn http_client() -> &'static reqwest::Client {
    HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            // Без общего таймаута: стримы у Claude Code долгие.
            // Таймаут только на установку соединения.
            .connect_timeout(Duration::from_secs(20))
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .expect("не удалось создать reqwest-клиент")
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    // Путь к конфигу: первый аргумент или ./config.yaml.
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.yaml".to_string());

    let config = Config::load(&config_path)
        .with_context(|| format!("загрузка конфига из {config_path}"))?;

    tracing::info!(
        listen = %config.listen,
        upstream = %config.upstream,
        auth_header = config.auth_header.header_name(),
        keys = config.keys.len(),
        markers = ?config.exhaustion_markers,
        failover_statuses = ?config.failover_statuses,
        "конфиг загружен"
    );

    let listen = config.listen.clone();
    let state = Arc::new(AppState::new(config));

    let app = Router::new()
        .route("/proxy/status", get(proxy::status_handler))
        // Универсальный reverse-proxy на всё остальное, любые методы.
        .fallback(any(proxy::proxy_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("не удалось забиндить {listen}"))?;

    tracing::info!("прокси слушает на http://{listen}");
    tracing::info!("настройка Claude Code: ANTHROPIC_BASE_URL=http://{listen}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("ошибка работы сервера")?;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_timer(UtcTime::rfc_3339())
        .with_target(false)
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("не удалось установить обработчик Ctrl+C");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("не удалось установить обработчик SIGTERM")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("получен сигнал завершения, останавливаюсь");
}
