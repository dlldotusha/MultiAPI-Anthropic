use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use futures_util::StreamExt;

use crate::state::AppState;

/// Порог буферизации тела для поиска маркера в НЕ-стримовых ответах.
/// Ответы об ошибках у Anthropic — короткий JSON, так что 256 КБ с запасом.
const MAX_BUFFER_FOR_MARKER: usize = 256 * 1024;

/// Заголовки, которые нельзя пробрасывать как есть на upstream/клиенту.
/// hop-by-hop + те, что reqwest/axum выставят сами.
fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
            | "content-length"
    )
}

/// GET /proxy/status — снимок метрик.
pub async fn status_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.snapshot())
}

/// Универсальный reverse-proxy handler для любых путей и методов.
pub async fn proxy_handler(
    State(state): State<Arc<AppState>>,
    req: Request,
) -> Response {
    match proxy_inner(state, req).await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::error!("ошибка прокси: {e:#}");
            (
                StatusCode::BAD_GATEWAY,
                format!("proxy error: {e}"),
            )
                .into_response()
        }
    }
}

async fn proxy_inner(state: Arc<AppState>, req: Request) -> anyhow::Result<Response> {
    let (parts, body) = req.into_parts();
    let method = parts.method;
    let uri = parts.uri;
    let headers = parts.headers;

    // Тело читаем один раз в память — нужно, чтобы повторять запрос на
    // следующем ключе при failover. Запросы Claude Code небольшие.
    let body_bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|e| anyhow::anyhow!("не удалось прочитать тело запроса: {e}"))?;

    let client = crate::http_client();
    let target_url = build_target_url(&state.config.upstream, &uri);

    let key_count = state.key_count();
    let mut attempt = 0usize;
    // Начинаем с текущего активного ключа.
    let mut key_index = state.active_index();

    // Максимум попыток = число ключей. Защита от бесконечного цикла.
    loop {
        attempt += 1;

        let outcome = forward_once(
            &client,
            &state,
            key_index,
            &method,
            &target_url,
            &headers,
            &body_bytes,
        )
        .await?;

        match outcome {
            Forward::Pass(resp) => return Ok(resp),
            Forward::Exhausted { status } => {
                // Лимит на этом ключе. Если попытки кончились — отдаём как есть.
                if attempt >= key_count {
                    tracing::warn!(
                        key = key_index,
                        status = status,
                        "все ключи исчерпаны, отдаю исходную ошибку клиенту"
                    );
                    return Ok(exhausted_response(status));
                }
                // Сдвигаем кольцо и берём следующий активный ключ.
                let next = state.rotate_from(key_index);
                tracing::info!(
                    key = key_index,
                    status = status,
                    failover = true,
                    switched_to = next,
                    "failover: переключение ключа"
                );
                key_index = next;
            }
        }
    }
}

/// Результат одной попытки форварда.
enum Forward {
    /// Ответ нужно отдать клиенту как есть.
    Pass(Response),
    /// Ключ исчерпан — нужен failover.
    Exhausted { status: u16 },
}

#[allow(clippy::too_many_arguments)]
async fn forward_once(
    client: &reqwest::Client,
    state: &AppState,
    key_index: usize,
    method: &Method,
    target_url: &str,
    in_headers: &HeaderMap,
    body_bytes: &Bytes,
) -> anyhow::Result<Forward> {
    let key = state.key_at(key_index).to_string();
    let auth = state.config.auth_header;

    // Собираем заголовки: копируем входящие, чистим hop-by-hop и старую авторизацию.
    let mut req_headers = HeaderMap::new();
    for (name, value) in in_headers.iter() {
        if is_hop_by_hop(name) {
            continue;
        }
        // Убираем любые входящие авторизационные заголовки — поставим свой.
        let lname = name.as_str();
        if lname == "x-api-key" || lname == "authorization" {
            continue;
        }
        req_headers.insert(name.clone(), value.clone());
    }
    // Ставим актуальный ключ в нужный заголовок.
    let header_name = HeaderName::from_static(match auth {
        crate::config::AuthHeader::XApiKey => "x-api-key",
        crate::config::AuthHeader::Authorization => "authorization",
    });
    let header_val = HeaderValue::from_str(&auth.header_value(&key))
        .map_err(|e| anyhow::anyhow!("некорректное значение auth-заголовка: {e}"))?;
    req_headers.insert(header_name, header_val);

    let upstream_method = reqwest::Method::from_bytes(method.as_str().as_bytes())
        .map_err(|e| anyhow::anyhow!("некорректный метод: {e}"))?;

    state.record_request(key_index);

    let resp = client
        .request(upstream_method, target_url)
        .headers(req_headers)
        .body(body_bytes.clone())
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("upstream-запрос не удался: {e}"))?;

    let status = resp.status();
    let status_u16 = status.as_u16();

    // Статусы из failover_statuses (401/402/403/429 по умолчанию) → failover.
    if state.config.failover_statuses.contains(&status_u16) {
        tracing::info!(
            key = key_index,
            status = status_u16,
            "ответ upstream: статус failover, переключаю ключ"
        );
        return Ok(Forward::Exhausted { status: status_u16 });
    }

    let is_stream = is_event_stream(resp.headers());

    if is_stream {
        // Стриминговый ответ. Подсматриваем ТОЛЬКО первый чанк, чтобы поймать
        // ранний error-event с маркером лимита, не буферизуя весь поток.
        handle_stream_response(state, key_index, status, resp).await
    } else {
        // Не-стрим: буферизуем тело (ошибки короткие) и ищем маркер.
        handle_buffered_response(state, key_index, status, resp).await
    }
}

/// Не-стримовый ответ: читаем тело целиком, ищем маркер лимита.
async fn handle_buffered_response(
    state: &AppState,
    key_index: usize,
    status: reqwest::StatusCode,
    resp: reqwest::Response,
) -> anyhow::Result<Forward> {
    let resp_headers = resp.headers().clone();
    let body = resp
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("не удалось прочитать тело ответа upstream: {e}"))?;

    let status_u16 = status.as_u16();

    // Маркер ищем только в относительно небольших телах (по сути — в ошибках).
    if body.len() <= MAX_BUFFER_FOR_MARKER && body_contains_marker(&body, &state.config.exhaustion_markers)
    {
        tracing::info!(
            key = key_index,
            status = status_u16,
            "ответ upstream: маркер лимита в теле"
        );
        return Ok(Forward::Exhausted { status: status_u16 });
    }

    tracing::info!(key = key_index, status = status_u16, "ответ upstream: проброс");
    let out = build_response(status, &resp_headers, Body::from(body));
    Ok(Forward::Pass(out))
}

/// Стримовый ответ: подсматриваем первый чанк, остальное проксируем потоком.
async fn handle_stream_response(
    state: &AppState,
    key_index: usize,
    status: reqwest::StatusCode,
    resp: reqwest::Response,
) -> anyhow::Result<Forward> {
    let resp_headers = resp.headers().clone();
    let status_u16 = status.as_u16();
    let markers = state.config.exhaustion_markers.clone();

    let mut upstream = resp.bytes_stream();

    // Берём первый чанк, чтобы поймать ранний error-event лимита.
    let first = match upstream.next().await {
        Some(Ok(chunk)) => Some(chunk),
        Some(Err(e)) => return Err(anyhow::anyhow!("ошибка чтения стрима upstream: {e}")),
        None => None, // пустой ответ
    };

    if let Some(ref chunk) = first {
        if body_contains_marker(chunk, &markers) {
            tracing::info!(
                key = key_index,
                status = status_u16,
                "ответ upstream: маркер лимита в начале стрима"
            );
            return Ok(Forward::Exhausted { status: status_u16 });
        }
    }

    tracing::info!(
        key = key_index,
        status = status_u16,
        stream = true,
        "ответ upstream: проброс стрима"
    );

    // Собираем выходной поток: сначала подсмотренный чанк, затем хвост upstream.
    let head = futures_util::stream::iter(first.map(Ok::<Bytes, std::io::Error>));
    let tail = upstream.map(|res| {
        res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    });
    let combined = head.chain(tail);

    let out = build_response(status, &resp_headers, Body::from_stream(combined));
    Ok(Forward::Pass(out))
}

/// Определить, является ли ответ SSE/стримом по content-type.
fn is_event_stream(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("text/event-stream"))
        .unwrap_or(false)
}

/// Проверить наличие любого из маркеров в байтах тела.
fn body_contains_marker(body: &[u8], markers: &[String]) -> bool {
    if markers.is_empty() {
        return false;
    }
    // Маркеры — ASCII/UTF-8 подстроки. Ищем по сырым байтам через lossy-строку.
    let text = String::from_utf8_lossy(body);
    markers.iter().any(|m| !m.is_empty() && text.contains(m.as_str()))
}

/// Построить целевой URL: upstream + path + query.
fn build_target_url(upstream: &str, uri: &Uri) -> String {
    let path = uri.path();
    match uri.query() {
        Some(q) => format!("{upstream}{path}?{q}"),
        None => format!("{upstream}{path}"),
    }
}

/// Собрать axum-ответ из upstream-статуса, заголовков и тела.
fn build_response(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
    body: Body,
) -> Response {
    let mut builder = Response::builder().status(status.as_u16());
    for (name, value) in headers.iter() {
        // axum/hyper сами выставят transfer-encoding/content-length для тела.
        let n = name.as_str();
        if n == "transfer-encoding" || n == "content-length" || n == "connection" {
            continue;
        }
        if let (Ok(hn), Ok(hv)) = (
            HeaderName::from_bytes(name.as_str().as_bytes()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) {
            builder = builder.header(hn, hv);
        }
    }
    builder.body(body).unwrap_or_else(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, "failed to build response").into_response()
    })
}

/// Ответ, когда все ключи исчерпаны: отдаём исходный статус лимита.
fn exhausted_response(status: u16) -> Response {
    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::PAYMENT_REQUIRED);
    (
        code,
        Json(serde_json::json!({
            "type": "error",
            "error": {
                "type": "all_keys_exhausted",
                "message": "Все API-ключи исчерпали лимит (proxy ring rotation)."
            }
        })),
    )
        .into_response()
}
