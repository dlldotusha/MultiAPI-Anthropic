use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::path::Path;

/// Какой заголовок использовать для авторизации на upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthHeader {
    /// Anthropic-стиль: `x-api-key: <KEY>`
    XApiKey,
    /// OpenAI-стиль: `authorization: Bearer <KEY>`
    Authorization,
}

impl AuthHeader {
    pub fn header_name(self) -> &'static str {
        match self {
            AuthHeader::XApiKey => "x-api-key",
            AuthHeader::Authorization => "authorization",
        }
    }

    /// Сформировать значение заголовка для данного ключа.
    pub fn header_value(self, key: &str) -> String {
        match self {
            AuthHeader::XApiKey => key.to_string(),
            AuthHeader::Authorization => format!("Bearer {key}"),
        }
    }
}

/// Конфиг как он лежит в YAML (сырой, до раскрытия ${ENV}).
#[derive(Debug, Deserialize)]
struct RawConfig {
    listen: String,
    upstream: String,
    #[serde(default = "default_auth_header")]
    auth_header: String,
    keys: Vec<String>,
    #[serde(default = "default_markers")]
    exhaustion_markers: Vec<String>,
    #[serde(default = "default_failover_statuses")]
    failover_statuses: Vec<u16>,
}

fn default_auth_header() -> String {
    "x-api-key".to_string()
}

fn default_markers() -> Vec<String> {
    vec!["Usage limit reached".to_string()]
}

/// HTTP-статусы upstream, при которых пробуем следующий ключ.
/// 401 — невалидный/протухший ключ, 402 — оплата/лимит,
/// 403 — доступ запрещён, 429 — rate limit.
fn default_failover_statuses() -> Vec<u16> {
    vec![401, 402, 403, 429]
}

/// Готовый к использованию конфиг.
#[derive(Debug, Clone)]
pub struct Config {
    pub listen: String,
    /// upstream без завершающего слэша.
    pub upstream: String,
    pub auth_header: AuthHeader,
    pub keys: Vec<String>,
    pub exhaustion_markers: Vec<String>,
    pub failover_statuses: Vec<u16>,
}

impl Config {
    /// Загрузить и провалидировать конфиг из YAML-файла.
    pub fn load(path: impl AsRef<Path>) -> Result<Config> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("не удалось прочитать конфиг {}", path.display()))?;
        let raw: RawConfig = serde_yaml::from_str(&text)
            .with_context(|| format!("не удалось разобрать YAML {}", path.display()))?;
        Config::from_raw(raw)
    }

    fn from_raw(raw: RawConfig) -> Result<Config> {
        let auth_header = match raw.auth_header.to_ascii_lowercase().as_str() {
            "x-api-key" => AuthHeader::XApiKey,
            "authorization" => AuthHeader::Authorization,
            other => bail!("auth_header должен быть 'x-api-key' или 'authorization', получено '{other}'"),
        };

        // Раскрываем ${VAR} из окружения и отбрасываем пустые ключи.
        let mut keys = Vec::with_capacity(raw.keys.len());
        for (i, k) in raw.keys.iter().enumerate() {
            let expanded = expand_env(k)
                .with_context(|| format!("ключ #{i} не удалось раскрыть"))?;
            let expanded = expanded.trim().to_string();
            if expanded.is_empty() {
                bail!("ключ #{i} пустой после раскрытия ${{ENV}}");
            }
            keys.push(expanded);
        }

        if keys.is_empty() {
            bail!("не задан ни один ключ в `keys`");
        }

        let upstream = raw.upstream.trim_end_matches('/').to_string();
        if upstream.is_empty() {
            bail!("`upstream` не задан");
        }

        Ok(Config {
            listen: raw.listen,
            upstream,
            auth_header,
            keys,
            exhaustion_markers: raw.exhaustion_markers,
            failover_statuses: raw.failover_statuses,
        })
    }
}

/// Раскрыть `${VAR}` в строке значением из окружения.
///
/// Поддерживается весь синтаксис `${VAR}` в любом месте строки.
/// Если переменной нет в окружении — ошибка (чтобы не уехать в проде с пустым ключом).
/// Строки без `${` возвращаются как есть (ключ указан напрямую).
fn expand_env(input: &str) -> Result<String> {
    if !input.contains("${") {
        return Ok(input.to_string());
    }

    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .context("незакрытая ${...} в значении ключа")?;
        let var_name = &after[..end];
        let value = std::env::var(var_name)
            .with_context(|| format!("переменная окружения '{var_name}' не установлена"))?;
        out.push_str(&value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}
