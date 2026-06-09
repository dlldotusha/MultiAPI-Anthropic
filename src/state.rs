use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::config::Config;

/// Метрики по одному ключу.
#[derive(Debug)]
struct KeyMetrics {
    requests: AtomicU64,
    failovers: AtomicU64,
}

impl KeyMetrics {
    fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            failovers: AtomicU64::new(0),
        }
    }
}

/// Общее состояние прокси: пул ключей как кольцо + метрики.
///
/// Ротация — кольцевая, без persistent-exhausted и без cooldown.
/// «Активный» ключ хранится как atomic-индекс. Гонка over-rotation
/// (когда N параллельных запросов одновременно ловят 402 на одном ключе
/// и каждый двигает указатель, перепрыгивая рабочие ключи) решается
/// через CAS: запрос двигает указатель ТОЛЬКО если активный ключ всё ещё
/// тот, на котором он получил отказ. Иначе он просто берёт уже обновлённый
/// активный ключ и повторяет — указатель сдвигается максимум на 1.
#[derive(Debug)]
pub struct AppState {
    pub config: Config,
    /// Текущий активный индекс в кольце [0, keys.len()).
    active: AtomicUsize,
    per_key: Vec<KeyMetrics>,
    requests_total: AtomicU64,
    failovers_total: AtomicU64,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        let per_key = (0..config.keys.len()).map(|_| KeyMetrics::new()).collect();
        Self {
            config,
            active: AtomicUsize::new(0),
            per_key,
            requests_total: AtomicU64::new(0),
            failovers_total: AtomicU64::new(0),
        }
    }

    pub fn key_count(&self) -> usize {
        self.config.keys.len()
    }

    /// Текущий активный индекс.
    pub fn active_index(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    /// Ключ по индексу.
    pub fn key_at(&self, index: usize) -> &str {
        &self.config.keys[index]
    }

    /// Учесть отправленный запрос (на конкретном ключе).
    pub fn record_request(&self, index: usize) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        self.per_key[index].requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Сдвинуть кольцо вперёд, если активный ключ всё ещё `from`.
    ///
    /// Реализует «key -> 402 -> в конец очереди, следующий становится активным».
    /// Поскольку кольцо круговое, «перенос в конец» == переход указателя на
    /// `(from + 1) % n`; через n шагов мы снова вернёмся к исходному ключу.
    ///
    /// Возвращает индекс ключа, который стал/является активным ПОСЛЕ вызова —
    /// это следующий ключ для повторной попытки. CAS гарантирует, что
    /// конкурентные отказы на одном `from` сдвинут указатель ровно на 1.
    pub fn rotate_from(&self, from: usize) -> usize {
        let next = (from + 1) % self.key_count();
        match self.active.compare_exchange(
            from,
            next,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            // Мы сдвинули указатель: считаем это событием failover.
            Ok(_) => {
                self.failovers_total.fetch_add(1, Ordering::Relaxed);
                self.per_key[from].failovers.fetch_add(1, Ordering::Relaxed);
                next
            }
            // Кто-то уже сдвинул указатель — берём актуальное значение,
            // не двигаем дальше (иначе перепрыгнем рабочий ключ).
            Err(current) => current,
        }
    }

    /// Снимок метрик для GET /proxy/status.
    pub fn snapshot(&self) -> StatusSnapshot {
        let per_key = self
            .per_key
            .iter()
            .enumerate()
            .map(|(index, m)| KeyStatus {
                index,
                requests: m.requests.load(Ordering::Relaxed),
                failovers: m.failovers.load(Ordering::Relaxed),
            })
            .collect();

        StatusSnapshot {
            active_key_index: self.active_index(),
            total_keys: self.key_count(),
            requests_total: self.requests_total.load(Ordering::Relaxed),
            failovers_total: self.failovers_total.load(Ordering::Relaxed),
            per_key,
        }
    }
}

/// Снимок состояния для JSON-ответа /proxy/status.
#[derive(Debug, serde::Serialize)]
pub struct StatusSnapshot {
    pub active_key_index: usize,
    pub total_keys: usize,
    pub requests_total: u64,
    pub failovers_total: u64,
    pub per_key: Vec<KeyStatus>,
}

#[derive(Debug, serde::Serialize)]
pub struct KeyStatus {
    pub index: usize,
    pub requests: u64,
    pub failovers: u64,
}
