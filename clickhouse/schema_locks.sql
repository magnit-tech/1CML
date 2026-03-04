-- =====================================================
-- Схема данных для анализа блокировок и дедлоков в 1С
-- ClickHouse
-- =====================================================

-- Создание базы данных
CREATE DATABASE IF NOT EXISTS techlog;

USE techlog;

-- =====================================================
-- 1. Таблица для сырых событий блокировок
-- =====================================================

CREATE TABLE IF NOT EXISTS lock_events (
    -- Временные метки
    event_date Date,
    event_hour UInt8,
    event_minute UInt8,
    event_datetime DateTime,
    
    -- Тип события
    event_type String,  -- LOCK, DEADLOCK, TTIMEOUT, SDBL
    
    -- Данные сессии
    session_id UInt64,
    transaction_id UInt64,
    user_name String,
    process_name String,
    
    -- Данные блокировки
    table_name String,
    lock_type String,   -- Row, Page, Table
    lock_mode String,   -- S, X, IS, IX, SIX
    lock_name String,
    
    -- Метрики
    lock_wait_time UInt64,  -- время ожидания в микросекундах
    lock_time UInt64,       -- время удержания в микросекундах
    dbpid UInt32,           -- PID в СУБД
    
    -- Дополнительная информация
    raw_line String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_hour, table_name, event_type)
TTL event_date + INTERVAL 3 MONTH
SETTINGS index_granularity = 8192;

-- =====================================================
-- 2. Таблица для агрегированной статистики по часам
-- =====================================================

CREATE TABLE IF NOT EXISTS lock_hourly_stats (
    event_date Date,
    event_hour UInt8,
    
    -- Общая статистика
    total_lock_events UInt64,
    total_deadlocks UInt64,
    total_timeouts UInt64,
    
    -- Временные характеристики
    avg_lock_wait_time Float64,
    max_lock_wait_time UInt64,
    p95_lock_wait_time Float64,
    min_lock_wait_time UInt64,
    
    avg_lock_time Float64,
    max_lock_time UInt64,
    
    -- Статистика по таблицам
    unique_tables UInt64,
    top_table String,  -- таблица с наибольшим числом блокировок
    
    -- Активность
    unique_sessions UInt64,
    unique_transactions UInt64,
    unique_users UInt64,
    
    -- Риск-метрики
    lock_intensity Float64,  -- блокировок в минуту
    deadlock_rate Float64,   -- дедлоков в час
    
    -- Флаги
    is_weekend UInt8,
    is_work_hour UInt8
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_hour);

-- =====================================================
-- 3. Таблица для статистики по конкретным таблицам
-- =====================================================

CREATE TABLE IF NOT EXISTS lock_table_stats (
    event_date Date,
    table_name String,
    
    lock_count UInt64,
    deadlock_count UInt64,
    timeout_count UInt64,
    
    avg_lock_wait_time Float64,
    max_lock_wait_time UInt64,
    p95_lock_wait_time Float64,
    
    avg_lock_time Float64,
    max_lock_time UInt64,
    
    unique_sessions UInt64,
    unique_transactions UInt64,
    unique_users UInt64,
    
    -- Для трендов
    growth_rate Float64 DEFAULT 0  -- скорость роста блокировок по дням
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, table_name);

-- =====================================================
-- 4. Таблица для статистики по пользователям
-- =====================================================

CREATE TABLE IF NOT EXISTS lock_user_stats (
    event_date Date,
    user_name String,
    
    lock_count UInt64,
    deadlock_count UInt64,
    timeout_count UInt64,
    
    avg_lock_wait_time Float64,
    max_lock_wait_time UInt64,
    
    unique_tables UInt64,
    unique_sessions UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_name);

-- =====================================================
-- 5. Таблица для обнаруженных аномалий
-- =====================================================

CREATE TABLE IF NOT EXISTS lock_anomalies (
    event_date Date,
    detected_at DateTime,
    
    anomaly_type String,  -- deadlock_spike, wait_time_spike, trend_alert
    severity String,      -- info, warning, critical
    
    description String,
    
    -- Метрики на момент обнаружения
    current_deadlocks UInt64,
    current_avg_wait UInt64,
    baseline_avg_wait UInt64,
    deviation_factor Float64,
    
    -- Затронутые объекты
    affected_tables String,
    affected_users String,
    
    -- Рекомендация
    recommendation String,
    
    -- Статус
    is_resolved UInt8 DEFAULT 0,
    resolved_at DateTime,
    jira_ticket String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, detected_at);

-- =====================================================
-- Материализованные представления для автоматической агрегации
-- =====================================================

-- 1. Почасовая агрегация
CREATE MATERIALIZED VIEW IF NOT EXISTS lock_hourly_stats_mv
TO lock_hourly_stats
AS SELECT
    event_date,
    event_hour,
    count() as total_lock_events,
    countIf(event_type = 'DEADLOCK') as total_deadlocks,
    countIf(event_type = 'TTIMEOUT') as total_timeouts,
    avg(lock_wait_time) as avg_lock_wait_time,
    max(lock_wait_time) as max_lock_wait_time,
    quantile(0.95)(lock_wait_time) as p95_lock_wait_time,
    min(lock_wait_time) as min_lock_wait_time,
    avg(lock_time) as avg_lock_time,
    max(lock_time) as max_lock_time,
    uniq(table_name) as unique_tables,
    argMax(table_name, lock_count) as top_table,
    uniq(session_id) as unique_sessions,
    uniq(transaction_id) as unique_transactions,
    uniq(user_name) as unique_users,
    count() / 60.0 as lock_intensity,
    countIf(event_type = 'DEADLOCK') / 1.0 as deadlock_rate,
    CASE WHEN toDayOfWeek(event_date) IN (6, 7) THEN 1 ELSE 0 END as is_weekend,
    CASE WHEN event_hour BETWEEN 9 AND 18 THEN 1 ELSE 0 END as is_work_hour
FROM lock_events
GROUP BY event_date, event_hour;

-- 2. Агрегация по таблицам
CREATE MATERIALIZED VIEW IF NOT EXISTS lock_table_stats_mv
TO lock_table_stats
AS SELECT
    event_date,
    table_name,
    count() as lock_count,
    countIf(event_type = 'DEADLOCK') as deadlock_count,
    countIf(event_type = 'TTIMEOUT') as timeout_count,
    avg(lock_wait_time) as avg_lock_wait_time,
    max(lock_wait_time) as max_lock_wait_time,
    quantile(0.95)(lock_wait_time) as p95_lock_wait_time,
    avg(lock_time) as avg_lock_time,
    max(lock_time) as max_lock_time,
    uniq(session_id) as unique_sessions,
    uniq(transaction_id) as unique_transactions,
    uniq(user_name) as unique_users,
    0 as growth_rate
FROM lock_events
WHERE table_name != ''
GROUP BY event_date, table_name;

-- 3. Агрегация по пользователям
CREATE MATERIALIZED VIEW IF NOT EXISTS lock_user_stats_mv
TO lock_user_stats
AS SELECT
    event_date,
    user_name,
    count() as lock_count,
    countIf(event_type = 'DEADLOCK') as deadlock_count,
    countIf(event_type = 'TTIMEOUT') as timeout_count,
    avg(lock_wait_time) as avg_lock_wait_time,
    max(lock_wait_time) as max_lock_wait_time,
    uniq(table_name) as unique_tables,
    uniq(session_id) as unique_sessions
FROM lock_events
WHERE user_name != ''
GROUP BY event_date, user_name;

-- =====================================================
-- 6. Таблица для хранения порогов и настроек
-- =====================================================

CREATE TABLE IF NOT EXISTS lock_thresholds (
    threshold_name String,
    threshold_value Float64,
    threshold_description String,
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY threshold_name;

-- Вставляем пороги по умолчанию
INSERT INTO lock_thresholds (threshold_name, threshold_value, threshold_description) VALUES
('deadlock_warning', 1, 'Появление любого deadlock - уже проблема'),
('timeout_warning', 10, 'Более 10 таймаутов в час - внимание'),
('wait_time_warning', 500000, 'Среднее время ожидания > 500 мс - предупреждение'),
('wait_time_critical', 1000000, 'Среднее время ожидания > 1 с - критично'),
('trend_warning', 50, 'Рост блокировок на 50% за неделю - внимание'),
('trend_critical', 100, 'Рост блокировок на 100% за неделю - критично'),
('lock_intensity_warning', 1000, 'Более 1000 блокировок в минуту - высокая нагрузка');

-- =====================================================
-- Полезные запросы для анализа (как комментарии)
-- =====================================================

/*
-- 1. Динамика дедлоков по дням
SELECT 
    event_date,
    countIf(event_type = 'DEADLOCK') as deadlocks,
    countIf(event_type = 'TTIMEOUT') as timeouts
FROM lock_events
WHERE event_date >= today() - 30
GROUP BY event_date
ORDER BY event_date;

-- 2. Топ-20 таблиц по блокировкам за сегодня
SELECT 
    table_name,
    count() as locks,
    countIf(event_type = 'DEADLOCK') as deadlocks,
    avg(lock_wait_time)/1000 as avg_wait_ms
FROM lock_events
WHERE event_date = today() AND table_name != ''
GROUP BY table_name
ORDER BY locks DESC
LIMIT 20;

-- 3. Часовой тренд (для прогноза)
SELECT 
    toStartOfHour(event_datetime) as hour,
    count() as locks,
    countIf(event_type = 'DEADLOCK') as deadlocks,
    avg(lock_wait_time)/1000 as avg_wait_ms
FROM lock_events
WHERE event_datetime >= now() - interval 7 day
GROUP BY hour
ORDER BY hour;

-- 4. Поиск конфликтующих таблиц
SELECT 
    a.table_name as table1,
    b.table_name as table2,
    count() as conflicts
FROM lock_events a
JOIN lock_events b ON a.event_datetime = b.event_datetime 
    AND a.session_id != b.session_id
    AND a.event_type = 'LOCK' 
    AND b.event_type = 'LOCK'
WHERE a.event_date >= today() - 7
  AND a.table_name < b.table_name
GROUP BY table1, table2
ORDER BY conflicts DESC
LIMIT 20;

-- 5. Пользователи-лидеры по блокировкам
SELECT 
    user_name,
    count() as locks,
    countIf(event_type = 'DEADLOCK') as deadlocks,
    uniq(table_name) as tables
FROM lock_events
WHERE event_date >= today() - 7
GROUP BY user_name
ORDER BY locks DESC
LIMIT 20;

-- 6. Скользящее среднее для выявления трендов
WITH daily AS (
    SELECT 
        event_date,
        avg(lock_wait_time) as avg_wait
    FROM lock_events
    GROUP BY event_date
)
SELECT 
    event_date,
    avg_wait/1000 as avg_wait_ms,
    avg(avg_wait) OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)/1000 as trend_7d
FROM daily
ORDER BY event_date;

-- 7. Корреляция между временем и количеством
SELECT 
    toHour(event_datetime) as hour,
    avg(lock_wait_time)/1000 as avg_wait_ms,
    count() as locks
FROM lock_events
WHERE event_date >= today() - 7
GROUP BY hour
ORDER BY hour;
*/
