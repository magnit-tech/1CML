-- 1. Динамика блокировок по дням
SELECT 
    event_date,
    count() as total_locks,
    countIf(event_type = 'DEADLOCK') as deadlocks,
    countIf(event_type = 'TTIMEOUT') as timeouts,
    avg(lock_wait_time) as avg_wait,
    max(lock_wait_time) as max_wait
FROM lock_events
WHERE event_date >= today() - 30
GROUP BY event_date
ORDER BY event_date;

-- 2. Топ-20 таблиц по блокировкам за сегодня
SELECT 
    table_name,
    count() as lock_count,
    countIf(event_type = 'DEADLOCK') as deadlocks,
    countIf(event_type = 'TTIMEOUT') as timeouts,
    avg(lock_wait_time) as avg_wait_ms,
    max(lock_wait_time) / 1000 as max_wait_ms
FROM lock_events
WHERE event_date = today()
  AND table_name != ''
GROUP BY table_name
ORDER BY lock_count DESC
LIMIT 20;

-- 3. Часовой тренд блокировок (для прогноза)
SELECT 
    toStartOfHour(event_datetime) as hour,
    count() as locks,
    countIf(event_type = 'DEADLOCK') as deadlocks,
    avg(lock_wait_time) as avg_wait,
    max(lock_wait_time) as max_wait
FROM lock_events
WHERE event_datetime >= now() - interval 7 day
GROUP BY hour
ORDER BY hour;

-- 4. Поиск паттернов дедлоков (какие таблицы участвуют)
SELECT 
    a.table_name as table1,
    b.table_name as table2,
    count() as deadlock_pairs
FROM lock_events a
JOIN lock_events b ON a.event_datetime = b.event_datetime 
    AND a.session_id != b.session_id
    AND a.event_type = 'DEADLOCK' 
    AND b.event_type = 'DEADLOCK'
WHERE a.event_date >= today() - 7
  AND a.table_name < b.table_name
GROUP BY table1, table2
ORDER BY deadlock_pairs DESC;

-- 5. Корреляция между активностью и блокировками
SELECT 
    toHour(event_datetime) as hour_of_day,
    avg(lock_wait_time) as avg_wait,
    count() as locks,
    uniq(session_id) as sessions
FROM lock_events
WHERE event_date >= today() - 7
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 6. Скользящее среднее для выявления трендов
SELECT 
    event_date,
    avg(lock_wait_time) as avg_wait,
    avg(avg(lock_wait_time)) OVER (
        ORDER BY event_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as trend_7d
FROM lock_events
GROUP BY event_date
ORDER BY event_date;
