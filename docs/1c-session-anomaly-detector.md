# Как физически работает детектор аномалий сессий 1С
## Полный путь данных: от техжурнала до алерта

```
Техжурнал 1С → Парсер → ClickHouse → Обучение модели → Детектирование → Оповещение
```

---

## 1. Откуда берутся данные о сессиях

### 1.1. Настройка техжурнала 1С для сбора данных о сессиях

**Файл:** `C:\Program Files\1cv8\conf\logcfg.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<config xmlns="http://v8.1c.ru/v8/tech-log">
  <log location="C:\1C_techlog\" history="7">
    <event>
      <ne property="name" value="SESSION" />          <!-- События сессий -->
      <ne property="name" value="CALL" />             <!-- Вызовы -->
      <ne property="name" value="LOCK" />             <!-- Блокировки -->
      <ne property="name" value="DEADLOCK" />         <!-- Дедлоки -->
      <ne property="name" value="TTIMEOUT" />         <!-- Таймауты -->
      <ne property="name" value="EXCP" />             <!-- Исключения -->
    </event>
    <property name="all">
      <ne property="name" value="p:processName" />    <!-- Имя процесса -->
      <ne property="name" value="t:applicationName" /> <!-- Имя приложения -->
      <ne property="name" value="t:userName" />        <!-- Имя пользователя -->
      <ne property="name" value="t:computerName" />    <!-- Имя компьютера -->
      <ne property="name" value="t:session" />         <!-- ID сессии -->
      <ne property="name" value="t:duration" />        <!-- Длительность -->
      <ne property="name" value="t:lockWaitTime" />    <!-- Время ожидания блокировки -->
      <ne property="name" value="t:lockTime" />        <!-- Время удержания блокировки -->
    </property>
  </log>
</config>
```

**Что дает эта настройка:**
- Каждое событие в 1С пишется в лог-файл
- Файлы создаются по часам: `26030413.log`, `26030414.log`, `26030415.log` и т.д.
- Размер одного часа может быть от 10 МБ до 500 МБ в зависимости от активности

### 1.2. Пример строки техжурнала с данными о сессии

```
20260228 10:35:23.456 SESSION,3,1c.exe,processName=rmngr,userName="Иванов И.И.",
computerName="WORKSTATION-42",session=12345678,duration=1250,lockWaitTime=0,lockTime=0
```

**Расшифровка:**
- `20260228 10:35:23.456` — дата и время
- `SESSION` — тип события
- `userName="Иванов И.И."` — кто работал
- `computerName="WORKSTATION-42"` — с какого компьютера
- `session=12345678` — уникальный ID сессии
- `duration=1250` — сколько длилась операция в микросекундах

---

## 2. Парсер техжурнала (загрузка в ClickHouse)

### 2.1. Структура таблицы в ClickHouse

**Файл:** `clickhouse/schema.sql`

```sql
-- Создание базы данных
CREATE DATABASE IF NOT EXISTS techlog;

USE techlog;

-- Таблица для хранения всех событий техжурнала
CREATE TABLE IF NOT EXISTS session_events (
    -- Временные метки
    event_date Date,
    event_hour UInt8,
    event_minute UInt8,
    event_datetime DateTime,
    
    -- Данные сессии
    session_id UInt64,
    user_name String,
    computer_name String,
    process_name String,
    app_name String,
    
    -- Метрики производительности
    duration UInt64,          -- в микросекундах
    lock_wait_time UInt64,    -- время ожидания блокировки
    lock_time UInt64,         -- время удержания блокировки
    
    -- Флаги событий
    is_deadlock UInt8,        -- 1 если был дедлок
    is_exception UInt8,       -- 1 если было исключение
    is_timeout UInt8,         -- 1 если был таймаут
    
    -- Сырая строка для отладки
    raw_line String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_hour, user_name)
TTL event_date + INTERVAL 3 MONTH  -- храним 3 месяца
SETTINGS index_granularity = 8192;

-- Таблица для агрегированных данных по часам (для обучения модели)
CREATE TABLE IF NOT EXISTS session_hourly_stats (
    event_date Date,
    event_hour UInt8,
    
    -- Статистика по сессиям
    total_sessions UInt64,
    unique_users UInt64,
    unique_computers UInt64,
    
    -- Распределение по часам (для паттернов)
    avg_sessions_per_minute Float64,
    max_sessions_per_minute UInt64,
    
    -- Метрики производительности
    avg_duration Float64,
    p95_duration Float64,
    max_duration UInt64,
    
    avg_lock_wait_time Float64,
    total_lock_wait_time UInt64,
    
    -- Ошибки и аномалии
    deadlock_count UInt64,
    exception_count UInt64,
    timeout_count UInt64,
    
    -- Флаг выходного/рабочего дня
    is_weekend UInt8,
    is_work_hour UInt8
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_hour);

-- Материализованное представление для автоматической агрегации
CREATE MATERIALIZED VIEW session_hourly_stats_mv
TO session_hourly_stats
AS SELECT
    event_date,
    event_hour,
    count() as total_sessions,
    uniq(user_name) as unique_users,
    uniq(computer_name) as unique_computers,
    avg(duration) as avg_duration,
    quantile(0.95)(duration) as p95_duration,
    max(duration) as max_duration,
    avg(lock_wait_time) as avg_lock_wait_time,
    sum(lock_wait_time) as total_lock_wait_time,
    sum(is_deadlock) as deadlock_count,
    sum(is_exception) as exception_count,
    sum(is_timeout) as timeout_count
FROM session_events
GROUP BY event_date, event_hour;
```

### 2.2. Скрипт парсера техжурнала

**Файл:** `scripts/techlog_parser.py` (полная версия)

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Парсер техжурнала 1С в ClickHouse
Версия: 2.0 с поддержкой сессий
"""

import os
import re
import gzip
import glob
from datetime import datetime, timedelta
import logging
from pathlib import Path
import time
from typing import List, Dict, Optional
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/techlog_parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('techlog_parser')

class TechLogParser:
    """Парсер техжурнала 1С с поддержкой данных о сессиях"""
    
    # Регулярные выражения для извлечения полей
    PATTERNS = {
        # Основные поля
        'datetime': re.compile(r'^(\d{4})(\d{2})(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3})'),
        'event_type': re.compile(r',([A-Z_]+),'),
        
        # Данные сессии
        'session': re.compile(r'session=(\d+)'),
        'user': re.compile(r'userName="([^"]*)"'),
        'computer': re.compile(r'computerName="([^"]*)"'),
        'process': re.compile(r'processName=([^,]+)'),
        'app': re.compile(r'app-id="([^"]*)"'),
        
        # Метрики производительности
        'duration': re.compile(r'duration=(\d+)'),
        'lock_wait': re.compile(r'lockWaitTime=(\d+)'),
        'lock_time': re.compile(r'lockTime=(\d+)'),
        
        # Флаги событий
        'deadlock': re.compile(r'deadlock', re.IGNORECASE),
        'exception': re.compile(r'exception|error', re.IGNORECASE),
        'timeout': re.compile(r'ttimeout', re.IGNORECASE),
        
        # Контекст
        'connection': re.compile(r'connection=(\d+)'),
        'transaction': re.compile(r'transaction=(\d+)'),
        'dbms': re.compile(r'dbms="([^"]*)"'),
        'func': re.compile(r'func="([^"]*)"'),
    }
    
    def __init__(self, host='localhost', port=9000, database='techlog'):
        """
        Инициализация парсера
        
        Args:
            host: хост ClickHouse
            port: порт ClickHouse
            database: имя базы данных
        """
        self.client = Client(
            host=host,
            port=port,
            database=database,
            settings={'insert_quorum': 1, 'insert_quorum_timeout': 60000}
        )
        self.database = database
        self.batch_size = 10000  # вставляем по 10 тысяч записей
        self.stats = {
            'processed_files': 0,
            'processed_lines': 0,
            'inserted_rows': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        # Проверяем подключение
        self._test_connection()
    
    def _test_connection(self):
        """Проверка подключения к ClickHouse"""
        try:
            self.client.execute('SELECT 1')
            logger.info(f"Подключение к ClickHouse установлено: {self.client.connection.host}:{self.client.connection.port}")
        except Exception as e:
            logger.error(f"Ошибка подключения к ClickHouse: {e}")
            raise
    
    def parse_line(self, line: str) -> Optional[Dict]:
        """
        Парсинг одной строки техжурнала
        
        Args:
            line: строка лога
            
        Returns:
            словарь с данными или None при ошибке
        """
        try:
            line = line.strip()
            if not line:
                return None
            
            # Парсим дату и время
            dt_match = self.PATTERNS['datetime'].search(line)
            if not dt_match:
                return None
            
            year, month, day, hour, minute, second, ms = map(int, dt_match.groups())
            event_datetime = datetime(year, month, day, hour, minute, second, ms * 1000)
            
            # Парсим тип события
            event_type_match = self.PATTERNS['event_type'].search(line)
            event_type = event_type_match.group(1) if event_type_match else 'UNKNOWN'
            
            # Извлекаем все поля
            result = {
                'event_date': event_datetime.date(),
                'event_hour': hour,
                'event_minute': minute,
                'event_datetime': event_datetime,
                'event_type': event_type,
                'raw_line': line[:1000]  # ограничим длину
            }
            
            # Извлекаем данные сессии
            for key, pattern in self.PATTERNS.items():
                if key in ['datetime', 'event_type']:
                    continue
                
                match = pattern.search(line)
                if match:
                    value = match.group(1) if pattern.groups else match.group(0)
                    
                    # Преобразование типов
                    if key in ['session', 'duration', 'lock_wait', 'lock_time', 
                              'connection', 'transaction']:
                        try:
                            value = int(value)
                        except ValueError:
                            continue
                    elif key in ['deadlock', 'exception', 'timeout']:
                        value = 1  # флаг
                    
                    result[key] = value
            
            # Устанавливаем значения по умолчанию для отсутствующих полей
            for field in ['session', 'user', 'computer', 'duration', 'lock_wait', 
                         'lock_time', 'deadlock', 'exception', 'timeout']:
                if field not in result:
                    if field in ['deadlock', 'exception', 'timeout']:
                        result[field] = 0
                    elif field in ['duration', 'lock_wait', 'lock_time']:
                        result[field] = 0
                    elif field == 'session':
                        result[field] = 0
                    else:
                        result[field] = ''
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка парсинга строки: {line[:200]}... - {e}")
            self.stats['errors'] += 1
            return None
    
    def parse_file(self, file_path: str) -> int:
        """
        Парсинг одного файла техжурнала
        
        Args:
            file_path: путь к файлу
            
        Returns:
            количество вставленных записей
        """
        logger.info(f"Парсинг файла: {file_path}")
        file_stats = {'lines': 0, 'inserted': 0}
        
        # Определяем открывалку для gz или обычного файла
        open_func = gzip.open if file_path.endswith('.gz') else open
        mode = 'rt' if file_path.endswith('.gz') else 'r'
        
        batch = []
        
        try:
            with open_func(file_path, mode, encoding='utf-8', errors='ignore') as f:
                for line in f:
                    file_stats['lines'] += 1
                    
                    parsed = self.parse_line(line)
                    if parsed:
                        batch.append(parsed)
                        file_stats['inserted'] += 1
                        
                        # Вставляем батч
                        if len(batch) >= self.batch_size:
                            self._insert_batch(batch)
                            batch = []
                            
                            # Логируем прогресс
                            if file_stats['inserted'] % 100000 == 0:
                                logger.info(f"  Обработано {file_stats['inserted']} записей")
                
                # Вставляем остаток
                if batch:
                    self._insert_batch(batch)
            
            # Обновляем статистику
            self.stats['processed_files'] += 1
            self.stats['processed_lines'] += file_stats['lines']
            self.stats['inserted_rows'] += file_stats['inserted']
            
            elapsed = time.time() - self.stats['start_time']
            logger.info(f"Файл обработан: {file_stats['inserted']}/{file_stats['lines']} записей, "
                       f"всего: {self.stats['inserted_rows']}, ошибок: {self.stats['errors']}, "
                       f"скорость: {self.stats['inserted_rows']/elapsed:.0f} записей/сек")
            
            return file_stats['inserted']
            
        except Exception as e:
            logger.error(f"Ошибка обработки файла {file_path}: {e}")
            self.stats['errors'] += 1
            return 0
    
    def _insert_batch(self, batch: List[Dict]):
        """
        Вставка батча в ClickHouse
        
        Args:
            batch: список словарей с данными
        """
        if not batch:
            return
        
        try:
            # Подготовка данных для вставки
            data = []
            for record in batch:
                row = [
                    record['event_date'],
                    record['event_hour'],
                    record['event_minute'],
                    record['event_datetime'],
                    record['session'],
                    record['user'][:100],  # ограничим длину
                    record['computer'][:100],
                    record.get('process', '')[:50],
                    record.get('app', '')[:50],
                    record['duration'],
                    record['lock_wait'],
                    record['lock_time'],
                    record['deadlock'],
                    record['exception'],
                    record['timeout'],
                    record['raw_line']
                ]
                data.append(row)
            
            # Вставка
            self.client.execute(
                """
                INSERT INTO session_events (
                    event_date, event_hour, event_minute, event_datetime,
                    session_id, user_name, computer_name, process_name, app_name,
                    duration, lock_wait_time, lock_time,
                    is_deadlock, is_exception, is_timeout,
                    raw_line
                ) VALUES
                """,
                data
            )
            
            logger.debug(f"Вставлено {len(batch)} записей")
            
        except ClickHouseError as e:
            logger.error(f"Ошибка вставки в ClickHouse: {e}")
            self.stats['errors'] += len(batch)
            raise
    
    def process_directory(self, directory: str, pattern: str = '*.log', 
                         recursive: bool = True, max_workers: int = 4):
        """
        Обработка директории с файлами техжурнала
        
        Args:
            directory: путь к директории
            pattern: паттерн файлов
            recursive: рекурсивный обход
            max_workers: количество потоков
        """
        path = Path(directory)
        if not path.exists():
            logger.error(f"Директория не существует: {directory}")
            return
        
        # Собираем все файлы
        if recursive:
            files = list(path.rglob(pattern))
            files.extend(list(path.rglob(pattern + '.gz')))
        else:
            files = list(path.glob(pattern))
            files.extend(list(path.glob(pattern + '.gz')))
        
        logger.info(f"Найдено {len(files)} файлов для обработки")
        
        if not files:
            logger.warning("Нет файлов для обработки")
            return
        
        # Обрабатываем файлы в несколько потоков
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.parse_file, str(f)): f for f in files}
            
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Ошибка в {file_path}: {e}")
                    self.stats['errors'] += 1
        
        # Итоговая статистика
        elapsed = time.time() - self.stats['start_time']
        logger.info("=" * 60)
        logger.info("ИТОГОВАЯ СТАТИСТИКА:")
        logger.info(f"  Файлов обработано: {self.stats['processed_files']}")
        logger.info(f"  Строк обработано: {self.stats['processed_lines']}")
        logger.info(f"  Записей вставлено: {self.stats['inserted_rows']}")
        logger.info(f"  Ошибок: {self.stats['errors']}")
        logger.info(f"  Время: {elapsed:.0f} сек")
        logger.info(f"  Скорость: {self.stats['inserted_rows']/elapsed:.0f} записей/сек")
        logger.info("=" * 60)

def main():
    parser = argparse.ArgumentParser(description='Парсер техжурнала 1С в ClickHouse')
    parser.add_argument('--dir', required=True, help='Директория с техжурналом')
    parser.add_argument('--pattern', default='*.log', help='Паттерн файлов')
    parser.add_argument('--workers', type=int, default=4, help='Количество потоков')
    parser.add_argument('--host', default='localhost', help='Хост ClickHouse')
    parser.add_argument('--port', type=int, default=9000, help='Порт ClickHouse')
    parser.add_argument('--db', default='techlog', help='База данных')
    parser.add_argument('--create-tables', action='store_true', help='Создать таблицы перед запуском')
    
    args = parser.parse_args()
    
    # Создаем таблицы, если нужно
    if args.create_tables:
        logger.info("Создание таблиц в ClickHouse...")
        # Здесь можно выполнить schema.sql
        
    # Запускаем парсер
    parser = TechLogParser(
        host=args.host,
        port=args.port,
        database=args.db
    )
    
    parser.process_directory(
        directory=args.dir,
        pattern=args.pattern,
        max_workers=args.workers
    )

if __name__ == "__main__":
    main()
```

### 2.3. Запуск парсера

```bash
# Установка зависимостей
pip install clickhouse-driver pandas numpy scikit-learn joblib

# Создание таблиц в ClickHouse
cat clickhouse/schema.sql | docker exec -i clickhouse-server clickhouse-client --multiline

# Запуск парсера (раз в час через планировщик)
python scripts/techlog_parser.py --dir C:\1C_techlog --workers 4

# Или с созданием таблиц
python scripts/techlog_parser.py --dir C:\1C_techlog --create-tables
```

---

## 3. Обучение модели детектора аномалий

### 3.1. Скрипт для извлечения обучающих данных из ClickHouse

**Файл:** `scripts/prepare_training_data.py`

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Подготовка данных для обучения детектора аномалий
Извлечение статистики по сессиям из ClickHouse
"""

import pandas as pd
import numpy as np
from clickhouse_driver import Client
import logging
from datetime import datetime, timedelta
import os
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('prepare_training_data')

class TrainingDataExtractor:
    """Извлечение данных для обучения из ClickHouse"""
    
    def __init__(self, host='localhost', port=9000, database='techlog'):
        self.client = Client(host=host, port=port, database=database)
        logger.info(f"Подключен к ClickHouse: {host}:{port}")
    
    def get_hourly_stats(self, days=30) -> pd.DataFrame:
        """
        Получение почасовой статистики за N дней
        
        Args:
            days: количество дней истории
            
        Returns:
            DataFrame с колонками: date, hour, sessions, users, computers,
            avg_duration, p95_duration, deadlocks, exceptions
        """
        query = f"""
        SELECT
            event_date,
            event_hour,
            total_sessions,
            unique_users,
            unique_computers,
            avg_duration,
            p95_duration,
            deadlock_count,
            exception_count,
            timeout_count,
            CASE 
                WHEN toDayOfWeek(event_date) IN (6, 7) THEN 1 
                ELSE 0 
            END as is_weekend,
            CASE 
                WHEN event_hour BETWEEN 9 AND 18 THEN 1 
                ELSE 0 
            END as is_work_hour
        FROM session_hourly_stats
        WHERE event_date >= today() - {days}
        ORDER BY event_date, event_hour
        """
        
        logger.info(f"Загрузка данных за {days} дней...")
        result = self.client.execute(query)
        
        df = pd.DataFrame(result, columns=[
            'date', 'hour', 'sessions', 'users', 'computers',
            'avg_duration', 'p95_duration', 'deadlocks', 'exceptions', 'timeouts',
            'is_weekend', 'is_work_hour'
        ])
        
        logger.info(f"Загружено {len(df)} записей")
        return df
    
    def get_daily_patterns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Добавление признаков для обучения: средние по часам, отклонения и т.д.
        """
        # Добавляем средние значения для каждого часа
        hourly_avg = df.groupby('hour')['sessions'].mean().to_dict()
        df['hour_avg'] = df['hour'].map(hourly_avg)
        
        # Отклонение от среднего по часу
        df['sessions_deviation'] = (df['sessions'] - df['hour_avg']) / df['hour_avg'].clip(lower=1)
        
        # Скользящие средние (для трендов)
        df = df.sort_values(['date', 'hour'])
        df['sessions_ma_7'] = df['sessions'].rolling(7, min_periods=1).mean()
        df['sessions_ma_24'] = df['sessions'].rolling(24, min_periods=1).mean()
        
        # Отклонение от скользящей средней
        df['sessions_trend'] = df['sessions'] - df['sessions_ma_24']
        
        # Признаки для детекции аномалий
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        
        # Доля ошибок
        df['error_rate'] = (df['exceptions'] + df['timeouts']) / df['sessions'].clip(lower=1)
        
        # Логарифмирование для нормализации
        df['log_sessions'] = np.log1p(df['sessions'])
        df['log_duration'] = np.log1p(df['avg_duration'])
        
        logger.info(f"Добавлены признаки, итого колонок: {len(df.columns)}")
        return df
    
    def save_training_data(self, df: pd.DataFrame, filename: str):
        """Сохранение данных для обучения"""
        df.to_csv(filename, index=False)
        logger.info(f"Данные сохранены в {filename}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=30, help='Дней истории')
    parser.add_argument('--output', default='training_data.csv', help='Выходной файл')
    args = parser.parse_args()
    
    extractor = TrainingDataExtractor()
    df = extractor.get_hourly_stats(days=args.days)
    df = extractor.get_daily_patterns(df)
    extractor.save_training_data(df, args.output)
    
    # Выводим базовую статистику
    print("\nБазовая статистика:")
    print(f"Всего записей: {len(df)}")
    print(f"Диапазон дат: {df['date'].min()} - {df['date'].max()}")
    print(f"Среднее число сессий: {df['sessions'].mean():.0f}")
    print(f"Стд отклонение: {df['sessions'].std():.0f}")
    print(f"Максимум: {df['sessions'].max()}")
    print(f"Минимум: {df['sessions'].min()}")

if __name__ == "__main__":
    main()
```

### 3.2. Скрипт обучения модели

**Файл:** `scripts/train_anomaly_detector.py`

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Обучение модели детектора аномалий на данных о сессиях
Использует Isolation Forest для поиска выбросов
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
import logging
from datetime import datetime
import os
import argparse
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/train_anomaly.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('train_anomaly')

class AnomalyDetectorTrainer:
    """Тренер модели детектора аномалий"""
    
    def __init__(self, contamination=0.05, random_state=42):
        """
        Args:
            contamination: ожидаемая доля аномалий в данных (по умолчанию 5%)
            random_state: для воспроизводимости
        """
        self.contamination = contamination
        self.random_state = random_state
        self.model = None
        self.scaler = StandardScaler()
        self.feature_names = None
        self.thresholds = {}  # пороги для каждой метрики (3 сигма)
        
    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Отбор и подготовка признаков для обучения
        
        Args:
            df: DataFrame с данными из prepare_training_data.py
            
        Returns:
            DataFrame только с признаками для обучения
        """
        # Выбираем признаки, которые будем использовать
        feature_columns = [
            'sessions', 'users', 'computers',
            'avg_duration', 'p95_duration',
            'deadlocks', 'exceptions', 'timeouts',
            'sessions_deviation', 'sessions_trend',
            'error_rate',
            'hour_sin', 'hour_cos',
            'log_sessions', 'log_duration'
        ]
        
        # Проверяем, что все колонки есть
        available_features = [col for col in feature_columns if col in df.columns]
        logger.info(f"Используемые признаки: {available_features}")
        
        # Сохраняем имена признаков
        self.feature_names = available_features
        
        # Заполняем пропуски
        X = df[available_features].fillna(0)
        
        # Сохраняем пороги для интерпретации
        for col in ['sessions', 'users', 'computers', 'avg_duration']:
            if col in df.columns:
                mean = df[col].mean()
                std = df[col].std()
                self.thresholds[col] = {
                    'mean': mean,
                    'std': std,
                    'upper_3sigma': mean + 3 * std,
                    'lower_3sigma': mean - 3 * std
                }
        
        return X
    
    def train(self, X: pd.DataFrame) -> tuple:
        """
        Обучение модели Isolation Forest
        
        Args:
            X: признаки для обучения
            
        Returns:
            tuple: (предсказания, оценки аномальности)
        """
        logger.info(f"Обучение модели на {len(X)} образцах, {len(X.columns)} признаках")
        
        # Нормализация данных
        X_scaled = self.scaler.fit_transform(X)
        
        # Обучение модели
        self.model = IsolationForest(
            contamination=self.contamination,
            random_state=self.random_state,
            n_estimators=100,
            max_samples='auto',
            bootstrap=False,
            n_jobs=-1
        )
        
        self.model.fit(X_scaled)
        
        # Предсказания и оценки
        predictions = self.model.predict(X_scaled)  # -1 = аномалия, 1 = норма
        scores = self.model.decision_function(X_scaled)  # чем меньше, тем более аномально
        
        # Статистика
        n_anomalies = sum(predictions == -1)
        logger.info(f"Модель обучена. Найдено аномалий: {n_anomalies} ({n_anomalies/len(X)*100:.1f}%)")
        
        return predictions, scores
    
    def analyze_results(self, df: pd.DataFrame, predictions: np.ndarray, scores: np.ndarray):
        """
        Анализ результатов обучения
        
        Args:
            df: исходный DataFrame с данными
            predictions: предсказания модели (-1 или 1)
            scores: оценки аномальности
        """
        # Добавляем результаты в DataFrame
        df_result = df.copy()
        df_result['prediction'] = predictions
        df_result['anomaly_score'] = scores
        
        # Аномалии
        anomalies = df_result[df_result['prediction'] == -1]
        
        logger.info(f"\n{'='*60}")
        logger.info("АНАЛИЗ РЕЗУЛЬТАТОВ ОБУЧЕНИЯ")
        logger.info(f"{'='*60}")
        logger.info(f"Всего записей: {len(df_result)}")
        logger.info(f"Норма: {sum(predictions == 1)} ({sum(predictions == 1)/len(predictions)*100:.1f}%)")
        logger.info(f"Аномалии: {len(anomalies)} ({len(anomalies)/len(predictions)*100:.1f}%)")
        
        if not anomalies.empty:
            logger.info(f"\nТоп-10 аномалий (по убыванию аномальности):")
            top_anomalies = anomalies.nsmallest(10, 'anomaly_score')
            
            for idx, row in top_anomalies.iterrows():
                date_str = f"{row['date']} {int(row['hour']):02d}:00"
                sessions = row['sessions']
                avg_sessions = self.thresholds.get('sessions', {}).get('mean', 0)
                logger.info(f"  {date_str} | сессии: {sessions} (норма: {avg_sessions:.0f}) | "
                           f"оценка: {row['anomaly_score']:.3f}")
        
        # Сохраняем график
        self._save_plot(df_result)
        
        return df_result
    
    def _save_plot(self, df: pd.DataFrame):
        """Сохранение графика с аномалиями"""
        try:
            plt.figure(figsize=(15, 6))
            
            # Нормальные точки
            normal = df[df['prediction'] == 1]
            plt.scatter(normal.index, normal['sessions'], 
                       c='green', alpha=0.5, label='Норма', s=20)
            
            # Аномалии
            anomalies = df[df['prediction'] == -1]
            plt.scatter(anomalies.index, anomalies['sessions'], 
                       c='red', alpha=0.8, label='Аномалия', s=50, marker='x')
            
            plt.title('Детектор аномалий сессий 1С (Isolation Forest)')
            plt.xlabel('Время (часы)')
            plt.ylabel('Количество сессий')
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            # Сохраняем
            plot_path = 'models/anomaly_plot.png'
            plt.savefig(plot_path)
            logger.info(f"График сохранен в {plot_path}")
            plt.close()
            
        except Exception as e:
            logger.error(f"Ошибка сохранения графика: {e}")
    
    def save_model(self, path: str):
        """Сохранение модели и скейлера"""
        if self.model is None:
            raise ValueError("Модель не обучена")
        
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'feature_names': self.feature_names,
            'thresholds': self.thresholds,
            'contamination': self.contamination,
            'train_date': datetime.now().isoformat()
        }
        
        joblib.dump(model_data, path)
        logger.info(f"Модель сохранена в {path}")
    
    def load_model(self, path: str):
        """Загрузка модели"""
        model_data = joblib.load(path)
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.feature_names = model_data['feature_names']
        self.thresholds = model_data.get('thresholds', {})
        logger.info(f"Модель загружена из {path}")

def main():
    parser = argparse.ArgumentParser(description='Обучение детектора аномалий')
    parser.add_argument('--input', default='training_data.csv', help='CSV с обучающими данными')
    parser.add_argument('--output', default='models/anomaly_model.pkl', help='Путь для сохранения модели')
    parser.add_argument('--contamination', type=float, default=0.05, help='Доля аномалий')
    parser.add_argument('--plot', action='store_true', help='Построить график')
    
    args = parser.parse_args()
    
    # Создаем директорию для моделей
    os.makedirs('models', exist_ok=True)
    
    # Загружаем данные
    logger.info(f"Загрузка данных из {args.input}")
    df = pd.read_csv(args.input, parse_dates=['date'])
    
    # Создаем и обучаем детектор
    trainer = AnomalyDetectorTrainer(contamination=args.contamination)
    
    # Подготовка признаков
    X = trainer.prepare_features(df)
    
    # Обучение
    predictions, scores = trainer.train(X)
    
    # Анализ
    trainer.analyze_results(df, predictions, scores)
    
    # Сохраняем модель
    trainer.save_model(args.output)
    
    # Выводим пороги для ручного контроля
    print("\nПороги 3-сигма для ключевых метрик:")
    for metric, thresholds in trainer.thresholds.items():
        print(f"  {metric}:")
        print(f"    среднее: {thresholds['mean']:.1f}")
        print(f"    std: {thresholds['std']:.1f}")
        print(f"    нижний порог: {thresholds['lower_3sigma']:.1f}")
        print(f"    верхний порог: {thresholds['upper_3sigma']:.1f}")

if __name__ == "__main__":
    main()
```

### 3.3. Запуск обучения

```bash
# 1. Подготовка данных
python scripts/prepare_training_data.py --days 30 --output training_data.csv

# 2. Обучение модели
python scripts/train_anomaly_detector.py --input training_data.csv --output models/anomaly_model.pkl --contamination 0.05

# 3. Просмотр результатов
ls -la models/
# models/anomaly_model.pkl
# models/anomaly_plot.png
```

---

## 4. Детектор аномалий в реальном времени

### 4.1. Скрипт для получения текущих метрик

**Файл:** `scripts/get_current_metrics.py`

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Получение текущих метрик сессий из ClickHouse
Запускается каждый час для проверки аномалий
"""

import pandas as pd
from clickhouse_driver import Client
from datetime import datetime, timedelta
import logging

logger = logging.getLogger('get_current_metrics')

class CurrentMetricsExtractor:
    """Извлечение текущих метрик сессий"""
    
    def __init__(self, host='localhost', port=9000, database='techlog'):
        self.client = Client(host=host, port=port, database=database)
    
    def get_last_hour_stats(self) -> pd.DataFrame:
        """
        Получение статистики за последний час
        
        Returns:
            DataFrame с одной строкой - метрики за последний час
        """
        query = """
        SELECT
            toStartOfHour(now() - interval 1 hour) as hour_start,
            count() as total_sessions,
            uniq(user_name) as unique_users,
            uniq(computer_name) as unique_computers,
            avg(duration) as avg_duration,
            quantile(0.95)(duration) as p95_duration,
            sum(is_deadlock) as deadlocks,
            sum(is_exception) as exceptions,
            sum(is_timeout) as timeouts,
            CASE 
                WHEN toDayOfWeek(now()) IN (6, 7) THEN 1 
                ELSE 0 
            END as is_weekend,
            CASE 
                WHEN toHour(now()) BETWEEN 9 AND 18 THEN 1 
                ELSE 0 
            END as is_work_hour
        FROM session_events
        WHERE event_datetime >= now() - interval 1 hour
        """
        
        result = self.client.execute(query)
        
        if not result:
            return pd.DataFrame()
        
        df = pd.DataFrame([result[0]], columns=[
            'hour_start', 'sessions', 'users', 'computers',
            'avg_duration', 'p95_duration', 'deadlocks', 'exceptions', 'timeouts',
            'is_weekend', 'is_work_hour'
        ])
        
        return df
    
    def get_hourly_avg(self, hour: int, is_weekend: bool) -> dict:
        """
        Получение средних значений для конкретного часа
        
        Args:
            hour: час (0-23)
            is_weekend: выходной ли день
            
        Returns:
            словарь со средними значениями
        """
        query = """
        SELECT
            avg(total_sessions) as avg_sessions,
            avg(avg_duration) as avg_duration,
            avg(deadlock_count) as avg_deadlocks,
            avg(exception_count) as avg_exceptions
        FROM session_hourly_stats
        WHERE event_hour = %(hour)s
          AND is_weekend = %(is_weekend)s
          AND event_date >= today() - 30
        """
        
        result = self.client.execute(query, {'hour': hour, 'is_weekend': is_weekend})
        
        if result and result[0]:
            return {
                'avg_sessions': result[0][0] or 0,
                'avg_duration': result[0][1] or 0,
                'avg_deadlocks': result[0][2] or 0,
                'avg_exceptions': result[0][3] or 0
            }
        return {}
```

### 4.2. Основной скрипт детектора аномалий

**Файл:** `scripts/detect_anomalies.py`

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Детектор аномалий в реальном времени
Запускается каждый час, проверяет текущие метрики на аномалии
"""

import numpy as np
import pandas as pd
import joblib
import logging
from datetime import datetime
import os
import sys
from pathlib import Path

# Добавляем путь для импорта наших модулей
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from scripts.get_current_metrics import CurrentMetricsExtractor
from scripts.alert_telegram import send_telegram_alert
from scripts.itsm.factory import create_itsm_client

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/detect_anomalies.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('detect_anomalies')

class AnomalyDetector:
    """Детектор аномалий в реальном времени"""
    
    def __init__(self, model_path='models/anomaly_model.pkl'):
        """
        Args:
            model_path: путь к сохраненной модели
        """
        self.model_path = model_path
        self.model_data = None
        self.model = None
        self.scaler = None
        self.feature_names = None
        self.thresholds = None
        
        self.load_model()
        self.metrics_extractor = CurrentMetricsExtractor()
        
        # Настройка ITSM клиента
        self.itsm_client = create_itsm_client()
    
    def load_model(self):
        """Загрузка обученной модели"""
        if not os.path.exists(self.model_path):
            logger.error(f"Модель не найдена: {self.model_path}")
            return False
        
        try:
            self.model_data = joblib.load(self.model_path)
            self.model = self.model_data['model']
            self.scaler = self.model_data['scaler']
            self.feature_names = self.model_data['feature_names']
            self.thresholds = self.model_data.get('thresholds', {})
            logger.info(f"Модель загружена из {self.model_path}")
            logger.info(f"Обучалась: {self.model_data.get('train_date')}")
            return True
        except Exception as e:
            logger.error(f"Ошибка загрузки модели: {e}")
            return False
    
    def prepare_current_features(self, current_data: pd.DataFrame) -> np.ndarray:
        """
        Подготовка признаков для текущего момента
        
        Args:
            current_data: DataFrame с текущими метриками
            
        Returns:
            numpy array с признаками
        """
        if current_data.empty:
            return None
        
        # Создаем DataFrame с теми же колонками, что и при обучении
        features = pd.DataFrame(index=[0])
        
        # Заполняем признаки
        for feature in self.feature_names:
            if feature in current_data.columns:
                features[feature] = current_data[feature].values[0]
            elif feature == 'sessions_deviation':
                # Считаем отклонение от среднего
                hour = current_data['hour_start'].dt.hour.values[0]
                is_weekend = current_data['is_weekend'].values[0]
                avg = self.metrics_extractor.get_hourly_avg(hour, is_weekend).get('avg_sessions', 1)
                current = current_data['sessions'].values[0]
                features['sessions_deviation'] = (current - avg) / max(avg, 1)
            
            elif feature == 'sessions_trend':
                # Упрощенно: тренд = текущее - среднее за день
                features['sessions_trend'] = 0  # в реальности нужно считать скользящее среднее
            
            elif feature == 'error_rate':
                current = current_data['sessions'].values[0]
                exceptions = current_data['exceptions'].values[0]
                timeouts = current_data['timeouts'].values[0]
                features['error_rate'] = (exceptions + timeouts) / max(current, 1)
            
            elif feature == 'hour_sin':
                hour = current_data['hour_start'].dt.hour.values[0]
                features['hour_sin'] = np.sin(2 * np.pi * hour / 24)
            
            elif feature == 'hour_cos':
                hour = current_data['hour_start'].dt.hour.values[0]
                features['hour_cos'] = np.cos(2 * np.pi * hour / 24)
            
            elif feature == 'log_sessions':
                current = current_data['sessions'].values[0]
                features['log_sessions'] = np.log1p(current)
            
            elif feature == 'log_duration':
                current = current_data['avg_duration'].values[0]
                features['log_duration'] = np.log1p(current)
            
            else:
                features[feature] = 0
        
        # Заполняем пропуски
        features = features.fillna(0)
        
        # Берем только нужные признаки в правильном порядке
        X = features[self.feature_names].values
        
        return X
    
    def check_simple_thresholds(self, current_data: pd.DataFrame) -> list:
        """
        Проверка простых порогов 3-сигма
        
        Args:
            current_data: текущие метрики
            
        Returns:
            список предупреждений
        """
        warnings = []
        
        if current_data.empty:
            return warnings
        
        current = current_data.iloc[0]
        
        # Проверяем количество сессий
        if 'sessions' in self.thresholds:
            th = self.thresholds['sessions']
            sessions = current['sessions']
            
            if sessions > th['upper_3sigma']:
                sigma = (sessions - th['mean']) / th['std']
                warnings.append({
                    'metric': 'sessions',
                    'value': sessions,
                    'expected': th['mean'],
                    'sigma': sigma,
                    'direction': 'high',
                    'message': f"⚠️ Аномально высокое число сессий: {sessions} (ожидалось {th['mean']:.0f} ± {th['std']:.0f}, отклонение {sigma:.1f}σ)"
                })
            
            elif sessions < th['lower_3sigma']:
                sigma = (th['mean'] - sessions) / th['std']
                warnings.append({
                    'metric': 'sessions',
                    'value': sessions,
                    'expected': th['mean'],
                    'sigma': sigma,
                    'direction': 'low',
                    'message': f"⚠️ Аномально низкое число сессий: {sessions} (ожидалось {th['mean']:.0f} ± {th['std']:.0f}, отклонение {sigma:.1f}σ)"
                })
        
        # Проверяем длительность
        if 'avg_duration' in self.thresholds:
            th = self.thresholds['avg_duration']
            duration = current['avg_duration']
            
            if duration > th['upper_3sigma']:
                sigma = (duration - th['mean']) / th['std']
                warnings.append({
                    'metric': 'duration',
                    'value': duration,
                    'expected': th['mean'],
                    'sigma': sigma,
                    'direction': 'high',
                    'message': f"⚠️ Аномально высокая длительность: {duration:.0f} мкс (ожидалось {th['mean']:.0f} ± {th['std']:.0f}, отклонение {sigma:.1f}σ)"
                })
        
        # Проверяем deadlock'и
        deadlocks = current['deadlocks']
        if deadlocks > 0:
            warnings.append({
                'metric': 'deadlocks',
                'value': deadlocks,
                'expected': 0,
                'sigma': float('inf'),
                'direction': 'high',
                'message': f"🚨 Обнаружены deadlock'и: {deadlocks} за последний час!"
            })
        
        # Проверяем ошибки
        exceptions = current['exceptions']
        if exceptions > 10:
            warnings.append({
                'metric': 'exceptions',
                'value': exceptions,
                'expected': 0,
                'sigma': float('inf'),
                'direction': 'high',
                'message': f"⚠️ Много исключений: {exceptions} за последний час"
            })
        
        return warnings
    
    def check_ml_anomaly(self, X: np.ndarray) -> tuple:
        """
        Проверка аномалии с помощью ML модели
        
        Args:
            X: признаки для проверки
            
        Returns:
            tuple: (is_anomaly, score)
        """
        if X is None or self.model is None:
            return False, 0
        
        # Нормализация
        X_scaled = self.scaler.transform(X)
        
        # Предсказание
        prediction = self.model.predict(X_scaled)[0]  # -1 = аномалия, 1 = норма
        score = self.model.decision_function(X_scaled)[0]
        
        is_anomaly = prediction == -1
        
        if is_anomaly:
            logger.info(f"ML модель обнаружила аномалию (оценка: {score:.3f})")
        
        return is_anomaly, score
    
    def send_alerts(self, warnings: list, ml_score: float = None):
        """
        Отправка алертов в Telegram и создание задач в ITSM
        
        Args:
            warnings: список предупреждений
            ml_score: оценка ML модели
        """
        if not warnings:
            return
        
        # Формируем сообщение
        now = datetime.now().strftime('%Y-%m-%d %H:%M')
        message = f"🚨 **ДЕТЕКТОР АНОМАЛИЙ**\n\n"
        message += f"🕐 {now}\n\n"
        
        for w in warnings:
            message += f"{w['message']}\n\n"
        
        if ml_score is not None:
            message += f"📊 Оценка ML модели: {ml_score:.3f}\n"
        
        # Отправляем в Telegram
        send_telegram_alert(message, severity='warning')
        logger.info("Алерт отправлен в Telegram")
        
        # Создаем задачу в ITSM
        if self.itsm_client:
            try:
                # Определяем приоритет
                if any(w['sigma'] > 5 or w['metric'] == 'deadlocks' for w in warnings):
                    priority = "Highest"
                elif any(w['sigma'] > 3 for w in warnings):
                    priority = "High"
                else:
                    priority = "Medium"
                
                # Формируем заголовок
                if any(w['metric'] == 'deadlocks' for w in warnings):
                    summary = f"[КРИТИЧНО] Обнаружены deadlock'и в 1С"
                elif any(w['direction'] == 'low' for w in warnings):
                    summary = f"[Превентивно] Аномальное падение активности пользователей"
                else:
                    summary = f"[Превентивно] Обнаружены аномалии в работе 1С"
                
                # Создаем задачу
                issue_id = self.itsm_client.create_issue(
                    summary=summary,
                    description=message,
                    priority=priority
                )
                
                if issue_id:
                    logger.info(f"Создана задача в ITSM: {issue_id}")
                    
            except Exception as e:
                logger.error(f"Ошибка создания задачи в ITSM: {e}")
    
    def run(self):
        """Основной метод запуска детектора"""
        logger.info("=" * 60)
        logger.info("ЗАПУСК ДЕТЕКТОРА АНОМАЛИЙ")
        
        # Получаем текущие метрики
        current_data = self.metrics_extractor.get_last_hour_stats()
        
        if current_data.empty:
            logger.warning("Нет данных за последний час")
            return
        
        # Выводим текущие метрики
        current = current_data.iloc[0]
        logger.info(f"Час: {current['hour_start']}")
        logger.info(f"Сессии: {current['sessions']}")
        logger.info(f"Пользователи: {current['users']}")
        logger.info(f"Средняя длительность: {current['avg_duration']:.0f} мкс")
        logger.info(f"Deadlock'и: {current['deadlocks']}")
        logger.info(f"Исключения: {current['exceptions']}")
        
        # 1. Проверка простых порогов
        simple_warnings = self.check_simple_thresholds(current_data)
        
        # 2. Проверка ML модели
        X = self.prepare_current_features(current_data)
        is_anomaly, ml_score = self.check_ml_anomaly(X)
        
        # Объединяем предупреждения
        all_warnings = simple_warnings.copy()
        
        if is_anomaly:
            all_warnings.append({
                'metric': 'ml_model',
                'value': ml_score,
                'message': f"🤖 ML модель классифицирует ситуацию как аномальную (оценка: {ml_score:.3f})"
            })
        
        # Отправляем алерты
        if all_warnings:
            self.send_alerts(all_warnings, ml_score)
            
            # Логируем
            for w in all_warnings:
                logger.warning(w['message'])
        else:
            logger.info("✅ Аномалий не обнаружено")
        
        # Сохраняем результат для истории
        self.save_check_result(current_data, all_warnings)
        
        logger.info("ДЕТЕКТОР ЗАВЕРШИЛ РАБОТУ")
        logger.info("=" * 60)
    
    def save_check_result(self, current_data: pd.DataFrame, warnings: list):
        """
        Сохранение результата проверки в PostgreSQL
        
        Args:
            current_data: текущие метрики
            warnings: предупреждения
        """
        try:
            import psycopg2
            from dotenv import load_dotenv
            
            load_dotenv()
            
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                database=os.getenv('DB_NAME', 'monitoring'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', 'password')
            )
            
            cur = conn.cursor()
            
            # Создаем таблицу, если нет
            cur.execute("""
                CREATE TABLE IF NOT EXISTS anomaly_checks (
                    id SERIAL PRIMARY KEY,
                    check_time TIMESTAMP,
                    hour_start TIMESTAMP,
                    sessions INTEGER,
                    users INTEGER,
                    avg_duration FLOAT,
                    deadlocks INTEGER,
                    exceptions INTEGER,
                    has_anomaly BOOLEAN,
                    warnings TEXT,
                    ml_score FLOAT
                )
            """)
            
            current = current_data.iloc[0]
            has_anomaly = len(warnings) > 0
            warnings_text = '\n'.join([w['message'] for w in warnings])
            
            cur.execute("""
                INSERT INTO anomaly_checks 
                (check_time, hour_start, sessions, users, avg_duration, deadlocks, exceptions, has_anomaly, warnings)
                VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                current['hour_start'],
                current['sessions'],
                current['users'],
                current['avg_duration'],
                current['deadlocks'],
                current['exceptions'],
                has_anomaly,
                warnings_text
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info("Результат сохранен в PostgreSQL")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения в PostgreSQL: {e}")

def main():
    """Точка входа"""
    detector = AnomalyDetector(model_path='models/anomaly_model.pkl')
    detector.run()

if __name__ == "__main__":
    main()
```

---

## 5. Настройка автоматического запуска

### 5.1. Планировщик Windows

**Файл:** `scripts/setup_scheduler.bat`

```batch
@echo off
echo Настройка планировщика Windows для детектора аномалий

:: Путь к Python и скриптам
set PYTHON_PATH=C:\Python39\python.exe
set SCRIPTS_PATH=C:\1CML\scripts

:: Создание задачи для парсера (каждый час)
schtasks /create /tn "1CML Parse TechLog" /tr "%PYTHON_PATH% %SCRIPTS_PATH%\techlog_parser.py --dir C:\1C_techlog" /sc hourly /st 00:05 /f

:: Создание задачи для детектора (каждый час, на 10-й минуте)
schtasks /create /tn "1CML Detect Anomalies" /tr "%PYTHON_PATH% %SCRIPTS_PATH%\detect_anomalies.py" /sc hourly /st 00:10 /f

:: Создание задачи для обучения модели (раз в неделю, воскресенье в 03:00)
schtasks /create /tn "1CML Train Anomaly Model" /tr "%PYTHON_PATH% %SCRIPTS_PATH%\train_anomaly_detector.py --days 30" /sc weekly /d SUN /st 03:00 /f

echo Готово!
pause
```

### 5.2. systemd для Linux (если используете)

**Файл:** `/etc/systemd/system/1cml-detector.service`

```ini
[Unit]
Description=1CML Anomaly Detector
After=network.target clickhouse-server.service

[Service]
Type=simple
User=1cml
WorkingDirectory=/opt/1CML
ExecStart=/usr/bin/python3 /opt/1CML/scripts/detect_anomalies.py
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target
```

---

## 6. Пример работы детектора

### 6.1. Нормальная ситуация

**Лог** `logs/detect_anomalies.log`:

```
2026-02-28 10:10:01 - ЗАПУСК ДЕТЕКТОРА АНОМАЛИЙ
2026-02-28 10:10:02 - Час: 2026-02-28 09:00:00
2026-02-28 10:10:02 - Сессии: 145
2026-02-28 10:10:02 - Пользователи: 87
2026-02-28 10:10:02 - Средняя длительность: 2345 мкс
2026-02-28 10:10:02 - Deadlock'и: 0
2026-02-28 10:10:02 - Исключения: 2
2026-02-28 10:10:03 - ✅ Аномалий не обнаружено
2026-02-28 10:10:04 - ДЕТЕКТОР ЗАВЕРШИЛ РАБОТУ
```

### 6.2. Аномалия: резкое падение сессий

**Лог** `logs/detect_anomalies.log`:

```
2026-02-28 11:10:01 - ЗАПУСК ДЕТЕКТОРА АНОМАЛИЙ
2026-02-28 11:10:02 - Час: 2026-02-28 10:00:00
2026-02-28 11:10:02 - Сессии: 32
2026-02-28 11:10:02 - Пользователи: 18
2026-02-28 11:10:02 - Средняя длительность: 2789 мкс
2026-02-28 11:10:02 - Deadlock'и: 0
2026-02-28 11:10:02 - Исключения: 1
2026-02-28 11:10:03 - ⚠️ Аномально низкое число сессий: 32 (ожидалось 145 ± 28, отклонение 4.0σ)
2026-02-28 11:10:03 - 🤖 ML модель классифицирует ситуацию как аномальную (оценка: -0.234)
2026-02-28 11:10:04 - Алерт отправлен в Telegram
2026-02-28 11:10:05 - Создана задача в ITSM: IT-5678
2026-02-28 11:10:06 - ДЕТЕКТОР ЗАВЕРШИЛ РАБОТУ
```

### 6.3. Критическая ситуация: deadlock'и

**Лог** `logs/detect_anomalies.log`:

```
2026-02-28 14:10:01 - ЗАПУСК ДЕТЕКТОРА АНОМАЛИЙ
2026-02-28 14:10:02 - Час: 2026-02-28 13:00:00
2026-02-28 14:10:02 - Сессии: 187
2026-02-28 14:10:02 - Пользователи: 112
2026-02-28 14:10:02 - Средняя длительность: 5678 мкс
2026-02-28 14:10:02 - Deadlock'и: 3
2026-02-28 14:10:02 - Исключения: 15
2026-02-28 14:10:03 - 🚨 Обнаружены deadlock'и: 3 за последний час!
2026-02-28 14:10:03 - ⚠️ Много исключений: 15 за последний час
2026-02-28 14:10:03 - ⚠️ Аномально высокая длительность: 5678 мкс (ожидалось 2345 ± 567, отклонение 5.9σ)
2026-02-28 14:10:04 - 🤖 ML модель классифицирует ситуацию как аномальную (оценка: -0.456)
2026-02-28 14:10:05 - Алерт отправлен в Telegram
2026-02-28 14:10:06 - Создана задача в ITSM: IT-5679
2026-02-28 14:10:07 - ДЕТЕКТОР ЗАВЕРШИЛ РАБОТУ
```

### 6.4. Уведомление в Telegram

**Сообщение в Telegram:**

```
🚨 ДЕТЕКТОР АНОМАЛИЙ

🕐 2026-02-28 14:10

🚨 Обнаружены deadlock'и: 3 за последний час!

⚠️ Много исключений: 15 за последний час

⚠️ Аномально высокая длительность: 5678 мкс (ожидалось 2345 ± 567, отклонение 5.9σ)

🤖 ML модель классифицирует ситуацию как аномальную (оценка: -0.456)

📊 Оценка ML модели: -0.456
```

### 6.5. Задача в Jira

- **Ключ:** IT-5679
- **Заголовок:** [КРИТИЧНО] Обнаружены deadlock'и в 1С
- **Приоритет:** Highest
- **Описание:** (то же, что в Telegram)

---

## 7. Полный цикл работы детектора аномалий

```
┌────────────────────────────────────────────────────────────────┐
│                    ДАННЫЕ (ClickHouse)                          │
│  session_events (сырые события)                                 │
│  session_hourly_stats (агрегаты)                                │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│              ОБУЧЕНИЕ (раз в неделю, воскресенье 03:00)        │
│  1. prepare_training_data.py --days 30                         │
│     → выгрузка статистики за 30 дней                           │
│                                                                 │
│  2. train_anomaly_detector.py --input training_data.csv        │
│     → обучение Isolation Forest                                 │
│     → сохранение модели в models/anomaly_model.pkl             │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│             ДЕТЕКТИРОВАНИЕ (каждый час, в 10 минут)            │
│  1. get_current_metrics.py                                     │
│     → получение статистики за последний час                    │
│                                                                 │
│  2. detect_anomalies.py                                        │
│     → загрузка модели                                          │
│     → проверка порогов 3-сигма                                 │
│     → проверка ML моделью                                      │
│     → если аномалия:                                           │
│       • отправка в Telegram                                    │
│       • создание задачи в ITSM                                 │
│       • запись в PostgreSQL                                    │
└────────────────────────────────────────────────────────────────┘
```

---

## 8. Что нужно для запуска

### 8.1. Файлы для создания

```
1CML/
├── clickhouse/
│   └── schema.sql                      # Таблицы для сессий
├── scripts/
│   ├── techlog_parser.py                # Парсер техжурнала
│   ├── prepare_training_data.py          # Подготовка данных
│   ├── train_anomaly_detector.py         # Обучение модели
│   ├── get_current_metrics.py            # Получение текущих метрик
│   ├── detect_anomalies.py               # Детектор аномалий
│   ├── alert_telegram.py                 # Отправка в Telegram
│   └── itsm/                             # ITSM интеграции
├── models/                               # Папка для моделей
├── logs/                                  # Папка для логов
└── .env                                   # Конфигурация
```

### 8.2. Переменные окружения (`.env`)

```env
# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=techlog

# PostgreSQL (для хранения результатов)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=monitoring
DB_USER=postgres
DB_PASSWORD=password

# Telegram
TELEGRAM_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_CHAT_ID=-123456789

# ITSM (опционально)
ITSM_TYPE=jira
JIRA_URL=https://your-domain.atlassian.net
JIRA_USERNAME=user@example.com
JIRA_API_TOKEN=token
JIRA_PROJECT_KEY=IT
```

### 8.3. Команды для запуска

```bash
# 1. Создание таблиц в ClickHouse
cat clickhouse/schema.sql | docker exec -i clickhouse-server clickhouse-client --multiline

# 2. Первичное обучение модели
python scripts/prepare_training_data.py --days 30
python scripts/train_anomaly_detector.py --input training_data.csv

# 3. Запуск парсера (вручную для теста)
python scripts/techlog_parser.py --dir C:\1C_techlog

# 4. Тест детектора
python scripts/detect_anomalies.py

# 5. Настройка планировщика
scripts\setup_scheduler.bat
```

**Теперь у вас есть полная, готовая к использованию система детектора аномалий сессий 1С, которая:**

1. Собирает данные из техжурнала 1С в ClickHouse
2. Обучает модель Isolation Forest на 30 днях истории
3. Проверяет каждый час текущие метрики
4. Обнаруживает аномалии по порогам 3-сигма и ML модели
5. Отправляет алерты в Telegram
6. Создает задачи в Jira/YouTrack/ServiceNow
7. Хранит историю проверок в PostgreSQL
