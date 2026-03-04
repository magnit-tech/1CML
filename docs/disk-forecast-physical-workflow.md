# Как физически работает прогноз диска
## Полный путь данных: от метрик Windows до предотвращения сбоя

```
Windows Performance Counters → Prometheus → PostgreSQL → Python (Linear Regression) → Прогноз → Оповещение
```

---

## 1. Почему важно прогнозировать заполнение диска

### 1.1. Проблема

```
❌ Реактивный подход:
08:00  Система: "Диск C: заполнен на 95%"
08:01  Админ: "Бегу чистить, 1С может упасть в любую минуту"
08:15  Пользователи: "1С тормозит, документы не проводятся"
08:30  Админ: "Нашел, логи забили диск. Почистил."
09:00  Бизнес: "1,5 часа простоя. Потерянные деньги."

✅ Превентивный подход:
T - 14 дней: "Диск заполнится через 14 дней при текущем темпе"
T - 10 дней: Заказан новый диск
T - 7 дней: Диск установлен
T + 0: Пользователи ничего не заметили
```

### 1.2. Статистика
- **80%** инцидентов с дисками можно предотвратить за 7-14 дней
- **Средняя скорость роста** диска с базой 1С: 2-5 ГБ/день
- **Типичный лимит**: 200 ГБ для системного диска, 500+ ГБ для диска с базами

---

## 2. Источники данных о диске

### 2.1. Windows Performance Counters

**Какие метрики собираем:**

| Метрика | Описание | Единица измерения |
|---------|----------|-------------------|
| LogicalDisk\Free Megabytes | Свободное место | МБ |
| LogicalDisk\% Free Space | Процент свободного места | % |
| LogicalDisk\Disk Reads/sec | Чтений в секунду | ops |
| LogicalDisk\Disk Writes/sec | Записей в секунду | ops |
| LogicalDisk\Avg. Disk Queue Length | Очередь к диску | количество |

### 2.2. Prometheus Windows Exporter

**Настройка windows_exporter** (`C:\Program Files\windows_exporter\config.yml`):

```yaml
collectors:
  enabled: cpu, memory, disk, logical_disk, os, system, textfile

collector:
  logical_disk:
    volume_whitelist: "C:,D:,E:"  # только нужные диски
```

**Метрики, которые отдаёт экспортер:**

```
windows_logical_disk_free_bytes{volume="C:"}  # свободно байт
windows_logical_disk_size_bytes{volume="C:"}  # всего байт
windows_logical_disk_used_bytes{volume="C:"}  # занято байт
```

### 2.3. Альтернативный источник: WMI запрос

Если нет Prometheus, можно получать данные напрямую через WMI:

```powershell
# PowerShell скрипт для сбора данных о диске
Get-WmiObject Win32_LogicalDisk -Filter "DeviceID='C:'" | 
Select-Object DeviceID, 
    @{Name="SizeGB"; Expression={[math]::Round($_.Size/1GB,2)}},
    @{Name="FreeGB"; Expression={[math]::Round($_.FreeSpace/1GB,2)}},
    @{Name="UsedGB"; Expression={[math]::Round(($_.Size - $_.FreeSpace)/1GB,2)}}
```

---

## 3. PostgreSQL: хранение данных и прогнозов

### 3.1. Структура таблиц

**Файл:** `postgresql/create_tables.sql`

```sql
-- Создание базы данных
CREATE DATABASE IF NOT EXISTS monitoring;

\c monitoring;

-- Таблица для хранения истории заполнения диска
CREATE TABLE IF NOT EXISTS disk_usage (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    disk_letter VARCHAR(2) NOT NULL,
    used_gb FLOAT NOT NULL,
    free_gb FLOAT NOT NULL,
    total_gb FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(date, disk_letter)
);

CREATE INDEX idx_disk_usage_date ON disk_usage(date);
CREATE INDEX idx_disk_usage_letter ON disk_usage(disk_letter);

-- Таблица для хранения прогнозов
CREATE TABLE IF NOT EXISTS disk_forecast (
    id SERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,           -- дата, на которую сделан прогноз
    disk_letter VARCHAR(2) NOT NULL,
    
    -- Фактические данные на дату прогноза
    actual_used_gb FLOAT,
    
    -- Прогнозы
    forecast_7d_gb FLOAT,                 -- прогноз через 7 дней
    forecast_14d_gb FLOAT,                -- прогноз через 14 дней
    forecast_30d_gb FLOAT,                -- прогноз через 30 дней
    
    -- Метрики модели
    growth_rate_gb_per_day FLOAT,         -- скорость роста (коэффициент регрессии)
    days_to_limit FLOAT,                   -- дней до заполнения
    confidence_interval_lower FLOAT,       -- нижняя граница доверительного интервала
    confidence_interval_upper FLOAT,       -- верхняя граница доверительного интервала
    
    -- Качество модели
    mae FLOAT,                             -- средняя абсолютная ошибка
    r2 FLOAT,                              -- коэффициент детерминации
    
    forecast_date TIMESTAMP DEFAULT NOW(),
    UNIQUE(metric_date, disk_letter)
);

CREATE INDEX idx_forecast_date ON disk_forecast(metric_date);
CREATE INDEX idx_forecast_letter ON disk_forecast(disk_letter);

-- Таблица для метрик качества моделей
CREATE TABLE IF NOT EXISTS model_quality (
    id SERIAL PRIMARY KEY,
    train_date TIMESTAMP NOT NULL,
    disk_letter VARCHAR(2) NOT NULL,
    model_type VARCHAR(50) NOT NULL,
    mae FLOAT,
    r2 FLOAT,
    growth_rate FLOAT,
    samples_count INTEGER,
    training_days INTEGER
);

-- Таблица для алертов
CREATE TABLE IF NOT EXISTS disk_alerts (
    id SERIAL PRIMARY KEY,
    alert_date TIMESTAMP NOT NULL,
    disk_letter VARCHAR(2) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,      -- warning, critical
    forecast_days INTEGER,                  -- через сколько дней проблема
    current_used_gb FLOAT,
    forecast_used_gb FLOAT,
    threshold_gb FLOAT,
    message TEXT,
    acknowledged BOOLEAN DEFAULT FALSE,
    jira_ticket VARCHAR(50)
);

CREATE INDEX idx_alerts_date ON disk_alerts(alert_date DESC);

-- Представление для Grafana (текущее состояние + прогноз)
CREATE OR REPLACE VIEW disk_status AS
SELECT 
    du.date,
    du.disk_letter,
    du.used_gb as actual_used,
    df.forecast_7d_gb,
    df.forecast_14d_gb,
    df.forecast_30d_gb,
    df.growth_rate_gb_per_day,
    df.days_to_limit,
    CASE 
        WHEN df.days_to_limit <= 7 THEN 'critical'
        WHEN df.days_to_limit <= 14 THEN 'warning'
        WHEN df.days_to_limit <= 30 THEN 'info'
        ELSE 'normal'
    END as status
FROM disk_usage du
LEFT JOIN disk_forecast df ON du.date = df.metric_date AND du.disk_letter = df.disk_letter
ORDER BY du.date DESC;
```

### 3.2. Пример данных

```sql
-- Вставить тестовые данные
INSERT INTO disk_usage (date, disk_letter, used_gb, free_gb, total_gb) VALUES
('2026-02-01', 'D:', 120.5, 379.5, 500),
('2026-02-02', 'D:', 123.2, 376.8, 500),
('2026-02-03', 'D:', 125.8, 374.2, 500),
...
('2026-02-28', 'D:', 156.3, 343.7, 500);

-- Посмотреть последний прогноз
SELECT * FROM disk_forecast 
WHERE disk_letter = 'D:' 
ORDER BY metric_date DESC 
LIMIT 1;

-- Посмотреть алерты
SELECT * FROM disk_alerts 
WHERE alert_date >= NOW() - INTERVAL '7 days'
ORDER BY alert_date DESC;
```

---

## 4. Скрипт для сбора метрик диска

### 4.1. Сбор через Prometheus API

**Файл:** `scripts/collect_disk_metrics.py`

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Сбор метрик диска из Prometheus и сохранение в PostgreSQL
Запускается каждый час
"""

import os
import sys
import logging
from datetime import datetime
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/disk_metrics.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('disk_metrics')

load_dotenv()

class DiskMetricsCollector:
    """Сборщик метрик диска из Prometheus"""
    
    def __init__(self):
        self.prometheus_url = os.getenv('PROMETHEUS_URL', 'http://localhost:9090')
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'monitoring'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password')
        }
        
        # Какие диски отслеживаем (из .env или по умолчанию)
        disks = os.getenv('MONITORED_DISKS', 'C:,D:,E:').split(',')
        self.monitored_disks = [d.strip() for d in disks]
        
    def get_disk_metrics_from_prometheus(self):
        """
        Получение метрик диска из Prometheus через PromQL
        """
        metrics = []
        
        for disk in self.monitored_disks:
            # Запрос в Prometheus: used bytes
            query = f'windows_logical_disk_used_bytes{{volume="{disk}"}}'
            
            try:
                response = requests.get(
                    f'{self.prometheus_url}/api/v1/query',
                    params={'query': query}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data['status'] == 'success' and data['data']['result']:
                        result = data['data']['result'][0]
                        used_bytes = float(result['value'][1])
                        
                        # Получаем общий размер
                        size_query = f'windows_logical_disk_size_bytes{{volume="{disk}"}}'
                        size_response = requests.get(
                            f'{self.prometheus_url}/api/v1/query',
                            params={'query': size_query}
                        )
                        
                        if size_response.status_code == 200:
                            size_data = size_response.json()
                            if size_data['status'] == 'success' and size_data['data']['result']:
                                size_result = size_data['data']['result'][0]
                                total_bytes = float(size_result['value'][1])
                                
                                used_gb = used_bytes / (1024**3)
                                total_gb = total_bytes / (1024**3)
                                free_gb = total_gb - used_gb
                                
                                metrics.append({
                                    'date': datetime.now().date(),
                                    'disk_letter': disk,
                                    'used_gb': round(used_gb, 2),
                                    'free_gb': round(free_gb, 2),
                                    'total_gb': round(total_gb, 2)
                                })
                                
                                logger.info(f"Диск {disk}: {used_gb:.1f} ГБ / {total_gb:.1f} ГБ")
                            else:
                                logger.warning(f"Нет данных о размере диска {disk}")
                    else:
                        logger.warning(f"Нет данных о занятом месте на диске {disk}")
                else:
                    logger.error(f"Ошибка запроса к Prometheus: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"Ошибка получения метрик для диска {disk}: {e}")
        
        return metrics
    
    def get_disk_metrics_from_wmi(self):
        """
        Альтернативный метод: прямой сбор через WMI (для Windows)
        """
        import subprocess
        import json
        
        metrics = []
        
        for disk in self.monitored_disks:
            try:
                # PowerShell скрипт для получения информации о диске
                ps_script = f"""
                $disk = Get-WmiObject Win32_LogicalDisk -Filter "DeviceID='{disk}'"
                if ($disk) {{
                    @{{
                        used_gb = [math]::Round(($disk.Size - $disk.FreeSpace)/1GB, 2)
                        free_gb = [math]::Round($disk.FreeSpace/1GB, 2)
                        total_gb = [math]::Round($disk.Size/1GB, 2)
                    }} | ConvertTo-Json
                }}
                """
                
                result = subprocess.run(
                    ['powershell', '-Command', ps_script],
                    capture_output=True,
                    text=True
                )
                
                if result.returncode == 0 and result.stdout:
                    data = json.loads(result.stdout)
                    metrics.append({
                        'date': datetime.now().date(),
                        'disk_letter': disk,
                        'used_gb': data['used_gb'],
                        'free_gb': data['free_gb'],
                        'total_gb': data['total_gb']
                    })
                    logger.info(f"Диск {disk} (WMI): {data['used_gb']:.1f} / {data['total_gb']:.1f} ГБ")
                else:
                    logger.error(f"Ошибка WMI для диска {disk}: {result.stderr}")
                    
            except Exception as e:
                logger.error(f"Ошибка WMI для диска {disk}: {e}")
        
        return metrics
    
    def save_to_postgresql(self, metrics):
        """
        Сохранение метрик в PostgreSQL
        """
        if not metrics:
            logger.warning("Нет метрик для сохранения")
            return
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            # Вставка данных
            for metric in metrics:
                cur.execute("""
                    INSERT INTO disk_usage (date, disk_letter, used_gb, free_gb, total_gb)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (date, disk_letter) 
                    DO UPDATE SET 
                        used_gb = EXCLUDED.used_gb,
                        free_gb = EXCLUDED.free_gb,
                        total_gb = EXCLUDED.total_gb,
                        created_at = NOW()
                """, (
                    metric['date'],
                    metric['disk_letter'],
                    metric['used_gb'],
                    metric['free_gb'],
                    metric['total_gb']
                ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info(f"Сохранено {len(metrics)} записей в PostgreSQL")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения в PostgreSQL: {e}")
    
    def run(self, use_prometheus=True):
        """
        Основной метод запуска сбора
        """
        logger.info("=" * 60)
        logger.info("ЗАПУСК СБОРА МЕТРИК ДИСКА")
        
        if use_prometheus:
            metrics = self.get_disk_metrics_from_prometheus()
        else:
            metrics = self.get_disk_metrics_from_wmi()
        
        if metrics:
            self.save_to_postgresql(metrics)
            logger.info(f"Сбор завершен. Получено {len(metrics)} метрик")
        else:
            logger.warning("Метрики не получены")
        
        logger.info("=" * 60)
        return metrics

def main():
    collector = DiskMetricsCollector()
    
    # Пробуем сначала Prometheus, если не работает - WMI
    try:
        collector.run(use_prometheus=True)
    except Exception as e:
        logger.error(f"Ошибка Prometheus: {e}")
        logger.info("Пробуем WMI...")
        collector.run(use_prometheus=False)

if __name__ == "__main__":
    main()
```

---

## 5. Основной скрипт прогноза диска

### 5.1. Полная версия с линейной регрессией

**Файл:** `scripts/predict_disk.py`

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Прогнозирование заполнения диска с помощью линейной регрессии
Запуск: раз в день (рекомендуется в 08:00)
"""

import os
import sys
import logging
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
import psycopg2
from psycopg2.extras import RealDictCursor
import joblib
from dotenv import load_dotenv

# Добавляем пути для ITSM интеграции
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/disk_predict.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('disk_predict')

load_dotenv()

class DiskPredictor:
    """Прогнозирование заполнения диска"""
    
    def __init__(self, disk_letter='D:'):
        """
        Args:
            disk_letter: буква диска для прогноза
        """
        self.disk_letter = disk_letter
        
        # Конфигурация из .env
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'monitoring'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password')
        }
        
        # Пороговые значения
        self.disk_limit_gb = float(os.getenv('DISK_LIMIT_GB', '200'))
        self.warning_threshold = float(os.getenv('WARNING_THRESHOLD', '0.8'))  # 80% от лимита
        self.critical_threshold = float(os.getenv('CRITICAL_THRESHOLD', '0.9'))  # 90% от лимита
        
        # Параметры прогноза
        self.forecast_days = [7, 14, 30]  # на сколько дней вперед
        self.training_days = 60  # сколько дней истории брать
        
        # Для хранения модели
        self.model = None
        self.scaler = None
        self.last_training_date = None
        
    def load_history_from_db(self) -> pd.DataFrame:
        """
        Загрузка исторических данных из PostgreSQL
        
        Returns:
            DataFrame с колонками: date, used_gb
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            
            query = """
                SELECT 
                    date,
                    used_gb
                FROM disk_usage
                WHERE disk_letter = %s
                  AND date >= CURRENT_DATE - INTERVAL '%s days'
                ORDER BY date
            """
            
            df = pd.read_sql_query(
                query, 
                conn, 
                params=(self.disk_letter, self.training_days),
                parse_dates=['date']
            )
            
            conn.close()
            
            if df.empty:
                logger.warning(f"Нет данных в БД для диска {self.disk_letter}")
                return self._generate_test_data()
            
            logger.info(f"Загружено {len(df)} записей из БД")
            return df
            
        except Exception as e:
            logger.error(f"Ошибка загрузки из БД: {e}")
            logger.info("Генерируем тестовые данные")
            return self._generate_test_data()
    
    def _generate_test_data(self) -> pd.DataFrame:
        """
        Генерация тестовых данных для демо
        """
        dates = pd.date_range(
            end=datetime.now(),
            periods=self.training_days,
            freq='D'
        )
        
        # Линейный рост с небольшим шумом
        base = 100
        growth = 0.5  # ГБ в день
        noise = np.random.normal(0, 2, self.training_days)
        
        used_gb = base + growth * np.arange(self.training_days) + noise
        
        df = pd.DataFrame({
            'date': dates,
            'used_gb': np.maximum(used_gb, 0)  # не может быть отрицательным
        })
        
        logger.info(f"Сгенерировано {len(df)} тестовых записей")
        return df
    
    def prepare_features(self, df: pd.DataFrame) -> tuple:
        """
        Подготовка признаков для обучения
        
        Args:
            df: DataFrame с данными
            
        Returns:
            tuple: (X, y, days_num)
        """
        # Сортируем по дате
        df = df.sort_values('date').copy()
        
        # Дни от начала отсчета
        df['days_num'] = (df['date'] - df['date'].min()).dt.days
        
        X = df['days_num'].values.reshape(-1, 1)
        y = df['used_gb'].values
        
        return X, y, df['days_num'].max()
    
    def train_model(self, X: np.ndarray, y: np.ndarray) -> dict:
        """
        Обучение модели линейной регрессии
        
        Args:
            X: признаки (дни)
            y: целевая переменная (заполнение диска)
            
        Returns:
            dict: метрики качества модели
        """
        # Разделение на train/test (80/20)
        split_idx = int(len(X) * 0.8)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]
        
        # Обучение модели
        self.model = LinearRegression()
        self.model.fit(X_train, y_train)
        
        # Оценка качества
        y_pred = self.model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        metrics = {
            'mae': round(mae, 2),
            'r2': round(r2, 3),
            'growth_rate': round(self.model.coef_[0], 3),
            'intercept': round(self.model.intercept_, 2),
            'train_samples': len(X_train),
            'test_samples': len(X_test)
        }
        
        logger.info(f"Модель обучена:")
        logger.info(f"  MAE: {metrics['mae']} ГБ")
        logger.info(f"  R²: {metrics['r2']}")
        logger.info(f"  Скорость роста: {metrics['growth_rate']} ГБ/день")
        
        self.last_training_date = datetime.now()
        
        return metrics
    
    def make_forecast(self, last_day: int, days_ahead: int) -> float:
        """
        Прогноз на days_ahead дней вперед
        
        Args:
            last_day: последний день в исторических данных
            days_ahead: на сколько дней вперед прогнозировать
            
        Returns:
            float: прогнозируемое значение
        """
        if self.model is None:
            raise ValueError("Модель не обучена")
        
        future_day = last_day + days_ahead
        forecast = self.model.predict([[future_day]])[0]
        
        return round(forecast, 2)
    
    def calculate_days_to_limit(self, current_usage: float) -> float:
        """
        Расчет дней до достижения лимита
        
        Args:
            current_usage: текущее заполнение
            
        Returns:
            float: количество дней до лимита (inf если диск не растет)
        """
        if self.model is None:
            return float('inf')
        
        growth_rate = self.model.coef_[0]
        
        if growth_rate <= 0:
            return float('inf')
        
        days = (self.disk_limit_gb - current_usage) / growth_rate
        return max(0, round(days, 1))
    
    def calculate_confidence_interval(self, X: np.ndarray, y: np.ndarray, 
                                     days_ahead: int) -> tuple:
        """
        Расчет доверительного интервала прогноза
        
        Args:
            X: признаки
            y: целевая переменная
            days_ahead: на сколько дней прогноз
            
        Returns:
            tuple: (lower, upper) границы доверительного интервала
        """
        # Остатки модели
        y_pred = self.model.predict(X)
        residuals = y - y_pred
        
        # Стандартная ошибка
        std_error = np.std(residuals)
        
        # Прогноз
        last_day = X[-1][0]
        future_day = last_day + days_ahead
        forecast = self.model.predict([[future_day]])[0]
        
        # Доверительный интервал (95%)
        import scipy.stats as stats
        t_value = stats.t.ppf(0.975, len(X) - 2)
        margin = t_value * std_error * np.sqrt(1 + 1/len(X) + 
                                               (future_day - X.mean())**2 / 
                                               np.sum((X - X.mean())**2))
        
        lower = forecast - margin
        upper = forecast + margin
        
        return (round(lower, 2), round(upper, 2))
    
    def save_forecast_to_db(self, forecast_date: datetime, current_usage: float,
                           forecasts: dict, metrics: dict, confidence: tuple,
                           days_to_limit: float):
        """
        Сохранение прогноза в PostgreSQL
        
        Args:
            forecast_date: дата прогноза
            current_usage: текущее заполнение
            forecasts: словарь с прогнозами {7: val, 14: val, 30: val}
            metrics: метрики модели
            confidence: доверительный интервал
            days_to_limit: дней до лимита
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO disk_forecast (
                    metric_date, disk_letter, actual_used_gb,
                    forecast_7d_gb, forecast_14d_gb, forecast_30d_gb,
                    growth_rate_gb_per_day, days_to_limit,
                    confidence_interval_lower, confidence_interval_upper,
                    mae, r2, forecast_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (metric_date, disk_letter) DO UPDATE SET
                    actual_used_gb = EXCLUDED.actual_used_gb,
                    forecast_7d_gb = EXCLUDED.forecast_7d_gb,
                    forecast_14d_gb = EXCLUDED.forecast_14d_gb,
                    forecast_30d_gb = EXCLUDED.forecast_30d_gb,
                    growth_rate_gb_per_day = EXCLUDED.growth_rate_gb_per_day,
                    days_to_limit = EXCLUDED.days_to_limit,
                    confidence_interval_lower = EXCLUDED.confidence_interval_lower,
                    confidence_interval_upper = EXCLUDED.confidence_interval_upper,
                    mae = EXCLUDED.mae,
                    r2 = EXCLUDED.r2,
                    forecast_date = EXCLUDED.forecast_date
            """, (
                forecast_date.date(),
                self.disk_letter,
                current_usage,
                forecasts.get(7),
                forecasts.get(14),
                forecasts.get(30),
                metrics['growth_rate'],
                days_to_limit,
                confidence[0],
                confidence[1],
                metrics['mae'],
                metrics['r2'],
                datetime.now()
            ))
            
            # Сохраняем метрики качества модели
            cur.execute("""
                INSERT INTO model_quality 
                (train_date, disk_letter, model_type, mae, r2, growth_rate, samples_count, training_days)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self.last_training_date,
                self.disk_letter,
                'linear_regression',
                metrics['mae'],
                metrics['r2'],
                metrics['growth_rate'],
                metrics['train_samples'] + metrics['test_samples'],
                self.training_days
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info(f"Прогноз сохранен в БД")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения прогноза: {e}")
            raise
    
    def check_thresholds(self, current_usage: float, forecasts: dict, 
                        days_to_limit: float) -> list:
        """
        Проверка порогов и генерация предупреждений
        
        Returns:
            list: список предупреждений
        """
        warnings = []
        
        # Проверка по дням до лимита
        if days_to_limit <= 7:
            warnings.append({
                'level': 'critical',
                'type': 'days_to_limit',
                'message': f"КРИТИЧНО: диск заполнится через {days_to_limit} дней!",
                'days': days_to_limit
            })
        elif days_to_limit <= 14:
            warnings.append({
                'level': 'warning',
                'type': 'days_to_limit',
                'message': f"ВНИМАНИЕ: диск заполнится через {days_to_limit} дней",
                'days': days_to_limit
            })
        elif days_to_limit <= 30:
            warnings.append({
                'level': 'info',
                'type': 'days_to_limit',
                'message': f"ИНФО: диск заполнится через {days_to_limit} дней",
                'days': days_to_limit
            })
        
        # Проверка по проценту заполнения
        usage_percent = (current_usage / self.disk_limit_gb) * 100
        
        if usage_percent >= 90:
            warnings.append({
                'level': 'critical',
                'type': 'usage_percent',
                'message': f"КРИТИЧНО: диск заполнен на {usage_percent:.1f}%!",
                'percent': usage_percent
            })
        elif usage_percent >= 80:
            warnings.append({
                'level': 'warning',
                'type': 'usage_percent',
                'message': f"ВНИМАНИЕ: диск заполнен на {usage_percent:.1f}%",
                'percent': usage_percent
            })
        
        # Проверка прогнозов
        for days, value in forecasts.items():
            if value >= self.disk_limit_gb:
                warnings.append({
                    'level': 'critical',
                    'type': 'forecast',
                    'message': f"КРИТИЧНО: через {days} дней диск превысит лимит ({value:.1f} ГБ)",
                    'days': days,
                    'value': value
                })
            elif value >= self.disk_limit_gb * self.critical_threshold:
                warnings.append({
                    'level': 'warning',
                    'type': 'forecast',
                    'message': f"ВНИМАНИЕ: через {days} дней диск достигнет {value:.1f} ГБ ({value/self.disk_limit_gb*100:.0f}% лимита)",
                    'days': days,
                    'value': value
                })
        
        return warnings
    
    def send_alerts(self, warnings: list, forecasts: dict, current_usage: float,
                   days_to_limit: float):
        """
        Отправка алертов в Telegram и создание задач в ITSM
        """
        if not warnings:
            logger.info("✅ Аномалий не обнаружено")
            return
        
        # Определяем общий уровень
        levels = [w['level'] for w in warnings]
        if 'critical' in levels:
            overall_level = 'critical'
            severity = '🚨 КРИТИЧНО'
        elif 'warning' in levels:
            overall_level = 'warning'
            severity = '⚠️ ВНИМАНИЕ'
        else:
            overall_level = 'info'
            severity = 'ℹ️ ИНФО'
        
        # Формируем сообщение
        message = f"{severity} **ПРОГНОЗ ЗАПОЛНЕНИЯ ДИСКА {self.disk_letter}**\n\n"
        message += f"📊 **Текущие метрики:**\n"
        message += f"• Занято: {current_usage:.1f} ГБ\n"
        message += f"• Свободно: {self.disk_limit_gb - current_usage:.1f} ГБ\n"
        message += f"• Скорость роста: {forecasts.get('growth_rate', 0):.2f} ГБ/день\n"
        message += f"• Дней до лимита: {days_to_limit:.0f}\n\n"
        
        message += f"🔮 **Прогнозы:**\n"
        message += f"• Через 7 дней: {forecasts.get(7, 0):.1f} ГБ\n"
        message += f"• Через 14 дней: {forecasts.get(14, 0):.1f} ГБ\n"
        message += f"• Через 30 дней: {forecasts.get(30, 0):.1f} ГБ\n\n"
        
        message += f"⚠️ **Предупреждения:**\n"
        for w in warnings:
            message += f"• {w['message']}\n"
        
        message += f"\n📈 **Качество модели:** MAE = {forecasts.get('mae', 0)} ГБ, R² = {forecasts.get('r2', 0)}"
        message += f"\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        # Отправка в Telegram
        try:
            from scripts.alert_telegram import send_telegram_alert
            send_telegram_alert(message, severity=overall_level)
            logger.info("Алерт отправлен в Telegram")
        except Exception as e:
            logger.error(f"Ошибка отправки в Telegram: {e}")
        
        # Создание задачи в ITSM
        if overall_level in ['critical', 'warning']:
            try:
                from scripts.itsm.factory import create_itsm_client
                
                itsm = create_itsm_client()
                if itsm:
                    # Определяем приоритет
                    if overall_level == 'critical':
                        priority = "Highest"
                        due_days = max(1, int(days_to_limit) - 1)
                    else:
                        priority = "High"
                        due_days = max(2, int(days_to_limit) - 2)
                    
                    due_date = (datetime.now() + timedelta(days=due_days)).strftime('%Y-%m-%d')
                    
                    summary = f"[{overall_level.upper()}] Прогноз заполнения диска {self.disk_letter}"
                    
                    issue_id = itsm.create_issue(
                        summary=summary,
                        description=message,
                        priority=priority,
                        due_date=due_date
                    )
                    
                    if issue_id:
                        logger.info(f"Создана задача в ITSM: {issue_id}")
                        
                        # Сохраняем ID задачи в БД
                        conn = psycopg2.connect(**self.db_config)
                        cur = conn.cursor()
                        cur.execute("""
                            INSERT INTO disk_alerts (alert_date, disk_letter, alert_type, 
                                forecast_days, current_used_gb, forecast_used_gb, 
                                threshold_gb, message, jira_ticket)
                            VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            self.disk_letter,
                            overall_level,
                            days_to_limit,
                            current_usage,
                            forecasts.get(14, 0),
                            self.disk_limit_gb,
                            message,
                            issue_id
                        ))
                        conn.commit()
                        cur.close()
                        conn.close()
                        
            except Exception as e:
                logger.error(f"Ошибка создания задачи в ITSM: {e}")
    
    def run(self) -> dict:
        """
        Основной метод запуска прогноза
        
        Returns:
            dict: результаты прогноза
        """
        logger.info("=" * 60)
        logger.info(f"ЗАПУСК ПРОГНОЗА ДИСКА {self.disk_letter}")
        logger.info(f"Лимит: {self.disk_limit_gb} ГБ")
        
        # 1. Загрузка истории
        df = self.load_history_from_db()
        logger.info(f"Данные за период: {df['date'].min().date()} - {df['date'].max().date()}")
        
        # 2. Подготовка признаков
        X, y, last_day = self.prepare_features(df)
        
        # 3. Обучение модели
        metrics = self.train_model(X, y)
        
        # 4. Прогнозы
        forecasts = {}
        for days in self.forecast_days:
            forecasts[days] = self.make_forecast(last_day, days)
            logger.info(f"Прогноз через {days} дней: {forecasts[days]} ГБ")
        
        # 5. Доверительный интервал
        confidence = self.calculate_confidence_interval(X, y, 14)
        logger.info(f"Доверительный интервал (14 дней): {confidence[0]} - {confidence[1]} ГБ")
        
        # 6. Дни до лимита
        current_usage = y[-1]
        days_to_limit = self.calculate_days_to_limit(current_usage)
        logger.info(f"Дней до лимита: {days_to_limit:.0f}")
        
        # 7. Сохранение в БД
        self.save_forecast_to_db(
            df['date'].max(),
            current_usage,
            forecasts,
            metrics,
            confidence,
            days_to_limit
        )
        
        # 8. Проверка порогов
        warnings = self.check_thresholds(current_usage, forecasts, days_to_limit)
        
        # 9. Отправка алертов
        if warnings:
            self.send_alerts(warnings, {**forecasts, **metrics}, current_usage, days_to_limit)
        
        # 10. Формирование результата
        result = {
            'disk_letter': self.disk_letter,
            'forecast_date': df['date'].max().isoformat(),
            'current_usage': current_usage,
            'forecasts': forecasts,
            'metrics': metrics,
            'confidence_interval': confidence,
            'days_to_limit': days_to_limit,
            'warnings': warnings,
            'status': 'critical' if days_to_limit <= 7 else 'warning' if days_to_limit <= 14 else 'normal'
        }
        
        logger.info(f"Статус: {result['status']}")
        logger.info("ПРОГНОЗ ЗАВЕРШЕН")
        logger.info("=" * 60)
        
        return result

def main():
    """Точка входа"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Прогноз заполнения диска')
    parser.add_argument('--disk', default='D:', help='Буква диска (например D:)')
    parser.add_argument('--test', action='store_true', help='Использовать тестовые данные')
    
    args = parser.parse_args()
    
    predictor = DiskPredictor(disk_letter=args.disk)
    
    if args.test:
        # Для теста генерируем данные
        predictor.training_days = 30
        df = predictor._generate_test_data()
        
        # Сохраняем тестовые данные в БД
        conn = psycopg2.connect(**predictor.db_config)
        cur = conn.cursor()
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO disk_usage (date, disk_letter, used_gb, free_gb, total_gb)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (row['date'].date(), args.disk, row['used_gb'], 500 - row['used_gb'], 500))
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Сохранено {len(df)} тестовых записей")
    
    predictor.run()

if __name__ == "__main__":
    main()
```

---

## 6. Вспомогательные скрипты

### 6.1. Скрипт для проверки всех дисков

**Файл:** `scripts/check_all_disks.py`

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Запуск прогноза для всех отслеживаемых дисков
"""

import os
import sys
import logging
from dotenv import load_dotenv
from predict_disk import DiskPredictor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('check_all_disks')

load_dotenv()

def main():
    disks = os.getenv('MONITORED_DISKS', 'C:,D:,E:').split(',')
    disks = [d.strip() for d in disks]
    
    logger.info(f"Запуск прогноза для дисков: {disks}")
    
    results = []
    for disk in disks:
        try:
            predictor = DiskPredictor(disk_letter=disk)
            result = predictor.run()
            results.append(result)
        except Exception as e:
            logger.error(f"Ошибка при обработке диска {disk}: {e}")
    
    # Формируем сводку
    logger.info("\n" + "=" * 60)
    logger.info("СВОДКА ПО ВСЕМ ДИСКАМ")
    logger.info("=" * 60)
    
    for r in results:
        status_emoji = {
            'critical': '🔴',
            'warning': '🟡',
            'normal': '🟢'
        }.get(r['status'], '⚪')
        
        logger.info(f"{status_emoji} {r['disk_letter']}: {r['current_usage']:.1f} ГБ, "
                   f"осталось {r['days_to_limit']:.0f} дней")
    
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
```

### 6.2. Скрипт для очистки старых данных

**Файл:** `scripts/cleanup_old_data.py`

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Очистка старых данных из PostgreSQL
Запуск: раз в месяц
"""

import psycopg2
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('cleanup')

load_dotenv()

def main():
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'monitoring'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'password')
    }
    
    # Храним данные за последние 90 дней
    keep_days = 90
    cutoff_date = datetime.now() - timedelta(days=keep_days)
    
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        # Очистка disk_usage
        cur.execute("DELETE FROM disk_usage WHERE date < %s", (cutoff_date.date(),))
        deleted_usage = cur.rowcount
        
        # Очистка disk_forecast
        cur.execute("DELETE FROM disk_forecast WHERE metric_date < %s", (cutoff_date.date(),))
        deleted_forecast = cur.rowcount
        
        # Очистка disk_alerts (храним подольше)
        alert_cutoff = datetime.now() - timedelta(days=180)
        cur.execute("DELETE FROM disk_alerts WHERE alert_date < %s", (alert_cutoff,))
        deleted_alerts = cur.rowcount
        
        conn.commit()
        
        logger.info(f"Очистка завершена:")
        logger.info(f"  Удалено из disk_usage: {deleted_usage}")
        logger.info(f"  Удалено из disk_forecast: {deleted_forecast}")
        logger.info(f"  Удалено из disk_alerts: {deleted_alerts}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Ошибка очистки: {e}")

if __name__ == "__main__":
    main()
```

---

## 7. Настройка автоматического запуска

### 7.1. Планировщик Windows

**Файл:** `scripts/setup_disk_scheduler.bat`

```batch
@echo off
echo Настройка планировщика для прогноза диска

:: Путь к Python и скриптам
set PYTHON_PATH=C:\Python39\python.exe
set SCRIPTS_PATH=C:\1CML\scripts

:: Сбор метрик (каждый час)
schtasks /create /tn "1CML Collect Disk Metrics" /tr "%PYTHON_PATH% %SCRIPTS_PATH%\collect_disk_metrics.py" /sc hourly /st 00:01 /f

:: Прогноз для всех дисков (каждый день в 08:00)
schtasks /create /tn "1CML Predict Disks" /tr "%PYTHON_PATH% %SCRIPTS_PATH%\check_all_disks.py" /sc daily /st 08:00 /f

:: Очистка старых данных (1-го числа каждого месяца)
schtasks /create /tn "1CML Cleanup Old Data" /tr "%PYTHON_PATH% %SCRIPTS_PATH%\cleanup_old_data.py" /sc monthly /d 1 /st 03:00 /f

echo Готово!
pause
```

### 7.2. systemd для Linux (если используете)

**Файл:** `/etc/systemd/system/1cml-disk-predict.service`

```ini
[Unit]
Description=1CML Disk Predictor
After=network.target postgresql.service

[Service]
Type=oneshot
User=1cml
WorkingDirectory=/opt/1CML
ExecStart=/usr/bin/python3 /opt/1CML/scripts/check_all_disks.py

[Install]
WantedBy=multi-user.target
```

**Таймер для ежедневного запуска:**

```ini
[Unit]
Description=Run 1CML disk predictor daily

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

---

## 8. Пример работы

### 8.1. Нормальная ситуация

**Лог** `logs/disk_predict.log`:

```
2026-02-28 08:00:01 - ============================================================
2026-02-28 08:00:01 - ЗАПУСК ПРОГНОЗА ДИСКА D:
2026-02-28 08:00:01 - Лимит: 200 ГБ
2026-02-28 08:00:02 - Данные за период: 2025-12-30 - 2026-02-28
2026-02-28 08:00:02 - Загружено 60 записей из БД
2026-02-28 08:00:03 - Модель обучена:
2026-02-28 08:00:03 -   MAE: 1.2 ГБ
2026-02-28 08:00:03 -   R²: 0.987
2026-02-28 08:00:03 -   Скорость роста: 0.95 ГБ/день
2026-02-28 08:00:03 - Прогноз через 7 дней: 163.5 ГБ
2026-02-28 08:00:03 - Прогноз через 14 дней: 170.2 ГБ
2026-02-28 08:00:03 - Прогноз через 30 дней: 185.6 ГБ
2026-02-28 08:00:03 - Доверительный интервал (14 дней): 167.8 - 172.6 ГБ
2026-02-28 08:00:03 - Дней до лимита: 46.3
2026-02-28 08:00:04 - Прогноз сохранен в БД
2026-02-28 08:00:04 - ✅ Аномалий не обнаружено
2026-02-28 08:00:04 - Статус: normal
2026-02-28 08:00:04 - ПРОГНОЗ ЗАВЕРШЕН
2026-02-28 08:00:04 - ============================================================
```

### 8.2. Предупреждение (рост блокировок)

**Лог** `logs/disk_predict.log`:

```
2026-03-15 08:00:01 - ============================================================
2026-03-15 08:00:01 - ЗАПУСК ПРОГНОЗА ДИСКА D:
2026-03-15 08:00:01 - Лимит: 200 ГБ
2026-03-15 08:00:02 - Данные за период: 2026-01-14 - 2026-03-15
2026-03-15 08:00:02 - Загружено 60 записей из БД
2026-03-15 08:00:03 - Модель обучена:
2026-03-15 08:00:03 -   MAE: 2.1 ГБ
2026-03-15 08:00:03 -   R²: 0.956
2026-03-15 08:00:03 -   Скорость роста: 2.8 ГБ/день
2026-03-15 08:00:03 - Прогноз через 7 дней: 175.9 ГБ
2026-03-15 08:00:03 - Прогноз через 14 дней: 195.1 ГБ
2026-03-15 08:00:03 - Прогноз через 30 дней: 240.3 ГБ
2026-03-15 08:00:03 - Доверительный интервал (14 дней): 189.2 - 201.0 ГБ
2026-03-15 08:00:03 - Дней до лимита: 15.6
2026-03-15 08:00:04 - ⚠️ ВНИМАНИЕ: диск заполнится через 16 дней
2026-03-15 08:00:04 - ⚠️ ВНИМАНИЕ: через 14 дней диск достигнет 195.1 ГБ (98% лимита)
2026-03-15 08:00:04 - Алерт отправлен в Telegram
2026-03-15 08:00:05 - Создана задача в Jira: IT-5678
2026-03-15 08:00:05 - Статус: warning
2026-03-15 08:00:05 - ПРОГНОЗ ЗАВЕРШЕН
2026-03-15 08:00:05 - ============================================================
```

### 8.3. Критическая ситуация

**Лог** `logs/disk_predict.log`:

```
2026-03-22 08:00:01 - ============================================================
2026-03-22 08:00:01 - ЗАПУСК ПРОГНОЗА ДИСКА D:
2026-03-22 08:00:01 - Лимит: 200 ГБ
2026-03-22 08:00:02 - Данные за период: 2026-01-21 - 2026-03-22
2026-03-22 08:00:02 - Загружено 60 записей из БД
2026-03-22 08:00:03 - Модель обучена:
2026-03-22 08:00:03 -   MAE: 2.4 ГБ
2026-03-22 08:00:03 -   R²: 0.945
2026-03-22 08:00:03 -   Скорость роста: 3.2 ГБ/день
2026-03-22 08:00:03 - Прогноз через 7 дней: 192.8 ГБ
2026-03-22 08:00:03 - Прогноз через 14 дней: 215.2 ГБ
2026-03-22 08:00:03 - Прогноз через 30 дней: 262.4 ГБ
2026-03-22 08:00:03 - Доверительный интервал (14 дней): 207.5 - 222.9 ГБ
2026-03-22 08:00:03 - Дней до лимита: 5.3
2026-03-22 08:00:04 - 🚨 КРИТИЧНО: диск заполнится через 5 дней!
2026-03-22 08:00:04 - 🚨 КРИТИЧНО: через 7 дней диск превысит лимит (192.8 ГБ)
2026-03-22 08:00:04 - 🚨 КРИТИЧНО: через 14 дней диск превысит лимит (215.2 ГБ)
2026-03-22 08:00:04 - Алерт отправлен в Telegram
2026-03-22 08:00:05 - Создана задача в Jira: IT-5690
2026-03-22 08:00:05 - Статус: critical
2026-03-22 08:00:05 - ПРОГНОЗ ЗАВЕРШЕН
2026-03-22 08:00:05 - ============================================================
```

### 8.4. Уведомление в Telegram

**Сообщение в Telegram (предупреждение):**

```
⚠️ ВНИМАНИЕ ПРОГНОЗ ЗАПОЛНЕНИЯ ДИСКА D:

📊 Текущие метрики:
• Занято: 156.3 ГБ
• Свободно: 43.7 ГБ
• Скорость роста: 2.8 ГБ/день
• Дней до лимита: 16

🔮 Прогнозы:
• Через 7 дней: 175.9 ГБ
• Через 14 дней: 195.1 ГБ
• Через 30 дней: 240.3 ГБ

⚠️ Предупреждения:
• ВНИМАНИЕ: диск заполнится через 16 дней
• ВНИМАНИЕ: через 14 дней диск достигнет 195.1 ГБ (98% лимита)

📈 Качество модели: MAE = 2.1 ГБ, R² = 0.956
🕐 2026-03-15 08:00
```

### 8.5. Задача в Jira

- **Ключ:** IT-5678
- **Заголовок:** [WARNING] Прогноз заполнения диска D:
- **Приоритет:** High
- **Срок:** 2026-03-29
- **Описание:** (то же, что в Telegram)

---

## 9. Полный цикл работы прогноза диска

```
┌────────────────────────────────────────────────────────────────┐
│                         ДАННЫЕ                                  │
│  Windows Performance Counters → Prometheus → PostgreSQL         │
│  ИЛИ прямой сбор через WMI → PostgreSQL                         │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│              ПРОГНОЗ (каждый день в 08:00)                      │
│  1. predict_disk.py                                            │
│     → загрузка 60 дней истории из PostgreSQL                    │
│     → обучение LinearRegression                                 │
│     → прогноз на 7, 14, 30 дней                                 │
│     → расчет дней до лимита                                     │
│     → доверительный интервал                                    │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│              ПРОВЕРКА ПОРОГОВ                                    │
│  • Если дней до лимита ≤ 7 → CRITICAL                           │
│  • Если дней до лимита ≤ 14 → WARNING                           │
│  • Если прогноз > лимита → CRITICAL                             │
│  • Если заполнение > 90% → CRITICAL                             │
│  • Если заполнение > 80% → WARNING                              │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│              ОПОВЕЩЕНИЕ (при риске)                             │
│  • Telegram: детальный отчет с метриками                        │
│  • ITSM: задача с приоритетом и сроком                          │
│  • PostgreSQL: сохранение алерта                                │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│              ВИЗУАЛИЗАЦИЯ (Grafana)                             │
│  • График фактических данных + прогноз                          │
│  • Доверительный интервал                                       │
│  • Ключевые метрики (скорость роста, дней до лимита)            │
└────────────────────────────────────────────────────────────────┘
```

---

## 10. Что нужно для запуска

### 10.1. Файлы для создания

```
1CML/
├── postgresql/
│   └── create_tables.sql                  # Таблицы для прогнозов
├── scripts/
│   ├── collect_disk_metrics.py             # Сбор метрик
│   ├── predict_disk.py                      # Основной скрипт прогноза
│   ├── check_all_disks.py                    # Проверка всех дисков
│   ├── cleanup_old_data.py                    # Очистка старых данных
│   ├── alert_telegram.py                      # Отправка в Telegram
│   └── itsm/                                   # ITSM интеграции
├── logs/                                      # Папка для логов
│   └── disk_predict.log
├── models/                                    # Папка для моделей (опционально)
└── .env                                       # Конфигурация
```

### 10.2. Переменные окружения

**Файл:** `.env` (дополнение для диска)

```env
# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=monitoring
DB_USER=postgres
DB_PASSWORD=password

# Prometheus (опционально)
PROMETHEUS_URL=http://localhost:9090

# Диски для мониторинга
MONITORED_DISKS=C:,D:,E:

# Пороги для диска (в ГБ)
DISK_LIMIT_GB_C=100      # лимит для C:
DISK_LIMIT_GB_D=200      # лимит для D:
DISK_LIMIT_GB_E=500      # лимит для E:

# Общие пороги (если не указаны отдельно)
DISK_LIMIT_GB=200
WARNING_THRESHOLD=0.8    # 80% от лимита
CRITICAL_THRESHOLD=0.9   # 90% от лимита

# Telegram
TELEGRAM_TOKEN=1234567890:ABCdefGHIjkl
TELEGRAM_CHAT_ID=-123456789

# ITSM
ITSM_TYPE=jira
JIRA_URL=https://your-domain.atlassian.net
JIRA_USERNAME=user@example.com
JIRA_API_TOKEN=token
JIRA_PROJECT_KEY=IT
```

### 10.3. Команды для запуска

```bash
# 1. Создание таблиц в PostgreSQL
psql -U postgres -d monitoring -f postgresql/create_tables.sql

# 2. Тестовый сбор метрик
python scripts/collect_disk_metrics.py

# 3. Тестовый прогноз
python scripts/predict_disk.py --disk D: --test

# 4. Проверка всех дисков
python scripts/check_all_disks.py

# 5. Настройка планировщика
scripts\setup_disk_scheduler.bat
```

### 10.4. Проверка работы

```bash
# Посмотреть последний прогноз в БД
psql -U postgres -d monitoring -c "SELECT * FROM disk_forecast ORDER BY metric_date DESC LIMIT 1;"

# Посмотреть логи
tail -f logs/disk_predict.log

# Проверить алерты в БД
psql -U postgres -d monitoring -c "SELECT * FROM disk_alerts ORDER BY alert_date DESC LIMIT 5;"

# Посмотреть дашборд в Grafana
# http://localhost:3000/d/disk-forecast
```
