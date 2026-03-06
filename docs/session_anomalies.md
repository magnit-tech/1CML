### `session_anomalies.md`

# Детектор аномалий сессий

**Файлы:**
- `scripts/anomalies/train_anomaly_detector.py` — обучение модели (раз в неделю)
- `scripts/anomalies/detect_anomalies.py` — проверка текущих данных (каждый час)

Компонент для поиска необычного поведения пользователей в 1С.

---

## Часть 1: Обучение модели (раз в неделю)

### 1.1. Загрузка данных из ClickHouse

```python
def get_hourly_stats(self, days=30) -> pd.DataFrame:
    """
    Загрузка почасовой статистики за N дней
    """
    query = f"""
    SELECT 
        event_date,
        event_hour,
        total_sessions,
        unique_users,
        avg_duration,
        deadlock_count,
        exception_count
    FROM session_hourly_stats
    WHERE event_date >= today() - {days}
    ORDER BY event_date, event_hour
    """
    
    result = self.client.execute(query)
    
    df = pd.DataFrame(result, columns=[
        'date', 'hour', 'sessions', 'users',
        'duration', 'deadlocks', 'exceptions'
    ])
    
    return df
```

**Что делает:**
- Запрашивает из ClickHouse почасовую статистику за 30 дней
- Возвращает DataFrame с данными для обучения

**Пример данных:**
```
date       | hour | sessions | users | duration | deadlocks
2026-02-01 | 9    | 145      | 87    | 2345     | 0
2026-02-01 | 10   | 187      | 112   | 2567     | 0
2026-02-01 | 11   | 203      | 134   | 2789     | 0
```

---

### 1.2. Создание признаков

```python
def create_features(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    Создание дополнительных признаков для обучения
    """
    # Временные признаки (циклические)
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    
    # Среднее по часу
    hourly_avg = df.groupby('hour')['sessions'].transform('mean')
    df['sessions_deviation'] = (df['sessions'] - hourly_avg) / hourly_avg
    
    # Скользящие средние
    df = df.sort_values(['date', 'hour'])
    df['sessions_ma_24'] = df['sessions'].rolling(24, min_periods=1).mean()
    
    # Логарифмирование
    df['log_sessions'] = np.log1p(df['sessions'])
    df['log_duration'] = np.log1p(df['duration'])
    
    return df
```

**Что делает:**
- Добавляет временные признаки для учета цикличности
- Считает отклонения от среднего по часу
- Добавляет скользящие средние для учета трендов
- Логарифмирует большие значения для нормализации

---

### 1.3. Расчет порогов 3-сигма

```python
def calculate_thresholds(self, df: pd.DataFrame) -> dict:
    """
    Расчет порогов по правилу трех сигм
    """
    thresholds = {}
    
    for col in ['sessions', 'users', 'duration']:
        mean = df[col].mean()
        std = df[col].std()
        
        thresholds[col] = {
            'mean': mean,
            'std': std,
            'upper': mean + 3 * std,
            'lower': mean - 3 * std
        }
    
    return thresholds
```

**Что делает:**
- Для каждой метрики считает среднее и стандартное отклонение
- Определяет границы нормы: среднее ± 3 стандартных отклонения

**Пример для сессий:**
```python
{
    'mean': 150,
    'std': 25,
    'upper': 225,  # верхняя граница
    'lower': 75    # нижняя граница
}
```

**Правило:** если значение меньше 75 или больше 225 — это аномалия.

---

### 1.4. Обучение Isolation Forest

```python
def train_isolation_forest(self, df: pd.DataFrame, feature_cols: list):
    """
    Обучение модели Isolation Forest
    """
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    
    # Подготовка данных
    X = df[feature_cols].values
    
    # Нормализация
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Создание модели
    model = IsolationForest(
        contamination=0.05,      # ожидаем 5% аномалий
        random_state=42,         # для воспроизводимости
        n_estimators=100,        # количество деревьев
        n_jobs=-1                # использовать все ядра
    )
    
    # Обучение
    model.fit(X_scaled)
    
    return model, scaler
```

**Как работает Isolation Forest:**

Алгоритм ищет аномалии по принципу: "аномалии легче изолировать, чем нормальные точки".

1. Случайно выбирает признак и порог разделения
2. Делит данные на левые и правые узлы
3. Повторяет, пока каждая точка не будет изолирована
4. Аномалии требуют меньше разделений (короче путь)

**Пример:**
- Нормальная точка: нужно 6 разделений
- Аномальная точка: нужно 2 разделения

---

### 1.5. Сохранение модели

```python
def save_model(self, model, scaler, feature_names, thresholds, path='models/anomaly_model.pkl'):
    """
    Сохранение модели в файл
    """
    import joblib
    
    model_data = {
        'model': model,
        'scaler': scaler,
        'feature_names': feature_names,
        'thresholds': thresholds,
        'train_date': datetime.now().isoformat()
    }
    
    joblib.dump(model_data, path)
    print(f"Модель сохранена в {path}")
```

**Что делает:**
- Сохраняет обученную модель и все необходимые данные
- Файл будет использоваться для ежечасной проверки

---

## Часть 2: Детектирование в реальном времени (каждый час)

### 2.1. Загрузка модели

```python
def load_model(self, path='models/anomaly_model.pkl'):
    """
    Загрузка сохраненной модели
    """
    import joblib
    
    model_data = joblib.load(path)
    
    self.model = model_data['model']
    self.scaler = model_data['scaler']
    self.feature_names = model_data['feature_names']
    self.thresholds = model_data['thresholds']
    
    print(f"Модель загружена, обучалась: {model_data['train_date']}")
```

**Что делает:**
- Загружает ранее обученную модель из файла
- Восстанавливает все параметры для проверки

---

### 2.2. Получение текущих метрик

```python
def get_current_metrics(self) -> dict:
    """
    Получение метрик за последний час
    """
    query = """
    SELECT 
        count() as sessions,
        uniq(user_name) as users,
        avg(duration) as avg_duration,
        countIf(event_type = 'DEADLOCK') as deadlocks,
        countIf(event_type = 'EXCEPTION') as exceptions
    FROM session_events
    WHERE event_datetime >= now() - interval 1 hour
    """
    
    result = self.client.execute(query)
    
    if result and result[0]:
        return {
            'sessions': result[0][0],
            'users': result[0][1],
            'duration': result[0][2] or 0,
            'deadlocks': result[0][3],
            'exceptions': result[0][4]
        }
    return {}
```

**Что делает:**
- Запрашивает из ClickHouse данные за последний час
- Возвращает словарь с текущими метриками

**Пример результата:**
```python
{
    'sessions': 42,
    'users': 23,
    'duration': 2345,
    'deadlocks': 0,
    'exceptions': 1
}
```

---

### 2.3. Подготовка признаков для текущего часа

```python
def prepare_current_features(self, metrics: dict) -> pd.DataFrame:
    """
    Подготовка признаков для текущего часа
    """
    current_hour = datetime.now().hour
    
    # Создаем те же признаки, что и при обучении
    data = {
        'sessions': metrics['sessions'],
        'users': metrics['users'],
        'duration': metrics['duration'],
        'deadlocks': metrics['deadlocks'],
        'exceptions': metrics['exceptions'],
        'hour_sin': np.sin(2 * np.pi * current_hour / 24),
        'hour_cos': np.cos(2 * np.pi * current_hour / 24),
        'log_sessions': np.log1p(metrics['sessions']),
        'log_duration': np.log1p(metrics['duration'])
    }
    
    return pd.DataFrame([data])
```

**Что делает:**
- Создает те же признаки, что использовались при обучении
- Добавляет временные признаки для текущего часа
- Логарифмирует значения для нормализации

---

### 2.4. Проверка по правилу 3-сигма

```python
def check_3sigma(self, metrics: dict) -> list:
    """
    Проверка текущих метрик по правилу 3-сигма
    """
    warnings = []
    
    # Проверка сессий
    sessions = metrics['sessions']
    mean = self.thresholds['sessions']['mean']
    std = self.thresholds['sessions']['std']
    
    if sessions > mean + 3 * std:
        sigma = (sessions - mean) / std
        warnings.append(f"⚠️ Аномально высокие сессии: {sessions} (норма {mean:.0f}±{std:.0f}, σ={sigma:.1f})")
    elif sessions < mean - 3 * std:
        sigma = (mean - sessions) / std
        warnings.append(f"⚠️ Аномально низкие сессии: {sessions} (норма {mean:.0f}±{std:.0f}, σ={sigma:.1f})")
    
    return warnings
```

**Что делает:**
- Сравнивает текущие значения с порогами
- Если значение выходит за границы ±3σ, добавляет предупреждение

**Пример:**
```python
sessions = 42
mean = 150
std = 25
# sessions < 150 - 75 → аномалия
# sigma = (150 - 42) / 25 = 4.32σ
```

---

### 2.5. Проверка моделью Isolation Forest

```python
def check_ml_model(self, features_df: pd.DataFrame) -> tuple:
    """
    Проверка текущих данных моделью Isolation Forest
    """
    X = features_df[self.feature_names].values
    X_scaled = self.scaler.transform(X)
    
    # Предсказание (-1 = аномалия, 1 = норма)
    prediction = self.model.predict(X_scaled)[0]
    
    # Оценка аномальности (чем меньше, тем более аномально)
    score = self.model.decision_function(X_scaled)[0]
    
    is_anomaly = (prediction == -1)
    
    return is_anomaly, score
```

**Что делает:**
- Подает текущие данные в обученную модель
- Получает предсказание и оценку аномальности

**Интерпретация:**
- `prediction = -1` → аномалия
- `prediction = 1` → норма
- `score` — чем меньше, тем более аномально

---

### 2.6. Полный цикл проверки

```python
def detect(self):
    """
    Основной метод проверки
    """
    # Получаем текущие метрики
    metrics = self.get_current_metrics()
    
    if not metrics:
        return
    
    # Проверка по правилу 3-сигма
    warnings = self.check_3sigma(metrics)
    
    # Подготовка признаков для ML
    features = self.prepare_current_features(metrics)
    
    # Проверка моделью
    is_anomaly, score = self.check_ml_model(features)
    
    if is_anomaly:
        warnings.append(f"🤖 ML модель: аномалия (score: {score:.3f})")
    
    # Сохранение результата
    self.save_to_db(metrics, warnings, is_anomaly)
    
    # Оповещение
    if warnings:
        self.send_alerts(metrics, warnings)
```

**Что делает:**
- Запускает все проверки последовательно
- Собирает предупреждения из разных источников
- Сохраняет результаты и отправляет уведомления

---

## Примеры работы

### Пример 1: Норма

```python
metrics = {
    'sessions': 145,
    'users': 87,
    'duration': 2345,
    'deadlocks': 0,
    'exceptions': 2
}

# 3-сигма: 145 в пределах 75-225 → норма
# ML модель: score = 0.62 → норма

# Результат: аномалий нет
```

### Пример 2: Падение активности

```python
metrics = {
    'sessions': 42,
    'users': 23,
    'duration': 2345,
    'deadlocks': 0,
    'exceptions': 1
}

# 3-сигма: 42 < 75 → аномалия (σ=4.32)
# ML модель: score = -0.345 → аномалия

# Результат:
warnings = [
    "⚠️ Аномально низкие сессии: 42 (норма 150±25, σ=4.3)",
    "🤖 ML модель: аномалия (score: -0.345)"
]
```

### Пример 3: Всплеск активности

```python
metrics = {
    'sessions': 312,
    'users': 187,
    'duration': 4567,
    'deadlocks': 2,
    'exceptions': 8
}

# 3-сигма: 312 > 225 → аномалия (σ=6.48)
# ML модель: score = -0.567 → аномалия

# Результат:
warnings = [
    "⚠️ Аномально высокие сессии: 312 (норма 150±25, σ=6.5)",
    "🤖 ML модель: аномалия (score: -0.567)"
]
```

---

## Параметры для настройки

В файле `.env`:

```env
# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=techlog

# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=monitoring
DB_USER=postgres
DB_PASSWORD=password

# Telegram
TELEGRAM_TOKEN=1234567890:ABCdefGHIjkl
TELEGRAM_CHAT_ID=-123456789

# Параметры модели
ANOMALY_CONTAMINATION=0.05
SIGMA_THRESHOLD=3
CRITICAL_SIGMA=5
```
