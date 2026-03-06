## Файлы документации для папки `docs/` (с кодом и пояснениями)

### 1. `docs/disk_prediction.md`

# Прогноз диска

**Файл:** `scripts/disk/predict_disk.py`

Компонент для прогнозирования заполнения диска с помощью линейной регрессии. Запускается ежедневно в 08:00.

---

## 1. Загрузка данных из PostgreSQL

```python
def load_history_from_db(self) -> pd.DataFrame:
    """
    Загрузка истории заполнения диска из PostgreSQL
    """
    conn = psycopg2.connect(**self.db_config)
    
    query = """
        SELECT date, used_gb
        FROM disk_usage
        WHERE disk_letter = %s
          AND date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY date
    """
    
    df = pd.read_sql_query(query, conn, params=('D:', 60), parse_dates=['date'])
    conn.close()
    
    return df
```

**Что делает:**
- Подключается к PostgreSQL
- Запрашивает данные за последние 60 дней для диска D:
- Возвращает DataFrame с колонками `date` и `used_gb`

**Пример результата:**
```
date        | used_gb
2026-01-01  | 100.0
2026-01-02  | 105.2
2026-01-03  | 111.5
```

---

## 2. Подготовка признаков

```python
def prepare_features(self, df: pd.DataFrame) -> tuple:
    """
    Преобразование дат в числовые признаки
    """
    df = df.sort_values('date').copy()
    
    # Дни от начала отсчета
    df['days_num'] = (df['date'] - df['date'].min()).dt.days
    
    X = df['days_num'].values.reshape(-1, 1)  # признаки
    y = df['used_gb'].values                   # целевая переменная
    last_day = df['days_num'].max()             # последний день
    
    return X, y, last_day
```

**Что делает:**
- Сортирует данные по дате
- Создает колонку `days_num` — количество дней от начала отсчета
- Формирует матрицу признаков `X` и целевую переменную `y`

**Пример преобразования:**
```
date       | used_gb | days_num
2026-01-01 | 100.0   | 0
2026-01-02 | 105.2   | 1
2026-01-03 | 111.5   | 2
```

---

## 3. Обучение модели линейной регрессии

```python
def train_model(self, X: np.ndarray, y: np.ndarray) -> dict:
    """
    Обучение модели и расчет метрик качества
    """
    # Разделение на train/test (80/20)
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    
    # Обучение модели
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Предсказание на тестовых данных
    y_pred = model.predict(X_test)
    
    # Метрики качества
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    self.model = model
    
    return {
        'mae': round(mae, 2),
        'r2': round(r2, 3),
        'growth_rate': round(model.coef_[0], 3),  # ГБ/день
        'intercept': round(model.intercept_, 2)
    }
```

**Что делает:**
- Делит данные на обучающие (80%) и тестовые (20%)
- Обучает модель линейной регрессии
- Считает метрики качества: MAE (средняя ошибка) и R² (качество модели)

**Математическая модель:** `used_gb = growth_rate × день + intercept`

**Пример результата:**
```python
{
    'mae': 1.2,           # модель ошибается в среднем на 1.2 ГБ
    'r2': 0.987,          # модель объясняет 98.7% изменений
    'growth_rate': 2.8,   # диск растет на 2.8 ГБ в день
    'intercept': 95.8      # начальный размер 95.8 ГБ
}
```

---

## 4. Прогнозирование

```python
def make_forecast(self, last_day: int, days_ahead: int) -> float:
    """
    Прогноз на days_ahead дней вперед
    """
    future_day = last_day + days_ahead
    forecast = self.model.predict([[future_day]])[0]
    return round(forecast, 2)
```

**Что делает:**
- Принимает последний день и количество дней для прогноза
- Подставляет будущий день в формулу модели
- Возвращает прогнозируемое значение

**Пример:**
```python
last_day = 59  # последний день в исторических данных
forecast_7d = make_forecast(last_day, 7)   # прогноз на 7 дней
forecast_14d = make_forecast(last_day, 14) # прогноз на 14 дней
forecast_30d = make_forecast(last_day, 30) # прогноз на 30 дней
```

---

## 5. Расчет дней до лимита

```python
def calculate_days_to_limit(self, current_usage: float) -> float:
    """
    Расчет количества дней до достижения критического порога
    """
    DISK_LIMIT = 200  # ГБ
    growth_rate = self.model.coef_[0]
    
    if growth_rate <= 0:
        return float('inf')  # диск не растет
    
    days = (DISK_LIMIT - current_usage) / growth_rate
    return round(max(0, days), 1)
```

**Что делает:**
- Берет текущее заполнение и скорость роста
- Считает, через сколько дней будет достигнут лимит

**Пример:**
```python
current_usage = 156.3
growth_rate = 2.8
days_to_limit = (200 - 156.3) / 2.8 = 15.6 дней
```

---

## 6. Проверка порогов

```python
def check_thresholds(self, current: float, forecasts: dict, days_to_limit: float) -> list:
    """
    Проверка порогов и формирование предупреждений
    """
    warnings = []
    DISK_LIMIT = 200
    
    # По дням до лимита
    if days_to_limit <= 7:
        warnings.append(f"🔴 КРИТИЧНО: диск заполнится через {days_to_limit} дней")
    elif days_to_limit <= 14:
        warnings.append(f"🟡 ВНИМАНИЕ: диск заполнится через {days_to_limit} дней")
    
    # По прогнозу на 14 дней
    if forecasts.get(14, 0) >= DISK_LIMIT:
        warnings.append("🔴 КРИТИЧНО: через 14 дней диск превысит лимит")
    
    # По текущему заполнению
    usage_percent = (current / DISK_LIMIT) * 100
    if usage_percent >= 90:
        warnings.append(f"🔴 КРИТИЧНО: диск заполнен на {usage_percent:.0f}%")
    elif usage_percent >= 80:
        warnings.append(f"🟡 ВНИМАНИЕ: диск заполнен на {usage_percent:.0f}%")
    
    return warnings
```

**Что делает:**
- Проверяет несколько условий
- Формирует список предупреждений разного уровня

**Пример результата:**
```python
[
    "🟡 ВНИМАНИЕ: диск заполнится через 16 дней",
    "🟡 ВНИМАНИЕ: диск заполнен на 78%"
]
```

---

## 7. Сохранение результатов

```python
def save_forecast_to_db(self, forecast_date, current_usage, forecasts, metrics, days_to_limit):
    """
    Сохранение прогноза в PostgreSQL
    """
    conn = psycopg2.connect(**self.db_config)
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO disk_forecast (
            metric_date, disk_letter, actual_used_gb,
            forecast_7d_gb, forecast_14d_gb, forecast_30d_gb,
            growth_rate_gb_per_day, days_to_limit, mae, r2
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        forecast_date.date(),
        'D:',
        current_usage,
        forecasts.get(7),
        forecasts.get(14),
        forecasts.get(30),
        metrics['growth_rate'],
        days_to_limit,
        metrics['mae'],
        metrics['r2']
    ))
    
    conn.commit()
    cur.close()
    conn.close()
```

**Что делает:**
- Сохраняет все рассчитанные значения в таблицу `disk_forecast`
- Данные будут использоваться для графиков в Grafana

---

## 8. Отправка алерта в Telegram

```python
def send_telegram_alert(self, message: str, level: str = 'warning'):
    """
    Отправка уведомления в Telegram
    """
    TOKEN = os.getenv('TELEGRAM_TOKEN')
    CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
    
    emoji = {'critical': '🔴', 'warning': '🟡', 'info': 'ℹ️'}.get(level, '📢')
    
    text = f"{emoji} **ПРОГНОЗ ДИСКА**\n\n{message}"
    
    requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={'chat_id': CHAT_ID, 'text': text, 'parse_mode': 'Markdown'}
    )
```

**Что делает:**
- Формирует сообщение с эмодзи в зависимости от уровня
- Отправляет POST-запрос к API Telegram

**Пример сообщения:**
```
🟡 **ПРОГНОЗ ДИСКА**

📊 Текущий объем: 156.3 ГБ
📈 Скорость роста: 2.8 ГБ/день
🔮 Через 14 дней: 195.1 ГБ
⚠️ Дней до лимита: 16
```

---

## 9. Полный цикл работы

```python
def run(self):
    """
    Основной метод запуска
    """
    # 1. Загрузка данных
    df = self.load_history_from_db()
    
    # 2. Подготовка признаков
    X, y, last_day = self.prepare_features(df)
    
    # 3. Обучение модели
    metrics = self.train_model(X, y)
    
    # 4. Прогнозы
    forecasts = {}
    for days in [7, 14, 30]:
        forecasts[days] = self.make_forecast(last_day, days)
    
    # 5. Дни до лимита
    current_usage = y[-1]
    days_to_limit = self.calculate_days_to_limit(current_usage)
    
    # 6. Проверка порогов
    warnings = self.check_thresholds(current_usage, forecasts, days_to_limit)
    
    # 7. Сохранение
    self.save_forecast_to_db(df['date'].max(), current_usage, 
                             forecasts, metrics, days_to_limit)
    
    # 8. Оповещение
    if warnings:
        message = self.format_message(warnings, current_usage, forecasts, days_to_limit)
        self.send_telegram_alert(message)
        
        if any('КРИТИЧНО' in w for w in warnings):
            self.create_jira_ticket(message)
```

**Что делает:**
- Запускает все шаги последовательно
- При обнаружении проблем отправляет уведомления

---

## Параметры для настройки

В файле `.env`:

```env
# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=monitoring
DB_USER=postgres
DB_PASSWORD=password

# Диск
DISK_LIMIT_GB=200
WARNING_THRESHOLD=0.8
CRITICAL_THRESHOLD=0.9

# Telegram
TELEGRAM_TOKEN=1234567890:ABCdefGHIjkl
TELEGRAM_CHAT_ID=-123456789

# Jira
JIRA_URL=https://your-domain.atlassian.net
JIRA_USERNAME=user@example.com
JIRA_API_TOKEN=token
JIRA_PROJECT_KEY=IT
```
```

---

