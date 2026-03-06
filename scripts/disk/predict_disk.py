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
