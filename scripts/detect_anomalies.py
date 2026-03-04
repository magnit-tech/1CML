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
