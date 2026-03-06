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
