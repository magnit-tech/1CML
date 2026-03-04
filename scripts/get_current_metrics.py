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
