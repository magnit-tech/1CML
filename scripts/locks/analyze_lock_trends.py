#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Анализ трендов блокировок и расчет риска дедлоков
"""

import pandas as pd
import numpy as np
from clickhouse_driver import Client
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('lock_trends')

class LockTrendAnalyzer:
    """Анализатор трендов блокировок"""
    
    def __init__(self, host='localhost', port=9000, database='techlog'):
        self.client = Client(host=host, port=port, database=database)
        
        # Пороги для разных уровней риска
        self.thresholds = {
            'deadlock_warning': 1,      # 1 дедлок в час - уже проблема
            'timeout_warning': 10,       # 10 таймаутов в час
            'wait_time_critical': 1000000,  # 1 секунда ожидания
            'trend_warning': 50,          # рост на 50% за неделю
            'trend_critical': 100,         # рост на 100% за неделю
        }
    
    def get_daily_stats(self, days: int = 30) -> pd.DataFrame:
        """
        Получение дневной статистики блокировок
        
        Args:
            days: количество дней истории
            
        Returns:
            DataFrame с колонками: date, total_locks, deadlocks, timeouts,
            avg_wait_time, max_wait_time
        """
        query = f"""
        SELECT 
            event_date,
            count() as total_locks,
            countIf(event_type = 'DEADLOCK') as deadlocks,
            countIf(event_type = 'TTIMEOUT') as timeouts,
            avg(lock_wait_time) as avg_wait_time,
            max(lock_wait_time) as max_wait_time,
            quantile(0.95)(lock_wait_time) as p95_wait_time
        FROM lock_events
        WHERE event_date >= today() - {days}
        GROUP BY event_date
        ORDER BY event_date
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=[
            'date', 'total_locks', 'deadlocks', 'timeouts',
            'avg_wait', 'max_wait', 'p95_wait'
        ])
        
        logger.info(f"Загружена статистика за {len(df)} дней")
        return df
    
    def get_hourly_trend(self, days: int = 7) -> pd.DataFrame:
        """
        Получение почасового тренда для детального анализа
        
        Args:
            days: количество дней истории
            
        Returns:
            DataFrame с почасовой статистикой
        """
        query = f"""
        SELECT 
            toStartOfHour(event_datetime) as hour,
            count() as locks,
            countIf(event_type = 'DEADLOCK') as deadlocks,
            countIf(event_type = 'TTIMEOUT') as timeouts,
            avg(lock_wait_time) as avg_wait,
            max(lock_wait_time) as max_wait,
            uniq(session_id) as sessions,
            uniq(table_name) as tables_involved
        FROM lock_events
        WHERE event_datetime >= now() - interval {days} day
        GROUP BY hour
        ORDER BY hour
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=[
            'hour', 'locks', 'deadlocks', 'timeouts',
            'avg_wait', 'max_wait', 'sessions', 'tables'
        ])
        
        return df
    
    def get_top_tables(self, days: int = 1) -> pd.DataFrame:
        """
        Получение топ-таблиц по блокировкам
        
        Args:
            days: количество дней для анализа
            
        Returns:
            DataFrame с таблицами-лидерами по блокировкам
        """
        query = f"""
        SELECT 
            table_name,
            count() as lock_count,
            countIf(event_type = 'DEADLOCK') as deadlocks,
            countIf(event_type = 'TTIMEOUT') as timeouts,
            avg(lock_wait_time) as avg_wait,
            max(lock_wait_time) as max_wait,
            uniq(session_id) as sessions
        FROM lock_events
        WHERE event_date >= today() - {days}
          AND table_name != ''
        GROUP BY table_name
        ORDER BY lock_count DESC
        LIMIT 50
        """
        
        result = self.client.execute(query)
        df = pd.DataFrame(result, columns=[
            'table', 'lock_count', 'deadlocks', 'timeouts',
            'avg_wait', 'max_wait', 'sessions'
        ])
        
        return df
    
    def calculate_trends(self, df: pd.DataFrame) -> Dict:
        """
        Расчет трендов и метрик риска
        
        Args:
            df: DataFrame с дневной статистикой
            
        Returns:
            Словарь с метриками риска
        """
        if len(df) < 7:
            return {'error': 'Недостаточно данных'}
        
        # Сортируем по дате
        df = df.sort_values('date')
        
        # Берем первую неделю (базовый уровень) и последнюю неделю (текущий)
        base_week = df.iloc[:7]
        current_week = df.iloc[-7:]
        
        # Расчет метрик
        metrics = {
            'analysis_date': datetime.now().isoformat(),
            'days_analyzed': len(df),
            
            # Базовые метрики
            'base_avg_locks': base_week['total_locks'].mean(),
            'base_avg_deadlocks': base_week['deadlocks'].mean(),
            'base_avg_wait': base_week['avg_wait'].mean(),
            
            'current_avg_locks': current_week['total_locks'].mean(),
            'current_avg_deadlocks': current_week['deadlocks'].mean(),
            'current_avg_wait': current_week['avg_wait'].mean(),
            
            # Тренды
            'locks_trend_pct': self._calc_trend(
                base_week['total_locks'].mean(),
                current_week['total_locks'].mean()
            ),
            'deadlocks_trend_pct': self._calc_trend(
                base_week['deadlocks'].mean(),
                current_week['deadlocks'].mean()
            ),
            'wait_time_trend_pct': self._calc_trend(
                base_week['avg_wait'].mean(),
                current_week['avg_wait'].mean()
            ),
            
            # Пиковые значения
            'max_deadlocks_day': df['deadlocks'].max(),
            'max_deadlocks_date': df.loc[df['deadlocks'].idxmax(), 'date'].isoformat(),
            'max_wait_time': df['max_wait'].max(),
            'max_wait_date': df.loc[df['max_wait'].idxmax(), 'date'].isoformat(),
        }
        
        # Расчет уровня риска
        risk_score = 0
        risk_factors = []
        
        # 1. Дедлоки
        if metrics['current_avg_deadlocks'] > 0:
            risk_score += 30
            risk_factors.append(f"Есть дедлоки: {metrics['current_avg_deadlocks']:.1f} в день")
        elif metrics['deadlocks_trend_pct'] > 50:
            risk_score += 20
            risk_factors.append(f"Рост дедлоков на {metrics['deadlocks_trend_pct']:.0f}%")
        
        # 2. Время ожидания
        if metrics['current_avg_wait'] > self.thresholds['wait_time_critical']:
            risk_score += 25
            risk_factors.append(f"Критическое время ожидания: {metrics['current_avg_wait']/1000:.0f} мс")
        elif metrics['wait_time_trend_pct'] > self.thresholds['trend_critical']:
            risk_score += 20
            risk_factors.append(f"Рост времени ожидания на {metrics['wait_time_trend_pct']:.0f}%")
        elif metrics['wait_time_trend_pct'] > self.thresholds['trend_warning']:
            risk_score += 10
            risk_factors.append(f"Рост времени ожидания на {metrics['wait_time_trend_pct']:.0f}%")
        
        # 3. Общее количество блокировок
        if metrics['locks_trend_pct'] > self.thresholds['trend_critical']:
            risk_score += 15
            risk_factors.append(f"Рост числа блокировок на {metrics['locks_trend_pct']:.0f}%")
        elif metrics['locks_trend_pct'] > self.thresholds['trend_warning']:
            risk_score += 10
            risk_factors.append(f"Рост числа блокировок на {metrics['locks_trend_pct']:.0f}%")
        
        # Определение уровня риска
        if risk_score >= 50:
            risk_level = 'critical'
        elif risk_score >= 25:
            risk_level = 'high'
        elif risk_score >= 10:
            risk_level = 'warning'
        else:
            risk_level = 'normal'
        
        metrics['risk_score'] = risk_score
        metrics['risk_level'] = risk_level
        metrics['risk_factors'] = risk_factors
        
        return metrics
    
    def _calc_trend(self, base: float, current: float) -> float:
        """Расчет процентного изменения"""
        if base == 0:
            return 100.0 if current > 0 else 0.0
        return ((current - base) / base) * 100
    
    def predict_deadlock_risk(self, days_ahead: int = 7) -> Dict:
        """
        Прогноз риска дедлоков на основе трендов
        
        Args:
            days_ahead: на сколько дней вперед прогнозировать
            
        Returns:
            Словарь с прогнозом
        """
        # Получаем данные за последние 30 дней
        df = self.get_daily_stats(days=30)
        
        if len(df) < 14:
            return {'error': 'Недостаточно данных для прогноза'}
        
        # Простая линейная экстраполяция
        from sklearn.linear_model import LinearRegression
        
        # Подготовка данных
        X = np.arange(len(df)).reshape(-1, 1)
        y_locks = df['total_locks'].values
        y_wait = df['avg_wait'].values
        
        # Обучение моделей
        model_locks = LinearRegression()
        model_locks.fit(X, y_locks)
        
        model_wait = LinearRegression()
        model_wait.fit(X, y_wait)
        
        # Прогноз
        future_X = np.arange(len(df), len(df) + days_ahead).reshape(-1, 1)
        forecast_locks = model_locks.predict(future_X)
        forecast_wait = model_wait.predict(future_X)
        
        # Оценка риска
        risk_days = []
        for i in range(days_ahead):
            day_risk = {
                'day': i + 1,
                'forecast_locks': float(forecast_locks[i]),
                'forecast_wait': float(forecast_wait[i]),
                'deadlock_probability': min(100, float(forecast_wait[i] / 1000000 * 10))
            }
            risk_days.append(day_risk)
        
        # Когда ожидается первый дедлок (грубая оценка)
        days_to_deadlock = None
        for i, day in enumerate(risk_days):
            if day['deadlock_probability'] > 70:
                days_to_deadlock = i + 1
                break
        
        return {
            'forecast_date': (datetime.now() + timedelta(days=days_ahead)).isoformat(),
            'days_to_deadlock': days_to_deadlock,
            'risk_days': risk_days,
            'trend_locks': float(model_locks.coef_[0]),
            'trend_wait': float(model_wait.coef_[0])
        }

def main():
    """Основная функция"""
    analyzer = LockTrendAnalyzer()
    
    # Получаем статистику
    df = analyzer.get_daily_stats(days=30)
    
    # Анализируем тренды
    trends = analyzer.calculate_trends(df)
    
    print("\n" + "="*60)
    print("АНАЛИЗ БЛОКИРОВОК И РИСК ДЕДЛОКОВ")
    print("="*60)
    
    print(f"\n📊 Период анализа: {df['date'].min()} - {df['date'].max()}")
    print(f"\nТЕКУЩИЕ МЕТРИКИ (последние 7 дней):")
    print(f"  • Среднее число блокировок в день: {trends['current_avg_locks']:.0f}")
    print(f"  • Среднее число дедлоков в день: {trends['current_avg_deadlocks']:.2f}")
    print(f"  • Среднее время ожидания: {trends['current_avg_wait']/1000:.0f} мс")
    
    print(f"\n📈 ТРЕНДЫ (изменение за 30 дней):")
    print(f"  • Блокировки: {trends['locks_trend_pct']:+.1f}%")
    print(f"  • Дедлоки: {trends['deadlocks_trend_pct']:+.1f}%")
    print(f"  • Время ожидания: {trends['wait_time_trend_pct']:+.1f}%")
    
    print(f"\n⚠️ УРОВЕНЬ РИСКА: {trends['risk_level'].upper()}")
    print(f"  • Оценка риска: {trends['risk_score']}/100")
    if trends['risk_factors']:
        print("  • Факторы риска:")
        for factor in trends['risk_factors']:
            print(f"    - {factor}")
    
    # Прогноз
    if trends['risk_level'] in ['high', 'critical']:
        forecast = analyzer.predict_deadlock_risk(days_ahead=7)
        if forecast.get('days_to_deadlock'):
            print(f"\n🔮 ПРОГНОЗ:")
            print(f"  • Ожидаемый дедлок через {forecast['days_to_deadlock']} дней")
            print(f"  • Тренд роста блокировок: {forecast['trend_locks']:.1f} блокировок/день")
    
    # Топ таблиц
    print(f"\n📋 ТОП-10 ТАБЛИЦ ПО БЛОКИРОВКАМ (за последние 7 дней):")
    top_tables = analyzer.get_top_tables(days=7)
    for idx, row in top_tables.head(10).iterrows():
        deadlock_mark = "🔴" if row['deadlocks'] > 0 else "⚪"
        print(f"  {deadlock_mark} {row['table']}: {row['lock_count']} блокировок, "
              f"среднее ожидание {row['avg_wait']/1000:.0f} мс")
    
    print("="*60)

if __name__ == "__main__":
    main()
