#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Скрипт для проверки риска дедлоков и оповещения
Запуск: каждый час
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from clickhouse_driver import Client
import requests
import json
from pathlib import Path

# Добавляем пути
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/deadlocks.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('deadlock_checker')

# Загрузка переменных окружения
load_dotenv()

class DeadlockDetector:
    """Детектор риска дедлоков на основе техжурнала"""
    
    def __init__(self):
        """Инициализация подключения к ClickHouse"""
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.clickhouse_port = int(os.getenv('CLICKHOUSE_PORT', 9000))
        self.clickhouse_db = os.getenv('CLICKHOUSE_DB', 'techlog')
        
        # Пороги для алертов
        self.DEADLOCK_THRESHOLD = 1  # 1 дедлок в час - уже критично
        self.WAIT_TIME_THRESHOLD = 500000  # 500 мс - внимание
        self.WAIT_TIME_CRITICAL = 1000000  # 1 секунда - критично
        self.TREND_THRESHOLD = 50  # рост на 50% за неделю
        
        try:
            self.client = Client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                database=self.clickhouse_db
            )
            logger.info(f"Подключен к ClickHouse: {self.clickhouse_host}:{self.clickhouse_port}")
        except Exception as e:
            logger.error(f"Ошибка подключения к ClickHouse: {e}")
            sys.exit(1)
    
    def get_last_hour_locks(self):
        """
        Получение статистики по блокировкам за последний час
        """
        query = """
        SELECT 
            count() as total_locks,
            countIf(event_type = 'DEADLOCK') as deadlocks,
            countIf(event_type = 'TTIMEOUT') as timeouts,
            avg(lock_wait_time) as avg_wait_time,
            max(lock_wait_time) as max_wait_time,
            uniq(table_name) as tables_involved,
            uniq(session_id) as sessions_involved
        FROM lock_events
        WHERE event_datetime >= now() - interval 1 hour
        """
        
        result = self.client.execute(query)
        if result and result[0]:
            stats = {
                'total_locks': result[0][0],
                'deadlocks': result[0][1],
                'timeouts': result[0][2],
                'avg_wait_ms': result[0][3] / 1000 if result[0][3] else 0,
                'max_wait_ms': result[0][4] / 1000 if result[0][4] else 0,
                'tables_involved': result[0][5],
                'sessions_involved': result[0][6]
            }
            return stats
        return None
    
    def get_weekly_trend(self):
        """
        Получение тренда за последние 7 дней
        """
        query = """
        SELECT 
            toDate(event_datetime) as date,
            avg(lock_wait_time) as avg_wait,
            countIf(event_type = 'DEADLOCK') as deadlocks
        FROM lock_events
        WHERE event_datetime >= now() - interval 7 day
        GROUP BY date
        ORDER BY date
        """
        
        result = self.client.execute(query)
        if len(result) < 2:
            return None
        
        # Считаем среднее за первую половину и вторую половину
        mid = len(result) // 2
        first_half = [r[1] for r in result[:mid]]
        second_half = [r[1] for r in result[mid:]]
        
        if first_half and second_half:
            avg_first = sum(first_half) / len(first_half)
            avg_second = sum(second_half) / len(second_half)
            
            if avg_first > 0:
                trend_pct = ((avg_second - avg_first) / avg_first) * 100
            else:
                trend_pct = 0
            
            return {
                'trend_pct': trend_pct,
                'avg_first_ms': avg_first / 1000,
                'avg_second_ms': avg_second / 1000
            }
        return None
    
    def get_top_tables_last_hour(self, limit=10):
        """
        Получение топ таблиц по блокировкам за последний час
        """
        query = f"""
        SELECT 
            table_name,
            count() as lock_count,
            countIf(event_type = 'DEADLOCK') as deadlocks,
            avg(lock_wait_time) as avg_wait
        FROM lock_events
        WHERE event_datetime >= now() - interval 1 hour
          AND table_name != ''
        GROUP BY table_name
        ORDER BY lock_count DESC
        LIMIT {limit}
        """
        
        result = self.client.execute(query)
        tables = []
        for row in result:
            tables.append({
                'table': row[0],
                'lock_count': row[1],
                'deadlocks': row[2],
                'avg_wait_ms': row[3] / 1000 if row[3] else 0
            })
        return tables
    
    def analyze_risk(self):
        """
        Анализ риска дедлоков на основе всех метрик
        
        Returns:
            dict: результаты анализа
        """
        risk = {
            'timestamp': datetime.now().isoformat(),
            'level': 'normal',
            'score': 0,
            'warnings': [],
            'metrics': {},
            'tables': []
        }
        
        # 1. Статистика за последний час
        last_hour = self.get_last_hour_locks()
        if last_hour:
            risk['metrics']['last_hour'] = last_hour
            
            # Проверка на дедлоки
            if last_hour['deadlocks'] >= self.DEADLOCK_THRESHOLD:
                risk['score'] += 50
                risk['warnings'].append({
                    'level': 'critical',
                    'message': f"Обнаружены deadlock'и: {last_hour['deadlocks']} за последний час!"
                })
            
            # Проверка времени ожидания
            if last_hour['max_wait_ms'] > self.WAIT_TIME_CRITICAL / 1000:
                risk['score'] += 30
                risk['warnings'].append({
                    'level': 'critical',
                    'message': f"Критическое время ожидания блокировки: {last_hour['max_wait_ms']:.0f} мс"
                })
            elif last_hour['avg_wait_ms'] > self.WAIT_TIME_THRESHOLD / 1000:
                risk['score'] += 15
                risk['warnings'].append({
                    'level': 'warning',
                    'message': f"Высокое среднее время ожидания: {last_hour['avg_wait_ms']:.0f} мс"
                })
        
        # 2. Тренд за неделю
        trend = self.get_weekly_trend()
        if trend:
            risk['metrics']['trend'] = trend
            
            if trend['trend_pct'] > 100:
                risk['score'] += 40
                risk['warnings'].append({
                    'level': 'critical',
                    'message': f"Рост времени ожидания на {trend['trend_pct']:.0f}% за неделю!"
                })
            elif trend['trend_pct'] > 50:
                risk['score'] += 20
                risk['warnings'].append({
                    'level': 'warning',
                    'message': f"Рост времени ожидания на {trend['trend_pct']:.0f}% за неделю"
                })
        
        # 3. Топ таблиц за последний час
        risk['tables'] = self.get_top_tables_last_hour(10)
        
        # Определение уровня риска
        if risk['score'] >= 70:
            risk['level'] = 'critical'
        elif risk['score'] >= 40:
            risk['level'] = 'high'
        elif risk['score'] >= 20:
            risk['level'] = 'warning'
        
        return risk
    
    def send_telegram_alert(self, risk):
        """Отправка алерта в Telegram"""
        telegram_token = os.getenv('TELEGRAM_TOKEN')
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not telegram_token or not chat_id:
            logger.warning("Telegram не настроен")
            return
        
        # Эмодзи для разных уровней
        emoji = {
            'critical': '🚨',
            'high': '⚠️',
            'warning': '⚡',
            'normal': '✅'
        }.get(risk['level'], '📢')
        
        # Формируем сообщение
        message = f"{emoji} **АНАЛИЗ БЛОКИРОВОК 1С**\n\n"
        message += f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        message += f"📊 Уровень риска: **{risk['level'].upper()}** (оценка: {risk['score']})\n\n"
        
        if risk['warnings']:
            message += "**⚠️ Предупреждения:**\n"
            for w in risk['warnings']:
                message += f"• {w['message']}\n"
            message += "\n"
        
        if risk['metrics'].get('last_hour'):
            m = risk['metrics']['last_hour']
            message += "**📈 За последний час:**\n"
            message += f"• Блокировок: {m['total_locks']}\n"
            message += f"• Дедлоков: {m['deadlocks']}\n"
            message += f"• Таймаутов: {m['timeouts']}\n"
            message += f"• Среднее ожидание: {m['avg_wait_ms']:.0f} мс\n"
            message += f"• Макс. ожидание: {m['max_wait_ms']:.0f} мс\n"
            message += "\n"
        
        if risk['tables']:
            message += "**📋 Топ таблиц по блокировкам:**\n"
            for t in risk['tables'][:5]:
                deadlock_mark = "🔴" if t['deadlocks'] > 0 else "⚪"
                message += f"{deadlock_mark} {t['table']}: {t['lock_count']} блокировок"
                if t['avg_wait_ms'] > 100:
                    message += f" ⏱️ {t['avg_wait_ms']:.0f} мс"
                message += "\n"
        
        try:
            url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
            response = requests.post(url, json={
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            })
            if response.status_code == 200:
                logger.info("Алерт отправлен в Telegram")
            else:
                logger.error(f"Ошибка Telegram: {response.text}")
        except Exception as e:
            logger.error(f"Ошибка отправки в Telegram: {e}")
    
    def create_jira_ticket(self, risk):
        """Создание задачи в Jira при высоком риске"""
        try:
            from scripts.itsm.jira_integration import JiraClient
            
            jira = JiraClient()
            
            # Определяем приоритет
            if risk['level'] == 'critical':
                priority = "Highest"
                summary = f"[КРИТИЧНО] Обнаружены deadlock'и в 1С"
            elif risk['level'] == 'high':
                priority = "High"
                summary = f"[СРОЧНО] Высокий риск дедлоков в 1С"
            else:
                priority = "Medium"
                summary = f"[ВНИМАНИЕ] Рост блокировок в 1С"
            
            # Формируем описание
            description = f"*Автоматически создано системой мониторинга 1CML*\n\n"
            description += f"**Проблема:** {risk['warnings'][0]['message'] if risk['warnings'] else 'Обнаружен рост блокировок'}\n\n"
            
            if risk['metrics'].get('last_hour'):
                m = risk['metrics']['last_hour']
                description += "**Метрики за последний час:**\n"
                description += f"• Дедлоки: {m['deadlocks']}\n"
                description += f"• Среднее время ожидания: {m['avg_wait_ms']:.0f} мс\n"
                description += f"• Макс. время ожидания: {m['max_wait_ms']:.0f} мс\n\n"
            
            if risk['tables']:
                description += "**Подозрительные таблицы:**\n"
                for t in risk['tables'][:5]:
                    if t['deadlocks'] > 0 or t['avg_wait_ms'] > 500:
                        description += f"• {t['table']}: {t['lock_count']} блокировок, {t['avg_wait_ms']:.0f} мс\n"
                description += "\n"
            
            description += "**Рекомендации:**\n"
            description += "1. Проверить индексы для указанных таблиц\n"
            description += "2. Проанализировать длительные транзакции\n"
            description += "3. Оптимизировать запросы к конфликтующим таблицам\n"
            
            # Создаем задачу
            issue_key = jira.create_issue(
                summary=summary,
                description=description,
                priority=priority
            )
            
            if issue_key:
                logger.info(f"Создана задача в Jira: {issue_key}")
                return issue_key
            
        except Exception as e:
            logger.error(f"Ошибка создания задачи в Jira: {e}")
        
        return None
    
    def save_to_postgresql(self, risk):
        """Сохранение результата в PostgreSQL"""
        try:
            import psycopg2
            
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
                CREATE TABLE IF NOT EXISTS deadlock_checks (
                    id SERIAL PRIMARY KEY,
                    check_time TIMESTAMP,
                    risk_level VARCHAR(20),
                    risk_score INTEGER,
                    deadlocks_last_hour INTEGER,
                    avg_wait_ms FLOAT,
                    max_wait_ms FLOAT,
                    trend_pct FLOAT,
                    top_tables TEXT,
                    warnings TEXT
                )
            """)
            
            # Вставляем данные
            m = risk['metrics'].get('last_hour', {})
            trend = risk['metrics'].get('trend', {})
            
            cur.execute("""
                INSERT INTO deadlock_checks 
                (check_time, risk_level, risk_score, deadlocks_last_hour, 
                 avg_wait_ms, max_wait_ms, trend_pct, top_tables, warnings)
                VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                risk['level'],
                risk['score'],
                m.get('deadlocks', 0),
                m.get('avg_wait_ms', 0),
                m.get('max_wait_ms', 0),
                trend.get('trend_pct', 0),
                json.dumps(risk['tables']),
                json.dumps(risk['warnings'])
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info("Результат сохранен в PostgreSQL")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения в PostgreSQL: {e}")
    
    def run(self):
        """Основной метод запуска"""
        logger.info("=" * 60)
        logger.info("ЗАПУСК АНАЛИЗА БЛОКИРОВОК")
        
        # Анализируем риск
        risk = self.analyze_risk()
        
        # Логируем результаты
        logger.info(f"Уровень риска: {risk['level'].upper()} (оценка: {risk['score']})")
        
        if risk['warnings']:
            logger.info("Предупреждения:")
            for w in risk['warnings']:
                logger.info(f"  {w['level']}: {w['message']}")
        
        if risk['metrics'].get('last_hour'):
            m = risk['metrics']['last_hour']
            logger.info(f"За последний час: блокировок {m['total_locks']}, "
                       f"дедлоков {m['deadlocks']}, "
                       f"среднее ожидание {m['avg_wait_ms']:.0f} мс")
        
        # Сохраняем результат
        self.save_to_postgresql(risk)
        
        # Отправляем алерты в зависимости от уровня риска
        if risk['level'] in ['critical', 'high']:
            self.send_telegram_alert(risk)
            self.create_jira_ticket(risk)
        elif risk['level'] == 'warning' and risk['score'] > 30:
            self.send_telegram_alert(risk)
        
        logger.info("АНАЛИЗ ЗАВЕРШЕН")
        logger.info("=" * 60)
        
        return risk

def main():
    """Точка входа"""
    detector = DeadlockDetector()
    detector.run()

if __name__ == "__main__":
    main()
