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
