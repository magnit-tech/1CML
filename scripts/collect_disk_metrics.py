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
