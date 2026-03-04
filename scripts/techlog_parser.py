#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Парсер техжурнала 1С в ClickHouse
Версия: 2.0 с поддержкой сессий
"""

import os
import re
import gzip
import glob
from datetime import datetime, timedelta
import logging
from pathlib import Path
import time
from typing import List, Dict, Optional
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/techlog_parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('techlog_parser')

class TechLogParser:
    """Парсер техжурнала 1С с поддержкой данных о сессиях"""
    
    # Регулярные выражения для извлечения полей
    PATTERNS = {
        # Основные поля
        'datetime': re.compile(r'^(\d{4})(\d{2})(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3})'),
        'event_type': re.compile(r',([A-Z_]+),'),
        
        # Данные сессии
        'session': re.compile(r'session=(\d+)'),
        'user': re.compile(r'userName="([^"]*)"'),
        'computer': re.compile(r'computerName="([^"]*)"'),
        'process': re.compile(r'processName=([^,]+)'),
        'app': re.compile(r'app-id="([^"]*)"'),
        
        # Метрики производительности
        'duration': re.compile(r'duration=(\d+)'),
        'lock_wait': re.compile(r'lockWaitTime=(\d+)'),
        'lock_time': re.compile(r'lockTime=(\d+)'),
        
        # Флаги событий
        'deadlock': re.compile(r'deadlock', re.IGNORECASE),
        'exception': re.compile(r'exception|error', re.IGNORECASE),
        'timeout': re.compile(r'ttimeout', re.IGNORECASE),
        
        # Контекст
        'connection': re.compile(r'connection=(\d+)'),
        'transaction': re.compile(r'transaction=(\d+)'),
        'dbms': re.compile(r'dbms="([^"]*)"'),
        'func': re.compile(r'func="([^"]*)"'),
    }
    
    def __init__(self, host='localhost', port=9000, database='techlog'):
        """
        Инициализация парсера
        
        Args:
            host: хост ClickHouse
            port: порт ClickHouse
            database: имя базы данных
        """
        self.client = Client(
            host=host,
            port=port,
            database=database,
            settings={'insert_quorum': 1, 'insert_quorum_timeout': 60000}
        )
        self.database = database
        self.batch_size = 10000  # вставляем по 10 тысяч записей
        self.stats = {
            'processed_files': 0,
            'processed_lines': 0,
            'inserted_rows': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        # Проверяем подключение
        self._test_connection()
    
    def _test_connection(self):
        """Проверка подключения к ClickHouse"""
        try:
            self.client.execute('SELECT 1')
            logger.info(f"Подключение к ClickHouse установлено: {self.client.connection.host}:{self.client.connection.port}")
        except Exception as e:
            logger.error(f"Ошибка подключения к ClickHouse: {e}")
            raise
    
    def parse_line(self, line: str) -> Optional[Dict]:
        """
        Парсинг одной строки техжурнала
        
        Args:
            line: строка лога
            
        Returns:
            словарь с данными или None при ошибке
        """
        try:
            line = line.strip()
            if not line:
                return None
            
            # Парсим дату и время
            dt_match = self.PATTERNS['datetime'].search(line)
            if not dt_match:
                return None
            
            year, month, day, hour, minute, second, ms = map(int, dt_match.groups())
            event_datetime = datetime(year, month, day, hour, minute, second, ms * 1000)
            
            # Парсим тип события
            event_type_match = self.PATTERNS['event_type'].search(line)
            event_type = event_type_match.group(1) if event_type_match else 'UNKNOWN'
            
            # Извлекаем все поля
            result = {
                'event_date': event_datetime.date(),
                'event_hour': hour,
                'event_minute': minute,
                'event_datetime': event_datetime,
                'event_type': event_type,
                'raw_line': line[:1000]  # ограничим длину
            }
            
            # Извлекаем данные сессии
            for key, pattern in self.PATTERNS.items():
                if key in ['datetime', 'event_type']:
                    continue
                
                match = pattern.search(line)
                if match:
                    value = match.group(1) if pattern.groups else match.group(0)
                    
                    # Преобразование типов
                    if key in ['session', 'duration', 'lock_wait', 'lock_time', 
                              'connection', 'transaction']:
                        try:
                            value = int(value)
                        except ValueError:
                            continue
                    elif key in ['deadlock', 'exception', 'timeout']:
                        value = 1  # флаг
                    
                    result[key] = value
            
            # Устанавливаем значения по умолчанию для отсутствующих полей
            for field in ['session', 'user', 'computer', 'duration', 'lock_wait', 
                         'lock_time', 'deadlock', 'exception', 'timeout']:
                if field not in result:
                    if field in ['deadlock', 'exception', 'timeout']:
                        result[field] = 0
                    elif field in ['duration', 'lock_wait', 'lock_time']:
                        result[field] = 0
                    elif field == 'session':
                        result[field] = 0
                    else:
                        result[field] = ''
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка парсинга строки: {line[:200]}... - {e}")
            self.stats['errors'] += 1
            return None
    
    def parse_file(self, file_path: str) -> int:
        """
        Парсинг одного файла техжурнала
        
        Args:
            file_path: путь к файлу
            
        Returns:
            количество вставленных записей
        """
        logger.info(f"Парсинг файла: {file_path}")
        file_stats = {'lines': 0, 'inserted': 0}
        
        # Определяем открывалку для gz или обычного файла
        open_func = gzip.open if file_path.endswith('.gz') else open
        mode = 'rt' if file_path.endswith('.gz') else 'r'
        
        batch = []
        
        try:
            with open_func(file_path, mode, encoding='utf-8', errors='ignore') as f:
                for line in f:
                    file_stats['lines'] += 1
                    
                    parsed = self.parse_line(line)
                    if parsed:
                        batch.append(parsed)
                        file_stats['inserted'] += 1
                        
                        # Вставляем батч
                        if len(batch) >= self.batch_size:
                            self._insert_batch(batch)
                            batch = []
                            
                            # Логируем прогресс
                            if file_stats['inserted'] % 100000 == 0:
                                logger.info(f"  Обработано {file_stats['inserted']} записей")
                
                # Вставляем остаток
                if batch:
                    self._insert_batch(batch)
            
            # Обновляем статистику
            self.stats['processed_files'] += 1
            self.stats['processed_lines'] += file_stats['lines']
            self.stats['inserted_rows'] += file_stats['inserted']
            
            elapsed = time.time() - self.stats['start_time']
            logger.info(f"Файл обработан: {file_stats['inserted']}/{file_stats['lines']} записей, "
                       f"всего: {self.stats['inserted_rows']}, ошибок: {self.stats['errors']}, "
                       f"скорость: {self.stats['inserted_rows']/elapsed:.0f} записей/сек")
            
            return file_stats['inserted']
            
        except Exception as e:
            logger.error(f"Ошибка обработки файла {file_path}: {e}")
            self.stats['errors'] += 1
            return 0
    
    def _insert_batch(self, batch: List[Dict]):
        """
        Вставка батча в ClickHouse
        
        Args:
            batch: список словарей с данными
        """
        if not batch:
            return
        
        try:
            # Подготовка данных для вставки
            data = []
            for record in batch:
                row = [
                    record['event_date'],
                    record['event_hour'],
                    record['event_minute'],
                    record['event_datetime'],
                    record['session'],
                    record['user'][:100],  # ограничим длину
                    record['computer'][:100],
                    record.get('process', '')[:50],
                    record.get('app', '')[:50],
                    record['duration'],
                    record['lock_wait'],
                    record['lock_time'],
                    record['deadlock'],
                    record['exception'],
                    record['timeout'],
                    record['raw_line']
                ]
                data.append(row)
            
            # Вставка
            self.client.execute(
                """
                INSERT INTO session_events (
                    event_date, event_hour, event_minute, event_datetime,
                    session_id, user_name, computer_name, process_name, app_name,
                    duration, lock_wait_time, lock_time,
                    is_deadlock, is_exception, is_timeout,
                    raw_line
                ) VALUES
                """,
                data
            )
            
            logger.debug(f"Вставлено {len(batch)} записей")
            
        except ClickHouseError as e:
            logger.error(f"Ошибка вставки в ClickHouse: {e}")
            self.stats['errors'] += len(batch)
            raise
    
    def process_directory(self, directory: str, pattern: str = '*.log', 
                         recursive: bool = True, max_workers: int = 4):
        """
        Обработка директории с файлами техжурнала
        
        Args:
            directory: путь к директории
            pattern: паттерн файлов
            recursive: рекурсивный обход
            max_workers: количество потоков
        """
        path = Path(directory)
        if not path.exists():
            logger.error(f"Директория не существует: {directory}")
            return
        
        # Собираем все файлы
        if recursive:
            files = list(path.rglob(pattern))
            files.extend(list(path.rglob(pattern + '.gz')))
        else:
            files = list(path.glob(pattern))
            files.extend(list(path.glob(pattern + '.gz')))
        
        logger.info(f"Найдено {len(files)} файлов для обработки")
        
        if not files:
            logger.warning("Нет файлов для обработки")
            return
        
        # Обрабатываем файлы в несколько потоков
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.parse_file, str(f)): f for f in files}
            
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Ошибка в {file_path}: {e}")
                    self.stats['errors'] += 1
        
        # Итоговая статистика
        elapsed = time.time() - self.stats['start_time']
        logger.info("=" * 60)
        logger.info("ИТОГОВАЯ СТАТИСТИКА:")
        logger.info(f"  Файлов обработано: {self.stats['processed_files']}")
        logger.info(f"  Строк обработано: {self.stats['processed_lines']}")
        logger.info(f"  Записей вставлено: {self.stats['inserted_rows']}")
        logger.info(f"  Ошибок: {self.stats['errors']}")
        logger.info(f"  Время: {elapsed:.0f} сек")
        logger.info(f"  Скорость: {self.stats['inserted_rows']/elapsed:.0f} записей/сек")
        logger.info("=" * 60)

def main():
    parser = argparse.ArgumentParser(description='Парсер техжурнала 1С в ClickHouse')
    parser.add_argument('--dir', required=True, help='Директория с техжурналом')
    parser.add_argument('--pattern', default='*.log', help='Паттерн файлов')
    parser.add_argument('--workers', type=int, default=4, help='Количество потоков')
    parser.add_argument('--host', default='localhost', help='Хост ClickHouse')
    parser.add_argument('--port', type=int, default=9000, help='Порт ClickHouse')
    parser.add_argument('--db', default='techlog', help='База данных')
    parser.add_argument('--create-tables', action='store_true', help='Создать таблицы перед запуском')
    
    args = parser.parse_args()
    
    # Создаем таблицы, если нужно
    if args.create_tables:
        logger.info("Создание таблиц в ClickHouse...")
        # Здесь можно выполнить schema.sql
        
    # Запускаем парсер
    parser = TechLogParser(
        host=args.host,
        port=args.port,
        database=args.db
    )
    
    parser.process_directory(
        directory=args.dir,
        pattern=args.pattern,
        max_workers=args.workers
    )

if __name__ == "__main__":
    main()
