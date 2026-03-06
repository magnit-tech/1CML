Анализ медленных запросов в 1С
Поиск проблемных запросов и рекомендации по индексам

Идея для будущей реализации
Запуск: раз в день


🔍 Анализ медленных запросов за март 2026

┌─────────────────────────────────────────────────────────────────┐
│ Топ-5 самых медленных запросов                                  │
├─────────────────────────────────────────────────────────────────┤
│ 1. "Оборотно-сальдовая ведомость"                               │
│    • Среднее время: 45 секунд (+120% за месяц)                  │
│    • Таблицы: _AccumRgTurnover, _InfoRg                         │
│    • Используемые индексы: нет                                  │
│    • ⚠️ Рост: 120% за месяц                                    │
│    • 💡 Рекомендация: создать индекс по периоду                 │
│                                                                 │
│ 2. "Анализ продаж"                                              │
│    • Среднее время: 23 секунды (стабильно)                      │
│    • Таблицы: _DocumentSales, _RegisterSales                    │
│    • Используемые индексы: IX_Sales_Date                        │
│    • ✅ Стабильно                                               │
│    • 💡 Рекомендация: обновить статистику таблиц                │
└─────────────────────────────────────────────────────────────────┘

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import IsolationForest
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SlowQueryAnalyzer:
    
    Анализ медленных запросов и генерация рекомендаций
    
    def __init__(self):
        self.kmeans = None
        self.vectorizer = None
        self.outlier_detector = None
        
    def load_queries_from_clickhouse(self, days=30):
     
        Загрузка данных о запросах из ClickHouse
     
        # TODO: запрос к ClickHouse
        # SELECT 
        #     event_date,
        #     query_hash,
        #     query_text,
        #     duration,
        #     table_names,
        #     index_used,
        #     rows_processed
        # FROM slow_queries
        # WHERE duration > 5000000  -- больше 5 секунд
        pass
    
    def extract_tables_from_query(self, query_text):
       
        Извлечение имен таблиц из текста запроса
      
        # Простой regex для поиска таблиц
        # TODO: улучшить парсинг
        tables = re.findall(r'_([A-Za-z]+[0-9]*)', query_text)
        return list(set(tables))
    
    def extract_conditions(self, query_text):
        
        Извлечение условий WHERE для анализа потенциальных индексов
        
        # TODO: реализовать парсинг условий
        pass
    
    def vectorize_queries(self, queries_df):
       
        Векторизация текста запросов для кластеризации
  
        self.vectorizer = TfidfVectorizer(
            max_features=100,
            stop_words=['select', 'from', 'where', 'and', 'or', 'inner', 'left', 'join']
        )
        
        X = self.vectorizer.fit_transform(queries_df['query_text'])
        return X
    
    def cluster_queries(self, X, n_clusters=10):
       
        Кластеризация похожих запросов
       
        self.kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        clusters = self.kmeans.fit_predict(X)
        return clusters
    
    def detect_outliers(self, queries_df, duration_col='duration'):
        
        Поиск выбросов по времени выполнения
      
        self.outlier_detector = IsolationForest(contamination=0.1, random_state=42)
        X = queries_df[[duration_col]].values
        outliers = self.outlier_detector.fit_predict(X)
        return outliers  # -1 = аномалия, 1 = норма
    
    def generate_index_recommendation(self, query_df):
        
        Генерация рекомендаций по индексам
       
        # TODO: анализ планов запросов и рекомендации
        recommendations = []
        
        # Анализ таблиц без индексов
        no_index_queries = query_df[query_df['index_used'] == '']
        if len(no_index_queries) > 0:
            tables = set()
            for q in no_index_queries['query_text']:
                tables.update(self.extract_tables_from_query(q))
            
            recommendations.append({
                'type': 'missing_index',
                'tables': list(tables),
                'message': f"Создать индексы для таблиц: {', '.join(list(tables)[:5])}"
            })
        
        # Анализ устаревшей статистики
        old_stats_queries = query_df[query_df['stats_age_days'] > 7]
        if len(old_stats_queries) > 0:
            recommendations.append({
                'type': 'old_statistics',
                'message': "Обновить статистику для часто используемых таблиц"
            })
        
        return recommendations
    
    def analyze(self, df):
      
        Полный анализ медленных запросов
        
        # Векторизация и кластеризация
        X = self.vectorize_queries(df)
        df['cluster'] = self.cluster_queries(X)
        
        # Поиск выбросов
        df['is_outlier'] = self.detect_outliers(df)
        
        # Статистика по кластерам
        cluster_stats = df.groupby('cluster').agg({
            'duration': ['mean', 'max', 'count'],
            'query_text': 'count'
        }).round(2)
        
        logger.info("Статистика по кластерам запросов:")
        logger.info(cluster_stats)
        
        # Поиск самого медленного кластера
        slowest_cluster = cluster_stats['duration']['mean'].idxmax()
        logger.info(f"Самый медленный кластер: {slowest_cluster}")
        
        # Генерация рекомендаций
        recommendations = self.generate_index_recommendation(df)
        
        return {
            'cluster_stats': cluster_stats,
            'slowest_cluster': slowest_cluster,
            'recommendations': recommendations,
            'data': df
        }


def main():
    
    Точка входа для тестирования
    
    analyzer = SlowQueryAnalyzer()
    
    # TODO: загрузить данные
    # df = analyzer.load_queries_from_clickhouse()
    
    # TODO: выполнить анализ
    # results = analyzer.analyze(df)
    
    logger.info("Модуль в разработке. См. docs/future_1C_ML.md для деталей.")


if __name__ == "__main__":
    main()
