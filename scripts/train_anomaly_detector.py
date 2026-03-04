#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Обучение модели детектора аномалий на данных о сессиях
Использует Isolation Forest для поиска выбросов
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
import logging
from datetime import datetime
import os
import argparse
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/train_anomaly.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('train_anomaly')

class AnomalyDetectorTrainer:
    """Тренер модели детектора аномалий"""
    
    def __init__(self, contamination=0.05, random_state=42):
        """
        Args:
            contamination: ожидаемая доля аномалий в данных (по умолчанию 5%)
            random_state: для воспроизводимости
        """
        self.contamination = contamination
        self.random_state = random_state
        self.model = None
        self.scaler = StandardScaler()
        self.feature_names = None
        self.thresholds = {}  # пороги для каждой метрики (3 сигма)
        
    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Отбор и подготовка признаков для обучения
        
        Args:
            df: DataFrame с данными из prepare_training_data.py
            
        Returns:
            DataFrame только с признаками для обучения
        """
        # Выбираем признаки, которые будем использовать
        feature_columns = [
            'sessions', 'users', 'computers',
            'avg_duration', 'p95_duration',
            'deadlocks', 'exceptions', 'timeouts',
            'sessions_deviation', 'sessions_trend',
            'error_rate',
            'hour_sin', 'hour_cos',
            'log_sessions', 'log_duration'
        ]
        
        # Проверяем, что все колонки есть
        available_features = [col for col in feature_columns if col in df.columns]
        logger.info(f"Используемые признаки: {available_features}")
        
        # Сохраняем имена признаков
        self.feature_names = available_features
        
        # Заполняем пропуски
        X = df[available_features].fillna(0)
        
        # Сохраняем пороги для интерпретации
        for col in ['sessions', 'users', 'computers', 'avg_duration']:
            if col in df.columns:
                mean = df[col].mean()
                std = df[col].std()
                self.thresholds[col] = {
                    'mean': mean,
                    'std': std,
                    'upper_3sigma': mean + 3 * std,
                    'lower_3sigma': mean - 3 * std
                }
        
        return X
    
    def train(self, X: pd.DataFrame) -> tuple:
        """
        Обучение модели Isolation Forest
        
        Args:
            X: признаки для обучения
            
        Returns:
            tuple: (предсказания, оценки аномальности)
        """
        logger.info(f"Обучение модели на {len(X)} образцах, {len(X.columns)} признаках")
        
        # Нормализация данных
        X_scaled = self.scaler.fit_transform(X)
        
        # Обучение модели
        self.model = IsolationForest(
            contamination=self.contamination,
            random_state=self.random_state,
            n_estimators=100,
            max_samples='auto',
            bootstrap=False,
            n_jobs=-1
        )
        
        self.model.fit(X_scaled)
        
        # Предсказания и оценки
        predictions = self.model.predict(X_scaled)  # -1 = аномалия, 1 = норма
        scores = self.model.decision_function(X_scaled)  # чем меньше, тем более аномально
        
        # Статистика
        n_anomalies = sum(predictions == -1)
        logger.info(f"Модель обучена. Найдено аномалий: {n_anomalies} ({n_anomalies/len(X)*100:.1f}%)")
        
        return predictions, scores
    
    def analyze_results(self, df: pd.DataFrame, predictions: np.ndarray, scores: np.ndarray):
        """
        Анализ результатов обучения
        
        Args:
            df: исходный DataFrame с данными
            predictions: предсказания модели (-1 или 1)
            scores: оценки аномальности
        """
        # Добавляем результаты в DataFrame
        df_result = df.copy()
        df_result['prediction'] = predictions
        df_result['anomaly_score'] = scores
        
        # Аномалии
        anomalies = df_result[df_result['prediction'] == -1]
        
        logger.info(f"\n{'='*60}")
        logger.info("АНАЛИЗ РЕЗУЛЬТАТОВ ОБУЧЕНИЯ")
        logger.info(f"{'='*60}")
        logger.info(f"Всего записей: {len(df_result)}")
        logger.info(f"Норма: {sum(predictions == 1)} ({sum(predictions == 1)/len(predictions)*100:.1f}%)")
        logger.info(f"Аномалии: {len(anomalies)} ({len(anomalies)/len(predictions)*100:.1f}%)")
        
        if not anomalies.empty:
            logger.info(f"\nТоп-10 аномалий (по убыванию аномальности):")
            top_anomalies = anomalies.nsmallest(10, 'anomaly_score')
            
            for idx, row in top_anomalies.iterrows():
                date_str = f"{row['date']} {int(row['hour']):02d}:00"
                sessions = row['sessions']
                avg_sessions = self.thresholds.get('sessions', {}).get('mean', 0)
                logger.info(f"  {date_str} | сессии: {sessions} (норма: {avg_sessions:.0f}) | "
                           f"оценка: {row['anomaly_score']:.3f}")
        
        # Сохраняем график
        self._save_plot(df_result)
        
        return df_result
    
    def _save_plot(self, df: pd.DataFrame):
        """Сохранение графика с аномалиями"""
        try:
            plt.figure(figsize=(15, 6))
            
            # Нормальные точки
            normal = df[df['prediction'] == 1]
            plt.scatter(normal.index, normal['sessions'], 
                       c='green', alpha=0.5, label='Норма', s=20)
            
            # Аномалии
            anomalies = df[df['prediction'] == -1]
            plt.scatter(anomalies.index, anomalies['sessions'], 
                       c='red', alpha=0.8, label='Аномалия', s=50, marker='x')
            
            plt.title('Детектор аномалий сессий 1С (Isolation Forest)')
            plt.xlabel('Время (часы)')
            plt.ylabel('Количество сессий')
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            # Сохраняем
            plot_path = 'models/anomaly_plot.png'
            plt.savefig(plot_path)
            logger.info(f"График сохранен в {plot_path}")
            plt.close()
            
        except Exception as e:
            logger.error(f"Ошибка сохранения графика: {e}")
    
    def save_model(self, path: str):
        """Сохранение модели и скейлера"""
        if self.model is None:
            raise ValueError("Модель не обучена")
        
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'feature_names': self.feature_names,
            'thresholds': self.thresholds,
            'contamination': self.contamination,
            'train_date': datetime.now().isoformat()
        }
        
        joblib.dump(model_data, path)
        logger.info(f"Модель сохранена в {path}")
    
    def load_model(self, path: str):
        """Загрузка модели"""
        model_data = joblib.load(path)
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.feature_names = model_data['feature_names']
        self.thresholds = model_data.get('thresholds', {})
        logger.info(f"Модель загружена из {path}")

def main():
    parser = argparse.ArgumentParser(description='Обучение детектора аномалий')
    parser.add_argument('--input', default='training_data.csv', help='CSV с обучающими данными')
    parser.add_argument('--output', default='models/anomaly_model.pkl', help='Путь для сохранения модели')
    parser.add_argument('--contamination', type=float, default=0.05, help='Доля аномалий')
    parser.add_argument('--plot', action='store_true', help='Построить график')
    
    args = parser.parse_args()
    
    # Создаем директорию для моделей
    os.makedirs('models', exist_ok=True)
    
    # Загружаем данные
    logger.info(f"Загрузка данных из {args.input}")
    df = pd.read_csv(args.input, parse_dates=['date'])
    
    # Создаем и обучаем детектор
    trainer = AnomalyDetectorTrainer(contamination=args.contamination)
    
    # Подготовка признаков
    X = trainer.prepare_features(df)
    
    # Обучение
    predictions, scores = trainer.train(X)
    
    # Анализ
    trainer.analyze_results(df, predictions, scores)
    
    # Сохраняем модель
    trainer.save_model(args.output)
    
    # Выводим пороги для ручного контроля
    print("\nПороги 3-сигма для ключевых метрик:")
    for metric, thresholds in trainer.thresholds.items():
        print(f"  {metric}:")
        print(f"    среднее: {thresholds['mean']:.1f}")
        print(f"    std: {thresholds['std']:.1f}")
        print(f"    нижний порог: {thresholds['lower_3sigma']:.1f}")
        print(f"    верхний порог: {thresholds['upper_3sigma']:.1f}")

if __name__ == "__main__":
    main()
