Прогнозирование длительности выполнения типовых операций в 1С
(закрытие месяца, расчет себестоимости, обмен с банком)

Идея для будущей реализации
Запуск: раз в день (рекомендуется перед выполнением регламентных операций)

"""
📊 Прогноз времени закрытия месяца (апрель 2026)

┌─────────────────────────────────────────────────────────────────┐
│ Параметр                | Значение                              │
│─────────────────────────|───────────────────────────────────────│
│ Среднее за 12 месяцев   | 2.5 часа                             │
│ Прогноз на этот месяц   | 3.8 часа (+52%)                      │
│─────────────────────────|───────────────────────────────────────│
│ Факторы роста:                                                  │
│   • Количество документов | +30%                                │
│   • Новый релиз платформы | версия 8.3.24                      │
│   • Количество активных   | +15%                                │
│     пользователей         |                                      │
└─────────────────────────────────────────────────────────────────┘

⚠️ ВНИМАНИЕ: прогноз превышает SLA (3 часа)
   Рекомендация: запустить закрытие месяца на 2 часа раньше
   Запланировать на: 30 апреля 2026, 06:00
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import joblib
import logging
from datetime import datetime, timedelta
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OperationDurationPredictor:
    """
    Прогнозирование длительности операций
    Модель: Random Forest
    """
    
    def __init__(self):
        self.model = None
        self.feature_names = [
            'month',                # месяц года (1-12)
            'day_of_month',         # день месяца
            'day_of_week',          # день недели (0-6)
            'documents_count',      # количество документов
            'lines_count',          # количество строк
            'is_weekend',           # выходной или нет
            'is_month_start',       # начало месяца (1-5 числа)
            'is_month_end',         # конец месяца (25-31 числа)
            'is_quarter_end',       # конец квартала
            'is_year_end',          # конец года
            'platform_version',     # версия платформы (числовой код)
            'config_version',       # версия конфигурации
            'active_users_count',   # активных пользователей
            'db_size_gb'            # размер базы данных
        ]
    
    def load_data_from_clickhouse(self, days=365):
        """
        Загрузка истории выполнения операций из ClickHouse
        """
        # TODO: реализовать запрос к ClickHouse
        # SELECT 
        #     event_date,
        #     operation_name,
        #     duration,
        #     documents_count,
        #     lines_count,
        #     platform_version
        # FROM operation_log
        # WHERE event_date >= NOW() - INTERVAL %s DAY
        pass
    
    def load_data_from_postgresql(self, days=365):
        """
        Загрузка данных из PostgreSQL (альтернативный источник)
        """
        # TODO: реализовать запрос к PostgreSQL
        pass
    
    def prepare_features(self, df):
        """
        Подготовка признаков для обучения
        """
        df = df.copy()
        
        # Временные признаки
        df['date'] = pd.to_datetime(df['date'])
        df['month'] = df['date'].dt.month
        df['day_of_month'] = df['date'].dt.day
        df['day_of_week'] = df['date'].dt.dayofweek
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        df['is_month_start'] = (df['day_of_month'] <= 5).astype(int)
        df['is_month_end'] = (df['day_of_month'] >= 25).astype(int)
        df['is_quarter_end'] = ((df['month'] % 3 == 0) & (df['day_of_month'] >= 25)).astype(int)
        df['is_year_end'] = ((df['month'] == 12) & (df['day_of_month'] >= 25)).astype(int)
        
        # Логарифмирование для нормализации
        df['log_documents'] = np.log1p(df['documents_count'])
        df['log_lines'] = np.log1p(df['lines_count'])
        df['log_db_size'] = np.log1p(df['db_size_gb'])
        
        return df
    
    def train(self, df, target_column='duration_minutes'):
        """
        Обучение модели Random Forest
        """
        # Подготовка признаков
        df = self.prepare_features(df)
        
        # Выбор признаков для обучения
        feature_cols = [col for col in self.feature_names if col in df.columns]
        X = df[feature_cols]
        y = df[target_column]
        
        # Разделение на train/test
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Обучение модели
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
        self.model.fit(X_train, y_train)
        
        # Оценка качества
        score = self.model.score(X_test, y_test)
        logger.info(f"Модель обучена. R² = {score:.3f}")
        
        # Важность признаков
        importance = pd.DataFrame({
            'feature': feature_cols,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        logger.info("Топ-5 важных признаков:")
        logger.info(importance.head().to_string())
        
        return self.model
    
    def predict(self, operation_date, documents_count, lines_count, 
                platform_version, config_version, active_users, db_size):
        """
        Прогноз длительности операции на конкретную дату
        """
        # TODO: реализовать прогноз
        pass
    
    def save_model(self, path='models/operation_duration_model.pkl'):
        """Сохранение модели"""
        if self.model:
            joblib.dump(self.model, path)
            logger.info(f"Модель сохранена в {path}")
    
    def load_model(self, path='models/operation_duration_model.pkl'):
        """Загрузка модели"""
        self.model = joblib.load(path)
        logger.info(f"Модель загружена из {path}")


def main():
    """Точка входа для тестирования"""
    predictor = OperationDurationPredictor()
    
    # TODO: загрузить данные
    # df = predictor.load_data_from_clickhouse()
    
    # TODO: обучить модель
    # predictor.train(df)
    
    logger.info("Модуль в разработке. См. docs/future_1C_ML.md для деталей.")


if __name__ == "__main__":
    main()
