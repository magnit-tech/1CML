Прогнозирование роста базы данных 1С по отдельным таблицам

Идея для будущей реализации
Запуск: раз в месяц


📀 Прогноз роста таблиц на 6 месяцев (до сентября 2026)

┌─────────────────────────────────────────────────────────────────┐
│ Таблица             | Текущий | Прогноз | Рост  | Статус        │
│─────────────────────|─────────|─────────|───────|──────────────│
│ _AccumRgTurnover    | 150 ГБ  | 240 ГБ  | +60%  | 🔴 Критично  │
│ _InfoRgPrices       | 80 ГБ   | 95 ГБ   | +19%  | 🟢 Норма      │
│ _DocumentSales      | 120 ГБ  | 210 ГБ  | +75%  | 🔴 Критично  │
│ _AccumRgSettlements | 200 ГБ  | 290 ГБ  | +45%  | 🟡 Внимание   │
└─────────────────────────────────────────────────────────────────┘

⚠️ Прогноз превышения: 15.08.2026
   💡 Рекомендация: партиционировать по месяцам

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.multioutput import MultiOutputRegressor
import logging
from datetime import datetime, timedelta
import joblib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DBGrowthPredictor:
    
    Прогнозирование роста таблиц базы данных
    Модель: Множественная линейная регрессия
 
    
    def __init__(self):
        self.models = {}  # отдельная модель для каждой таблицы
        self.disk_limit_gb = 300  # лимит диска
        
    def load_table_stats_from_postgresql(self, days=180):
     
        Загрузка статистики по таблицам из PostgreSQL
        
        # TODO: запрос к PostgreSQL
        # SELECT 
        #     date,
        #     table_name,
        #     size_gb,
        #     rows_count,
        #     avg_row_size_bytes,
        #     growth_per_day_gb
        # FROM table_stats
        # WHERE date >= NOW() - INTERVAL '%s days'
        pass
    
    def load_business_metrics(self, days=180):
       
        Загрузка бизнес-показателей (количество документов, новых контрагентов)
      
        # TODO: запрос к бизнес-данным
        pass
    
    def prepare_features(self, df):
        
        Подготовка признаков для прогноза
        
        df = df.copy()
        
        # Временные признаки
        df['date'] = pd.to_datetime(df['date'])
        df['days'] = (df['date'] - df['date'].min()).dt.days
        df['month'] = df['date'].dt.month
        df['quarter'] = df['date'].dt.quarter
        df['year'] = df['date'].dt.year
        
        # Лаги (размер месяц назад, квартал назад)
        df = df.sort_values('date')
        df['size_lag_30'] = df.groupby('table_name')['size_gb'].shift(30)
        df['size_lag_90'] = df.groupby('table_name')['size_gb'].shift(90)
        
        # Скользящие средние
        df['size_ma_30'] = df.groupby('table_name')['size_gb'].transform(
            lambda x: x.rolling(30, min_periods=1).mean()
        )
        
        return df
    
    def train_per_table(self, df):
      
        Обучение отдельной модели для каждой таблицы
        
        tables = df['table_name'].unique()
        
        for table in tables:
            table_df = df[df['table_name'] == table].copy()
            
            if len(table_df) < 60:  # минимум 60 дней истории
                logger.warning(f"Недостаточно данных для таблицы {table}")
                continue
            
            # Признаки
            table_df = self.prepare_features(table_df)
            feature_cols = ['days', 'month', 'quarter', 'size_lag_30', 'size_lag_90', 'size_ma_30']
            feature_cols = [col for col in feature_cols if col in table_df.columns]
            
            X = table_df[feature_cols].fillna(0).values
            y = table_df['size_gb'].values
            
            # Обучение
            model = LinearRegression()
            model.fit(X, y)
            
            self.models[table] = {
                'model': model,
                'feature_cols': feature_cols,
                'last_data': table_df.iloc[-1].to_dict()
            }
            
            logger.info(f"Модель для {table} обучена")
    
    def train_multioutput(self, df):
        
        Обучение единой модели для всех таблиц (MultiOutput)
        
        df = self.prepare_features(df)
        
        # Подготовка данных
        feature_cols = ['days', 'month', 'quarter']
        
        # Создаем матрицу признаков
        X = df[feature_cols].values
        
        # Создаем матрицу целевых переменных (по одной колонке на таблицу)
        pivot_df = df.pivot_table(
            index='date', 
            columns='table_name', 
            values='size_gb'
        ).fillna(method='ffill')
        
        y = pivot_df.values
        
        # Обучение
        self.multi_model = MultiOutputRegressor(LinearRegression())
        self.multi_model.fit(X[:len(y)], y)
        
        logger.info(f"MultiOutput модель обучена для {len(pivot_df.columns)} таблиц")
    
    def predict_table(self, table_name, days_ahead=180):
        
        Прогноз для конкретной таблицы
       
        if table_name not in self.models:
            raise ValueError(f"Модель для {table_name} не найдена")
        
        model_info = self.models[table_name]
        model = model_info['model']
        feature_cols = model_info['feature_cols']
        last_data = model_info['last_data']
        
        # Подготовка данных для прогноза
        predictions = []
        current_data = last_data.copy()
        
        for i in range(1, days_ahead + 1):
            # Формирование признаков
            future_date = pd.to_datetime(current_data['date']) + timedelta(days=1)
            features = {
                'days': current_data['days'] + i,
                'month': future_date.month,
                'quarter': future_date.quarter,
                'size_lag_30': predictions[-30] if len(predictions) >= 30 else current_data['size_gb'],
                'size_lag_90': predictions[-90] if len(predictions) >= 90 else current_data['size_gb'],
                'size_ma_30': np.mean(predictions[-30:]) if len(predictions) >= 30 else current_data['size_gb']
            }
            
            # Вектор признаков
            X_pred = np.array([[features[col] for col in feature_cols]])
            
            # Прогноз
            pred = model.predict(X_pred)[0]
            predictions.append(pred)
        
        return predictions
    
    def check_disk_limit(self, predictions, table_name):
        
        Проверка достижения лимита диска
     
        for i, size in enumerate(predictions):
            if size > self.disk_limit_gb:
                return {
                    'table': table_name,
                    'days_to_limit': i + 1,
                    'limit_date': (datetime.now() + timedelta(days=i + 1)).date(),
                    'size_at_limit': size
                }
        return None
    
    def generate_report(self):
       
        Генерация отчета по всем таблицам
       
        report = []
        
        for table_name in self.models:
            predictions = self.predict_table(table_name, days_ahead=180)
            current_size = self.models[table_name]['last_data']['size_gb']
            
            # Проверка лимита
            limit_check = self.check_disk_limit(predictions, table_name)
            
            report.append({
                'table': table_name,
                'current_size': current_size,
                'forecast_30d': predictions[29] if len(predictions) > 29 else None,
                'forecast_90d': predictions[89] if len(predictions) > 89 else None,
                'forecast_180d': predictions[179] if len(predictions) > 179 else None,
                'growth_rate': (predictions[29] - current_size) / 30 if len(predictions) > 29 else 0,
                'limit_check': limit_check
            })
        
        return pd.DataFrame(report)


def main():
    
    Точка входа для тестирования

    predictor = DBGrowthPredictor()
    
    # TODO: загрузить данные
    # df = predictor.load_table_stats_from_postgresql()
    
    # TODO: обучить модели
    # predictor.train_per_table(df)
    
    # TODO: сгенерировать отчет
    # report = predictor.generate_report()
    
    logger.info("Модуль в разработке. См. docs/future_1C_ML.md для деталей.")


if __name__ == "__main__":
    main()
