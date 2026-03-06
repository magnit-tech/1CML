Прогнозирование пиковых нагрузок на сервер 1С
(количество пользователей, CPU, RAM, диск)

Идея для будущей реализации
Запуск: раз в день (прогноз на 7 дней вперед)

📈 Прогноз нагрузки на 30.04.2026 (предпраздничный день)

┌─────────────────────────────────────────────────────────────────┐
│ Время    | Пользователи | CPU   | RAM   | Статус                │
│──────────|──────────────|───────|───────|────────────────────── │
│ 08:00-09:00 | 280        | 45%   | 55%   | 🟢 Норма             │
│ 09:00-10:00 | 450 (+35%) | 85%   | 78%   | 🟡 Риск замедления   │
│ 10:00-11:00 | 420        | 82%   | 80%   | 🟡 Риск замедления   │
│ 11:00-12:00 | 380        | 70%   | 72%   | 🟢 Норма             │
│ 12:00-13:00 | 210        | 35%   | 45%   | 🟢 Норма (обед)      │
│ 13:00-14:00 | 350        | 65%   | 68%   | 🟢 Норма             │
│ 14:00-15:00 | 400        | 78%   | 75%   | 🟡 Риск замедления   │
│ 15:00-16:00 | 380        | 72%   | 70%   | 🟢 Норма             │
└─────────────────────────────────────────────────────────────────┘

⚠️ Пик нагрузки ожидается с 09:00 до 11:00 (450 пользователей)
   CPU достигнет 85% - возможно замедление работы

📋 Рекомендации:
   • Фоновые задания запускать с 13:00 до 15:00
   • Увеличить количество RAS процессов на утро
   • Предупредить пользователей о возможных замедлениях


import pandas as pd
import numpy as np
from prophet import Prophet
import logging
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ServerLoadPredictor:
  
    Прогнозирование нагрузки на сервер
    Модель: Prophet (учет сезонности)

    
    def __init__(self):
        self.models = {}  # отдельная модель для каждой метрики
        
    def load_metrics_from_prometheus(self, days=90):
      
        Загрузка метрик из Prometheus
      
        # TODO: запрос к Prometheus API
        # metrics: active_sessions, cpu_usage, memory_usage, disk_io
        pass
    
    def load_metrics_from_postgresql(self, days=90):
       
        Загрузка метрик из PostgreSQL (альтернативный источник)
      
        # TODO: запрос к PostgreSQL
        pass
    
    def prepare_data_for_prophet(self, df, metric_name):
        
        Подготовка данных для модели Prophet
        Prophet требует колонки: ds (дата), y (значение)
        
        prophet_df = pd.DataFrame()
        prophet_df['ds'] = df['timestamp']
        prophet_df['y'] = df[metric_name]
        return prophet_df
    
    def train_prophet_model(self, df, metric_name):
        
        Обучение модели Prophet для конкретной метрики
       
        # Подготовка данных
        prophet_df = self.prepare_data_for_prophet(df, metric_name)
        
        # Создание модели с учетом сезонности
        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=True,
            seasonality_mode='multiplicative',
            holidays_prior_scale=10.0,
            changepoint_prior_scale=0.05
        )
        
        # Добавление праздников РФ
        model.add_country_holidays(country_name='RU')
        
        # Обучение
        model.fit(prophet_df)
        
        self.models[metric_name] = model
        logger.info(f"Модель для {metric_name} обучена")
        
        return model
    
    def predict(self, metric_name, periods=7, freq='H'):
      
        Прогноз на periods дней вперед с частотой freq
       
        if metric_name not in self.models:
            raise ValueError(f"Модель для {metric_name} не обучена")
        
        model = self.models[metric_name]
        future = model.make_future_dataframe(periods=periods*24, freq='H')
        forecast = model.predict(future)
        
        return forecast
    
    def detect_risk_hours(self, forecast, cpu_threshold=80, sessions_threshold=400):
        
        Определение часов с риском перегрузки
       
        # TODO: реализовать анализ прогноза
        pass
    
    def plot_forecast(self, metric_name, forecast):
      
        Визуализация прогноза
       
        fig = self.models[metric_name].plot(forecast)
        plt.title(f'Прогноз {metric_name}')
        plt.savefig(f'plots/forecast_{metric_name}.png')
        plt.close()


def main():
   Точка входа для тестирования
    predictor = ServerLoadPredictor()
    
    # TODO: загрузить данные
    # df = predictor.load_metrics_from_prometheus()
    
    # TODO: обучить модели
    # for metric in ['sessions', 'cpu', 'memory']:
    #     predictor.train_prophet_model(df, metric)
    
    logger.info("Модуль в разработке. См. docs/future_1C_ML.md для деталей.")


if __name__ == "__main__":
    main()
