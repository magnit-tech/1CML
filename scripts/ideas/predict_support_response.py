Прогнозирование времени ответа техподдержки и загрузки

Идея для будущей реализации
Запуск: раз в день (прогноз на неделю)

📞 Прогноз обращений в техподдержку на апрель 2026

┌─────────────────────────────────────────────────────────────────┐
│ День недели | Среднее | Прогноз | Изменение | Пиковые часы     │
│─────────────|─────────|─────────|───────────|──────────────────│
│ Понедельник | 48      | 52      | +8%       | 09:00-11:00      │
│ Вторник     | 45      | 47      | +4%       | 10:00-12:00      │
│ Среда       | 42      | 44      | +5%       | 11:00-13:00      │
│ Четверг     | 38      | 40      | +5%       | 14:00-16:00      │
│ Пятница     | 35      | 38      | +9%       | 15:00-17:00      │
└─────────────────────────────────────────────────────────────────┘

⚠️ 1 апреля: закрытие квартала (+40% обращений)
   Рекомендация: усилить поддержку в эти часы


import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from prophet import Prophet
import logging
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SupportLoadPredictor:
    
    Прогнозирование загрузки техподдержки
   
    
    def __init__(self):
        self.arima_model = None
        self.prophet_model = None
        
    def load_tickets_from_itsm(self, days=365):
        
        Загрузка истории обращений из ITSM (Jira, YouTrack)
        
        # TODO: запрос к ITSM API
        # SELECT 
        #     created_date,
        #     resolved_date,
        #     first_response_time,
        #     resolution_time,
        #     priority,
        #     category,
        #     assignee
        # FROM support_tickets
        # WHERE created_date >= NOW() - INTERVAL '%s days'
        pass
    
    def prepare_daily_tickets(self, df):
       
        Подготовка ежедневной статистики
        
        df['date'] = pd.to_datetime(df['created_date']).dt.date
        daily = df.groupby('date').agg({
            'ticket_id': 'count',
            'first_response_time': 'mean',
            'resolution_time': 'mean'
        }).rename(columns={'ticket_id': 'tickets_count'})
        
        return daily
    
    def train_arima(self, daily_df):
        
        Обучение ARIMA модели для прогноза количества обращений
       
        # Подготовка временного ряда
        ts = daily_df['tickets_count'].values
        
        # Поиск параметров (можно автоматизировать)
        # p, d, q = 7, 1, 7  # недельная сезонность
        
        self.arima_model = ARIMA(ts, order=(7, 1, 7))
        self.arima_model_fit = self.arima_model.fit()
        
        logger.info("ARIMA модель обучена")
        logger.info(self.arima_model_fit.summary())
        
        return self.arima_model_fit
    
    def train_prophet(self, daily_df):
        
        Обучение Prophet модели с учетом праздников
        
        # Подготовка данных для Prophet
        prophet_df = pd.DataFrame()
        prophet_df['ds'] = pd.to_datetime(daily_df.index)
        prophet_df['y'] = daily_df['tickets_count'].values
        
        # Создание модели
        self.prophet_model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
            seasonality_mode='multiplicative',
            holidays_prior_scale=10.0
        )
        
        # Добавление праздников РФ
        self.prophet_model.add_country_holidays(country_name='RU')
        
        # Добавление специальных событий (закрытие квартала и т.д.)
        quarter_ends = pd.DataFrame({
            'holiday': 'quarter_end',
            'ds': pd.date_range(start='2024-01-01', end='2026-12-31', freq='Q'),
            'lower_window': -2,
            'upper_window': 2
        })
        
        self.prophet_model.add_country_holidays(country_name='RU')
        for _, row in quarter_ends.iterrows():
            self.prophet_model.add_seasonality(
                name=row['holiday'],
                period=1,
                fourier_order=3
            )
        
        # Обучение
        self.prophet_model.fit(prophet_df)
        
        logger.info("Prophet модель обучена")
        
        return self.prophet_model
    
    def predict_arima(self, steps=30):
        
        Прогноз на steps дней вперед (ARIMA)
        
        if not self.arima_model_fit:
            raise ValueError("ARIMA модель не обучена")
        
        forecast = self.arima_model_fit.forecast(steps=steps)
        conf_int = self.arima_model_fit.get_forecast(steps=steps).conf_int()
        
        return forecast, conf_int
    
    def predict_prophet(self, periods=30):
        
        Прогноз на periods дней вперед (Prophet)
        
        if not self.prophet_model:
            raise ValueError("Prophet модель не обучена")
        
        future = self.prophet_model.make_future_dataframe(periods=periods)
        forecast = self.prophet_model.predict(future)
        
        return forecast
    
    def predict_response_time(self, df):
        
        Прогноз времени ответа на основе приоритета и загрузки
       
        # TODO: реализовать модель для прогноза времени ответа
        # с учетом приоритета, категории, текущей загрузки
        pass
    
    def detect_bottlenecks(self, forecast, threshold=50):
        
        Поиск дней с пиковой нагрузкой
       
        bottlenecks = []
        
        for i, pred in enumerate(forecast):
            date = datetime.now() + timedelta(days=i)
            
            if pred > threshold:
                bottlenecks.append({
                    'date': date,
                    'forecast': pred,
                    'threshold': threshold,
                    'recommendation': f"Усилить поддержку на {pred-threshold:.0f} обращений"
                })
        
        return bottlenecks
    
    def generate_weekly_report(self, forecast, response_times=None):
        
        Генерация недельного отчета
       
        # TODO: формирование отчета
        pass


def main():
 
    Точка входа для тестирования
    
    predictor = SupportLoadPredictor()
    
    # TODO: загрузить данные
    # df = predictor.load_tickets_from_itsm()
    # daily = predictor.prepare_daily_tickets(df)
    
    # TODO: обучить модели
    # predictor.train_prophet(daily)
    
    # TODO: сделать прогноз
    # forecast = predictor.predict_prophet(periods=30)
    
    # TODO: найти узкие места
    # bottlenecks = predictor.detect_bottlenecks(forecast['yhat'].values[-30:])
    
    logger.info("Модуль в разработке. См. docs/future_1C_ML.md для деталей.")


if __name__ == "__main__":
    main()
