Прогнозирование потребности в лицензиях и стоимости

Идея для будущей реализации
Запуск: раз в месяц

💰 Прогноз потребности в лицензиях на 2026-2027

┌─────────────────────────────────────────────────────────────────┐
│ Период       | Пользователи | Лицензий | Затраты | Статус      │
│──────────────|───────────────|──────────|─────────|────────────│
│ Текущий      | 450           | 500      | 900 тыс | 🟢 Норма    │
│ Q2 2026      | 480 (+7%)     | 520      | 936 тыс | 🟢 Норма    │
│ Q3 2026      | 520 (+15%)    | 550      | 990 тыс | 🟡 Внимание │
│ Q4 2026      | 580 (+29%)    | 600      | 1.08 млн | 🟡 Внимание │
│ Q1 2027      | 650 (+44%)    | 700      | 1.26 млн | 🔴 Критично │
└─────────────────────────────────────────────────────────────────┘

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
import logging
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LicenseCostPredictor:

    Прогнозирование потребности в лицензиях

    
    def __init__(self):
        self.user_model = None
        self.poly_features = None
        self.license_price = 1800  # цена лицензии в рублях (пример)
        
    def load_license_history(self, months=24):

        Загрузка истории использования лицензий

        # TODO: запрос к данным о лицензиях
        # SELECT 
        #     month,
        #     total_users,
        #     active_users,
        #     peak_users,
        #     licenses_purchased,
        #     licenses_cost,
        #     new_hires_count,
        #     department_growth
        # FROM license_stats
        # WHERE month >= NOW() - INTERVAL '%s months'
        pass
    
    def load_company_growth(self):
 
        Загрузка данных о росте компании (HR данные)
   
        # TODO: интеграция с HR системой
        pass
    
    def prepare_features(self, df):
   
        Подготовка признаков для прогноза
  
        df = df.copy()
        df['month_num'] = range(len(df))
        df['quarter'] = df['month'].dt.quarter
        df['year'] = df['month'].dt.year
        
        # Лаги
        df['users_lag_3'] = df['total_users'].shift(3)
        df['users_lag_6'] = df['total_users'].shift(6)
        df['users_lag_12'] = df['total_users'].shift(12)
        
        # Скользящие средние
        df['users_ma_3'] = df['total_users'].rolling(3, min_periods=1).mean()
        df['users_ma_6'] = df['total_users'].rolling(6, min_periods=1).mean()
        
        # Рост
        df['growth_rate'] = df['total_users'].pct_change() * 100
        
        return df
    
    def train_linear_model(self, df):
    
        Обучение линейной регрессии с полиномиальными признаками
     
        # Подготовка
        df = self.prepare_features(df)
        df = df.dropna()
        
        # Признаки
        feature_cols = ['month_num', 'users_lag_3', 'users_lag_6', 'users_ma_3']
        X = df[feature_cols].values
        y = df['total_users'].values
        
        # Полиномиальные признаки (для нелинейного роста)
        self.poly_features = PolynomialFeatures(degree=2, include_bias=False)
        X_poly = self.poly_features.fit_transform(X)
        
        # Обучение
        self.user_model = LinearRegression()
        self.user_model.fit(X_poly, y)
        
        # Оценка
        score = self.user_model.score(X_poly, y)
        logger.info(f"Модель обучена. R² = {score:.3f}")
        
        return self.user_model
    
    def predict_users(self, months_ahead=12):
      
        Прогноз количества пользователей
        
        if not self.user_model or not self.poly_features:
            raise ValueError("Модель не обучена")
        
        # TODO: подготовка данных для прогноза
        # predictions = []
        # for i in range(months_ahead):
        #     X_pred = ...
        #     pred = self.user_model.predict(X_pred)
        #     predictions.append(pred)
        
        # return predictions
        pass
    
    def calculate_license_needs(self, user_forecast, safety_factor=1.1):
        
        Расчет необходимого количества лицензий
     
        licenses_needed = [int(users * safety_factor) for users in user_forecast]
        return licenses_needed
    
    def calculate_costs(self, licenses_needed):
     
        Расчет затрат на лицензии
       
        costs = [licenses * self.license_price for licenses in licenses_needed]
        return costs
    
    def generate_report(self, user_forecast, licenses_needed, costs, months):
        
        Генерация отчета
        
        report = []
        
        for i, month in enumerate(months):
            report.append({
                'period': month.strftime('%Y-%m'),
                'users_forecast': user_forecast[i],
                'licenses_needed': licenses_needed[i],
                'cost': costs[i],
                'growth': (user_forecast[i] / user_forecast[0] - 1) * 100
            })
        
        return pd.DataFrame(report)
    
    def check_budget(self, report, current_budget):
      
        Проверка соответствия бюджету
       
        total_cost = report['cost'].sum()
        
        if total_cost > current_budget:
            deficit = total_cost - current_budget
            return {
                'status': 'critical',
                'total_cost': total_cost,
                'budget': current_budget,
                'deficit': deficit,
                'message': f"Превышение бюджета на {deficit:,.0f} руб."
            }
        else:
            surplus = current_budget - total_cost
            return {
                'status': 'ok',
                'total_cost': total_cost,
                'budget': current_budget,
                'surplus': surplus,
                'message': f"Бюджет соблюден, остаток {surplus:,.0f} руб."
            }
    
    def plot_forecast(self, historical_df, forecast_df):
        
        Визуализация прогноза
        
        plt.figure(figsize=(12, 6))
        
        # Исторические данные
        plt.plot(historical_df['month'], historical_df['total_users'], 
                 'b-', label='Исторические данные', linewidth=2)
        
        # Прогноз
        plt.plot(forecast_df['period'], forecast_df['users_forecast'], 
                 'r--', label='Прогноз', linewidth=2)
        
        # Доверительный интервал
        plt.fill_between(
            forecast_df['period'],
            [u * 0.9 for u in forecast_df['users_forecast']],
            [u * 1.1 for u in forecast_df['users_forecast']],
            color='r', alpha=0.2, label='Доверительный интервал'
        )
        
        plt.title('Прогноз роста числа пользователей 1С')
        plt.xlabel('Дата')
        plt.ylabel('Пользователи')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig('plots/license_forecast.png')
        plt.close()


def main():
    
    Точка входа для тестирования
    
    predictor = LicenseCostPredictor()
    
    # TODO: загрузить данные
    # df = predictor.load_license_history(months=24)
    
    # TODO: обучить модель
    # predictor.train_linear_model(df)
    
    # TODO: прогноз
    # user_forecast = predictor.predict_users(months_ahead=12)
    # licenses_needed = predictor.calculate_license_needs(user_forecast)
    # costs = predictor.calculate_costs(licenses_needed)
    
    # TODO: отчет
    # months = pd.date_range(start=datetime.now(), periods=12, freq='M')
    # report = predictor.generate_report(user_forecast, licenses_needed, costs, months)
    
    logger.info("Модуль в разработке. См. docs/future_1C_ML.md для деталей.")


if __name__ == "__main__":
    main()
