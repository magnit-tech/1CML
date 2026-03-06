Анализ эффективности сотрудников на основе данных техжурнала

Идея для будущей реализации
Запуск: раз в месяц

📊 Анализ эффективности сотрудников (отдел продаж)

┌─────────────────────────────────────────────────────────────────┐
│ Кластеры эффективности                                          │
├─────────────────────────────────────────────────────────────────┤
│ 🔵 Высокая эффективность (15 сотрудников)                      │
│    • Среднее: 78 документов/день                                │
│    • Время на документ: 2.1 мин                                 │
│    • Ошибки: 0.5 в день                                         │
│                                                                 │
│ 🟢 Средняя эффективность (45 сотрудников)                       │
│    • Среднее: 45 документов/день                                │
│    • Время на документ: 3.2 мин                                 │
│    • Ошибки: 1.2 в день                                         │
│                                                                 │
│ 🟡 Низкая эффективность (8 сотрудников)                         │
│    • Среднее: 23 документа/день                                 │
│    • Время на документ: 5.8 мин                                 │
│    • Ошибки: 3.5 в день                                         │
└─────────────────────────────────────────────────────────────────┘


import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
import logging
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UserEfficiencyAnalyzer:
  
    Анализ эффективности сотрудников
    
    
    def __init__(self):
        self.kmeans = None
        self.scaler = StandardScaler()
        self.outlier_detector = None
        
    def load_data_from_clickhouse(self, days=90):
        
        Загрузка данных из ClickHouse
      
        # TODO: запрос к ClickHouse
        # SELECT 
        #     user_name,
        #     department,
        #     role,
        #     date,
        #     documents_processed,
        #     avg_time_per_document,
        #     active_hours,
        #     error_count
        # FROM user_efficiency
        # WHERE date >= NOW() - INTERVAL '%s days'
        pass
    
    def prepare_features(self, df):
       
        Подготовка признаков для кластеризации
       
        # Группировка по пользователям
        user_stats = df.groupby('user_name').agg({
            'documents_processed': 'mean',
            'avg_time_per_document': 'mean',
            'active_hours': 'mean',
            'error_count': 'mean',
            'department': 'first',
            'role': 'first'
        }).reset_index()
        
        # Признаки для ML
        feature_cols = ['documents_processed', 'avg_time_per_document', 'active_hours', 'error_count']
        X = user_stats[feature_cols].values
        
        return user_stats, X, feature_cols
    
    def cluster_users(self, X, n_clusters=3):
        
        Кластеризация пользователей по эффективности
        
        # Нормализация
        X_scaled = self.scaler.fit_transform(X)
        
        # K-means
        self.kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        clusters = self.kmeans.fit_predict(X_scaled)
        
        return clusters
    
    def detect_outliers(self, X):
        
        Поиск выбросов (очень медленные или очень быстрые)
       
        X_scaled = self.scaler.transform(X)
        
        self.outlier_detector = IsolationForest(contamination=0.1, random_state=42)
        outliers = self.outlier_detector.fit_predict(X_scaled)
        
        return outliers  # -1 = аномалия, 1 = норма
    
    def analyze_clusters(self, user_stats, clusters):
        
        Анализ полученных кластеров
       
        user_stats['cluster'] = clusters
        
        cluster_stats = []
        for i in range(clusters.max() + 1):
            cluster_data = user_stats[user_stats['cluster'] == i]
            
            stats = {
                'cluster': i,
                'count': len(cluster_data),
                'avg_documents': cluster_data['documents_processed'].mean(),
                'avg_time': cluster_data['avg_time_per_document'].mean(),
                'avg_errors': cluster_data['error_count'].mean(),
                'avg_hours': cluster_data['active_hours'].mean()
            }
            cluster_stats.append(stats)
            
            logger.info(f"\nКластер {i} ({len(cluster_data)} чел.):")
            logger.info(f"  Документов: {stats['avg_documents']:.1f}/день")
            logger.info(f"  Время на документ: {stats['avg_time']:.1f} мин")
            logger.info(f"  Ошибок: {stats['avg_errors']:.1f}/день")
        
        return pd.DataFrame(cluster_stats)
    
    def find_best_practices(self, user_stats, top_cluster):
        
        Поиск лучших практик в самом продуктивном кластере
        
        best_users = user_stats[user_stats['cluster'] == top_cluster]
        
        # Анализ рабочих часов
        peak_hours = best_users['active_hours'].describe()
        
        # Анализ должностей
        role_stats = best_users['role'].value_counts()
        
        return {
            'peak_hours': peak_hours,
            'role_stats': role_stats,
            'avg_documents': best_users['documents_processed'].mean(),
            'avg_time': best_users['avg_time_per_document'].mean()
        }
    
    def find_needs_training(self, user_stats, outliers):
        
        Поиск сотрудников, нуждающихся в обучении
       
        user_stats['is_outlier'] = outliers
        
        # Выбросы с низкой эффективностью
        low_performers = user_stats[
            (user_stats['is_outlier'] == -1) & 
            (user_stats['documents_processed'] < user_stats['documents_processed'].median())
        ].sort_values('documents_processed')
        
        return low_performers
    
    def generate_recommendations(self, cluster_stats, best_practices, low_performers):
        
        Генерация рекомендаций
        
        recommendations = []
        
        # Определение лучшего кластера
        best_cluster = cluster_stats.loc[cluster_stats['avg_documents'].idxmax()]
        recommendations.append({
            'type': 'best_practice',
            'message': f"Лучшее время для работы: {best_practices['peak_hours']['mean']:.0f} часов в день"
        })
        
        # Рекомендации для отстающих
        for _, user in low_performers.head(5).iterrows():
            recommendations.append({
                'type': 'training',
                'user': user['user_name'],
                'role': user['role'],
                'message': f"Обучение для {user['user_name']}: "
                          f"{user['documents_processed']:.0f} док/день "
                          f"(среднее {best_cluster['avg_documents']:.0f})"
            })
        
        return recommendations
    
    def plot_clusters(self, user_stats):
       
        Визуализация кластеров
       
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        
        # Документы vs Время
        colors = ['blue', 'green', 'red']
        for i in range(3):
            cluster_data = user_stats[user_stats['cluster'] == i]
            axes[0].scatter(
                cluster_data['documents_processed'],
                cluster_data['avg_time_per_document'],
                c=colors[i],
                label=f'Кластер {i}',
                alpha=0.6
            )
        
        axes[0].set_xlabel('Документов в день')
        axes[0].set_ylabel('Время на документ (мин)')
        axes[0].legend()
        axes[0].grid(True, alpha=0.3)
        
        # Документы vs Ошибки
        for i in range(3):
            cluster_data = user_stats[user_stats['cluster'] == i]
            axes[1].scatter(
                cluster_data['documents_processed'],
                cluster_data['error_count'],
                c=colors[i],
                label=f'Кластер {i}',
                alpha=0.6
            )
        
        axes[1].set_xlabel('Документов в день')
        axes[1].set_ylabel('Ошибок в день')
        axes[1].legend()
        axes[1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('plots/user_efficiency_clusters.png')
        plt.close()


def main():
    
    Точка входа для тестирования
    
    analyzer = UserEfficiencyAnalyzer()
    
    # TODO: загрузить данные
    # df = analyzer.load_data_from_clickhouse()
    
    # TODO: подготовить признаки
    # user_stats, X, feature_cols = analyzer.prepare_features(df)
    
    # TODO: кластеризация
    # clusters = analyzer.cluster_users(X, n_clusters=3)
    # cluster_stats = analyzer.analyze_clusters(user_stats, clusters)
    
    # TODO: поиск выбросов
    # outliers = analyzer.detect_outliers(X)
    
    # TODO: лучшие практики
    # best_cluster = cluster_stats.loc[cluster_stats['avg_documents'].idxmax(), 'cluster']
    # best_practices = analyzer.find_best_practices(user_stats, best_cluster)
    
    # TODO: отстающие
    # low_performers = analyzer.find_needs_training(user_stats, outliers)
    
    # TODO: рекомендации
    # recommendations = analyzer.generate_recommendations(
    #     cluster_stats, best_practices, low_performers
    # )
    
    logger.info("Модуль в разработке. См. docs/future_1C_ML.md для деталей.")


if __name__ == "__main__":
    main()
