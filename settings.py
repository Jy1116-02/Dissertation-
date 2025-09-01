# config/settings.py
import os
from datetime import datetime, timedelta

class Config:
    """项目配置类"""
    
    # 项目基本设置
    PROJECT_NAME = "Asset Pricing with Alternative Data"
    VERSION = "1.0.0"
    
    # 数据设置
    START_DATE = "2015-01-01"
    END_DATE = "2024-12-31"
    
    # 股票池设置
    STOCK_UNIVERSE = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B', 'UNH', 'JNJ',
        'JPM', 'V', 'PG', 'XOM', 'HD', 'CVX', 'MA', 'BAC', 'ABBV', 'PFE',
        'AVGO', 'KO', 'LLY', 'WMT', 'PEP', 'TMO', 'COST', 'MRK', 'DIS', 'ABT',
    ]
    
    DATA_SOURCES = {
        'market_data': 'yahoo_finance',
        'fred_data': 'fred_api',
        'news_data': 'gnews_api',
        'social_data': 'reddit_api',
        'fomc_data': 'fed_website'
    }
    
    SENTIMENT_CONFIG = {
        'models': ['vader', 'textblob', 'finbert'],
        'finbert_model': 'ProsusAI/finbert',
        'batch_size': 32,
        'max_length': 512
    }
    
    FEATURE_CONFIG = {
        'sentiment_lag': [1, 2, 3, 5, 10],
        'volatility_window': 30,
        'momentum_window': [1, 3, 6, 12],
        'standardization': 'zscore'
    }
    
    MODEL_CONFIG = {
        'train_ratio': 0.7,
        'validation_ratio': 0.15,
        'test_ratio': 0.15,
        'cross_validation_folds': 5,
        'random_state': 42
    }
    
    ML_PARAMS = {
        'xgboost': {
            'n_estimators': 1000, 'max_depth': 6, 'learning_rate': 0.1,
            'subsample': 0.8, 'colsample_bytree': 0.8, 'random_state': 42
        },
        'lightgbm': {
            'n_estimators': 1000, 'max_depth': 6, 'learning_rate': 0.1,
            'subsample': 0.8, 'colsample_bytree': 0.8, 'random_state': 42
        },
        'neural_network': {
            'hidden_layers': [128, 64, 32],
            'dropout_rate': 0.3, 'learning_rate': 0.001,
            'batch_size': 64, 'epochs': 100
        }
    }
    
    EXTREME_MARKET_CONFIG = {
        'vix_threshold': 30,
        'volatility_threshold': 2,
        'drawdown_threshold': -0.10,
        'min_duration': 5
    }
    
    BACKTEST_CONFIG = {
        'initial_capital': 1000000,
        'commission': 0.001,
        'slippage': 0.0005,
        'rebalance_frequency': 'monthly',
        'max_position': 0.05,
        'benchmark': 'SPY'
    }
    
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
    PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
    MODELS_DIR = os.path.join(BASE_DIR, 'models')
    RESULTS_DIR = os.path.join(BASE_DIR, 'results')
    
    @classmethod
    def create_directories(cls):
        for p in [cls.DATA_DIR, cls.RAW_DATA_DIR, cls.PROCESSED_DATA_DIR, cls.MODELS_DIR, cls.RESULTS_DIR]:
            os.makedirs(p, exist_ok=True)
