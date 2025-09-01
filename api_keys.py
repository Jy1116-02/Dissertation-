# config/api_keys.py - 增强版
import os
from dotenv import load_dotenv

load_dotenv()

class APIKeys:
    """API密钥配置 - 增强版"""
    
    # Alpha Vantage - 股票和宏观数据
    ALPHA_VANTAGE_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', '')
    
    # FRED - 美联储经济数据
    FRED_API_KEY = os.getenv('FRED_API_KEY', '')
    
    # 新闻API - 双重配置
    NEWS_API_KEY = os.getenv('NEWS_API_KEY', '')
    GNEWS_API_KEY = os.getenv('GNEWS_API_KEY', '')
    
    # Reddit - 社交媒体数据
    REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID', '')
    REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET', '')
    REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'AssetPricingBot/1.0')
    
    # Twitter (可选)
    TWITTER_BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN', '')
    
    # 数据库配置
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///asset_pricing.db')
    REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

    @classmethod
    def validate_keys(cls):
        """检查API密钥配置状态"""
        validation_results = {
            'alpha_vantage': bool(cls.ALPHA_VANTAGE_KEY and not cls.ALPHA_VANTAGE_KEY.startswith('your_')),
            'fred': bool(cls.FRED_API_KEY and not cls.FRED_API_KEY.startswith('your_')),
            'newsapi': bool(cls.NEWS_API_KEY and not cls.NEWS_API_KEY.startswith('your_')),
            'gnews': bool(cls.GNEWS_API_KEY and not cls.GNEWS_API_KEY.startswith('your_')),
            'reddit': bool(cls.REDDIT_CLIENT_ID and cls.REDDIT_CLIENT_SECRET and 
                          not cls.REDDIT_CLIENT_ID.startswith('your_') and 
                          not cls.REDDIT_CLIENT_SECRET.startswith('your_'))
        }
        
        configured_count = sum(validation_results.values())
        total_apis = len(validation_results)
        
        print("📊 API配置状态:")
        for api, status in validation_results.items():
            status_icon = "✅" if status else "❌"
            print(f"  {status_icon} {api.upper()}: {'已配置' if status else '未配置'}")
        
        print(f"\n📈 配置完成度: {configured_count}/{total_apis} ({configured_count/total_apis*100:.1f}%)")
        
        # 至少需要3个API配置才算通过
        if configured_count >= 3:
            print("🎉 API配置充足，可以进行完整的数据分析")
            return True
        else:
            print("⚠️ 建议配置更多API以获得更丰富的数据")
            return configured_count > 0

    @classmethod
    def get_available_apis(cls):
        """获取可用的API列表"""
        available = []
        
        if cls.ALPHA_VANTAGE_KEY and not cls.ALPHA_VANTAGE_KEY.startswith('your_'):
            available.append('alpha_vantage')
        if cls.FRED_API_KEY and not cls.FRED_API_KEY.startswith('your_'):
            available.append('fred')
        if cls.NEWS_API_KEY and not cls.NEWS_API_KEY.startswith('your_'):
            available.append('newsapi')
        if cls.GNEWS_API_KEY and not cls.GNEWS_API_KEY.startswith('your_'):
            available.append('gnews')
        if (cls.REDDIT_CLIENT_ID and cls.REDDIT_CLIENT_SECRET and 
            not cls.REDDIT_CLIENT_ID.startswith('your_') and 
            not cls.REDDIT_CLIENT_SECRET.startswith('your_')):
            available.append('reddit')
            
        return available

if __name__ == "__main__":
    ok = APIKeys.validate_keys()
    available = APIKeys.get_available_apis()
    print(f"\n🔧 可用API: {', '.join(available) if available else '无'}")
    print("配置验证：", "通过✅" if ok else "需要更多配置⚠️")