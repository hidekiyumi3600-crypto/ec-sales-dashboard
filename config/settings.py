"""アプリケーション設定"""
import os
from dotenv import load_dotenv

load_dotenv()

# 楽天RMS API設定（複数店舗対応）
RAKUTEN_STORES = []

# 店舗1
if os.getenv("RAKUTEN_SERVICE_SECRET_1"):
    RAKUTEN_STORES.append({
        "name": os.getenv("RAKUTEN_SHOP_NAME_1", "店舗1"),
        "service_secret": os.getenv("RAKUTEN_SERVICE_SECRET_1", ""),
        "license_key": os.getenv("RAKUTEN_LICENSE_KEY_1", ""),
    })

# 店舗2
if os.getenv("RAKUTEN_SERVICE_SECRET_2"):
    RAKUTEN_STORES.append({
        "name": os.getenv("RAKUTEN_SHOP_NAME_2", "店舗2"),
        "service_secret": os.getenv("RAKUTEN_SERVICE_SECRET_2", ""),
        "license_key": os.getenv("RAKUTEN_LICENSE_KEY_2", ""),
    })

# 店舗3（将来用）
if os.getenv("RAKUTEN_SERVICE_SECRET_3"):
    RAKUTEN_STORES.append({
        "name": os.getenv("RAKUTEN_SHOP_NAME_3", "店舗3"),
        "service_secret": os.getenv("RAKUTEN_SERVICE_SECRET_3", ""),
        "license_key": os.getenv("RAKUTEN_LICENSE_KEY_3", ""),
    })

# 後方互換性のため（単一店舗の変数も維持）
RAKUTEN_SERVICE_SECRET = os.getenv("RAKUTEN_SERVICE_SECRET_1", os.getenv("RAKUTEN_SERVICE_SECRET", ""))
RAKUTEN_LICENSE_KEY = os.getenv("RAKUTEN_LICENSE_KEY_1", os.getenv("RAKUTEN_LICENSE_KEY", ""))
RAKUTEN_SHOP_URL = os.getenv("RAKUTEN_SHOP_URL", "")

# 楽天RMS APIエンドポイント
RAKUTEN_API_BASE_URL = "https://api.rms.rakuten.co.jp/es/2.0"
RAKUTEN_SEARCH_ORDER_URL = f"{RAKUTEN_API_BASE_URL}/order/searchOrder/"
RAKUTEN_GET_ORDER_URL = f"{RAKUTEN_API_BASE_URL}/order/getOrder/"

# Yahoo!ショッピング API設定
YAHOO_CLIENT_ID = os.getenv("YAHOO_CLIENT_ID", "")
YAHOO_CLIENT_SECRET = os.getenv("YAHOO_CLIENT_SECRET", "")
YAHOO_SELLER_ID = os.getenv("YAHOO_SELLER_ID", "")

# メルカリShops API設定
MERCARI_ACCESS_TOKEN = os.getenv("MERCARI_ACCESS_TOKEN", "")

# Google Sheets設定
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "config/credentials.json")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")

# ダッシュボード認証（SHA-256ハッシュ済みパスワード）
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")

# データ保存設定
DATA_DIR = "data"

# API制限設定
MAX_ORDERS_PER_REQUEST = 1000
API_RETRY_COUNT = 3
API_RETRY_DELAY = 1  # 秒
