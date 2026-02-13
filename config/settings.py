"""アプリケーション設定"""
import os
from dotenv import load_dotenv

load_dotenv()


def _get_secret(key: str, default: str = "") -> str:
    """環境変数またはStreamlit Cloudのsecretsから値を取得"""
    value = os.getenv(key)
    if value:
        return value
    try:
        import streamlit as st
        return st.secrets.get(key, default)
    except Exception:
        return default

# 楽天RMS API設定（複数店舗対応）
RAKUTEN_STORES = []

# 店舗1
if _get_secret("RAKUTEN_SERVICE_SECRET_1"):
    RAKUTEN_STORES.append({
        "name": _get_secret("RAKUTEN_SHOP_NAME_1", "店舗1"),
        "service_secret": _get_secret("RAKUTEN_SERVICE_SECRET_1"),
        "license_key": _get_secret("RAKUTEN_LICENSE_KEY_1"),
    })

# 店舗2
if _get_secret("RAKUTEN_SERVICE_SECRET_2"):
    RAKUTEN_STORES.append({
        "name": _get_secret("RAKUTEN_SHOP_NAME_2", "店舗2"),
        "service_secret": _get_secret("RAKUTEN_SERVICE_SECRET_2"),
        "license_key": _get_secret("RAKUTEN_LICENSE_KEY_2"),
    })

# 店舗3（将来用）
if _get_secret("RAKUTEN_SERVICE_SECRET_3"):
    RAKUTEN_STORES.append({
        "name": _get_secret("RAKUTEN_SHOP_NAME_3", "店舗3"),
        "service_secret": _get_secret("RAKUTEN_SERVICE_SECRET_3"),
        "license_key": _get_secret("RAKUTEN_LICENSE_KEY_3"),
    })

# 後方互換性のため（単一店舗の変数も維持）
RAKUTEN_SERVICE_SECRET = _get_secret("RAKUTEN_SERVICE_SECRET_1") or _get_secret("RAKUTEN_SERVICE_SECRET")
RAKUTEN_LICENSE_KEY = _get_secret("RAKUTEN_LICENSE_KEY_1") or _get_secret("RAKUTEN_LICENSE_KEY")
RAKUTEN_SHOP_URL = _get_secret("RAKUTEN_SHOP_URL")

# 楽天RMS APIエンドポイント
RAKUTEN_API_BASE_URL = "https://api.rms.rakuten.co.jp/es/2.0"
RAKUTEN_SEARCH_ORDER_URL = f"{RAKUTEN_API_BASE_URL}/order/searchOrder/"
RAKUTEN_GET_ORDER_URL = f"{RAKUTEN_API_BASE_URL}/order/getOrder/"

# Yahoo!ショッピング API設定
YAHOO_CLIENT_ID = _get_secret("YAHOO_CLIENT_ID")
YAHOO_CLIENT_SECRET = _get_secret("YAHOO_CLIENT_SECRET")
YAHOO_SELLER_ID = _get_secret("YAHOO_SELLER_ID")

# メルカリShops API設定
MERCARI_ACCESS_TOKEN = _get_secret("MERCARI_ACCESS_TOKEN")

# Google Sheets設定
GOOGLE_CREDENTIALS_PATH = _get_secret("GOOGLE_CREDENTIALS_PATH", "config/credentials.json")
SPREADSHEET_ID = _get_secret("SPREADSHEET_ID")

# ダッシュボード認証（SHA-256ハッシュ済みパスワード）
DASHBOARD_PASSWORD = _get_secret("DASHBOARD_PASSWORD")

# データ保存設定
DATA_DIR = "data"

# API制限設定
MAX_ORDERS_PER_REQUEST = 1000
API_RETRY_COUNT = 3
API_RETRY_DELAY = 1  # 秒
