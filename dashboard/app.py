"""Streamlitダッシュボード"""
import hashlib
import sys
import time as time_module
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.data_processor import DataProcessor
from src.rakuten_api import RakutenAPI, RakutenAPIError, get_all_rakuten_apis, get_all_stores_sales_data
from src.yahoo_api import YahooShoppingAPI, YahooAPIError
from src.yahoo_csv_import import YahooCSVImporter
from config.settings import RAKUTEN_STORES, DASHBOARD_PASSWORD


st.set_page_config(
    page_title="売上ダッシュボード",
    page_icon="📊",
    layout="wide",
)

# カスタムCSS
st.markdown("""
<style>
    /* メトリックカードのスタイル */
    [data-testid="stMetric"] {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e9ecef;
        min-width: 150px;
    }
    [data-testid="stMetric"] label {
        color: #495057 !important;
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #212529 !important;
        font-weight: bold !important;
        font-size: 1.5rem !important;
        white-space: nowrap !important;
    }
    /* プラス（増加）の色 - 濃い緑 */
    [data-testid="stMetric"] [data-testid="stMetricDelta"] svg[data-testid="stMetricDeltaIcon-Up"] {
        fill: #0d6e0d !important;
    }
    [data-testid="stMetric"] [data-testid="stMetricDelta"]:has(svg[data-testid="stMetricDeltaIcon-Up"]) {
        color: #0d6e0d !important;
        font-weight: bold !important;
    }
    /* マイナス（減少）の色 - 濃い赤 */
    [data-testid="stMetric"] [data-testid="stMetricDelta"] svg[data-testid="stMetricDeltaIcon-Down"] {
        fill: #c41e3a !important;
    }
    [data-testid="stMetric"] [data-testid="stMetricDelta"]:has(svg[data-testid="stMetricDeltaIcon-Down"]) {
        color: #c41e3a !important;
        font-weight: bold !important;
    }
    /* デルタ全般 */
    [data-testid="stMetricDelta"] > div {
        font-weight: bold !important;
    }
    /* カラムの最小幅 */
    [data-testid="column"] {
        min-width: 120px;
    }
</style>
""", unsafe_allow_html=True)


# ===== ディスクキャッシュ（サーバー再起動でも保持） =====
CACHE_DIR = Path(__file__).parent.parent / "data" / "cache"
CACHE_TTL_SECONDS = 7200  # 2時間


def _disk_cache_path(prefix: str, start_date: datetime, end_date: datetime) -> Path:
    key = f"{prefix}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    return CACHE_DIR / f"{key}.pkl"


def _read_disk_cache(cache_path: Path):
    """ディスクキャッシュ読み込み（TTLチェック付き）"""
    if cache_path.exists():
        age = time_module.time() - cache_path.stat().st_mtime
        if age < CACHE_TTL_SECONDS:
            try:
                return pd.read_pickle(cache_path)
            except Exception:
                pass
    return None


def _write_disk_cache(cache_path: Path, df: pd.DataFrame):
    """DataFrameをディスクキャッシュに保存"""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_pickle(cache_path)
    except Exception:
        pass


def _clear_all_disk_cache():
    """ディスクキャッシュを全クリア"""
    try:
        if CACHE_DIR.exists():
            for f in CACHE_DIR.glob("*.pkl"):
                f.unlink()
    except Exception:
        pass


def _fetch_rakuten_sales(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """楽天売上データ取得（ディスクキャッシュ付き・スレッド安全）"""
    cache_path = _disk_cache_path("rakuten", start_date, end_date)
    cached = _read_disk_cache(cache_path)
    if cached is not None:
        return cached

    try:
        orders = get_all_stores_sales_data(start_date, end_date)
        if not orders:
            return pd.DataFrame()
        processor = DataProcessor()
        df = processor.parse_orders(orders)
        if not df.empty:
            _write_disk_cache(cache_path, df)
        return df
    except Exception:
        return pd.DataFrame()


def _fetch_yahoo_sales(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Yahoo売上データ取得（ディスクキャッシュ付き・スレッド安全）"""
    cache_path = _disk_cache_path("yahoo", start_date, end_date)
    cached = _read_disk_cache(cache_path)
    if cached is not None:
        return cached

    # CSVインポートデータをチェック
    try:
        importer = YahooCSVImporter()
        csv_data = importer.load_saved_data(start_date, end_date)
        if not csv_data.empty:
            return csv_data
    except Exception:
        pass

    # API取得
    try:
        api = YahooShoppingAPI()
        if not api.is_authenticated():
            return pd.DataFrame()
        orders = api.get_sales_data(start_date, end_date)
        df = parse_yahoo_orders(orders)
        if not df.empty:
            _write_disk_cache(cache_path, df)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=7200)
def load_rakuten_sales_cached(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """楽天売上データを読み込み（メモリキャッシュ + ディスクキャッシュ）"""
    return _fetch_rakuten_sales(start_date, end_date)


@st.cache_data(ttl=7200)
def load_yahoo_sales_cached(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Yahoo売上データを読み込み（メモリキャッシュ + ディスクキャッシュ）"""
    return _fetch_yahoo_sales(start_date, end_date)


def parse_yahoo_orders(orders: list) -> pd.DataFrame:
    """Yahoo注文データをDataFrameに変換

    orderInfoレスポンス構造:
      OrderInfo > OrderId, OrderTime, OrderStatus
      OrderInfo > Pay > TotalPrice, UsePoint, GiftCardDiscount, PayCharge, ShipCharge
      OrderInfo > Detail > TotalPrice (明細合計)
      OrderInfo > Item (複数) > ItemId, Title, UnitPrice, Quantity, SubTotal
    """
    if not orders:
        return pd.DataFrame()

    records = []
    for order in orders:
        try:
            order_id = order.get("OrderId", "")
            order_time = order.get("OrderTime", "")

            # 日時パース
            if order_time:
                try:
                    order_date = datetime.strptime(order_time[:14], "%Y%m%d%H%M%S")
                except ValueError:
                    order_date = datetime.now()
            else:
                order_date = datetime.now()

            # 支払い情報（Pay配下）
            pay_info = order.get("Pay", {}) or {}
            total_price = int(pay_info.get("TotalPrice", 0) or 0)
            use_point = int(pay_info.get("UsePoint", 0) or 0)
            gift_card_discount = int(pay_info.get("GiftCardDiscount", 0) or 0)

            # TotalPriceがPay配下にない場合、Detail配下を参照
            if total_price == 0:
                detail_info = order.get("Detail", {}) or {}
                total_price = int(detail_info.get("TotalPrice", 0) or 0)

            # トップレベルのTotalPriceもフォールバック
            if total_price == 0:
                total_price = int(order.get("TotalPrice", 0) or 0)

            # 実売上（ポイント・ギフトカード割引控除後）
            net_sales = total_price - use_point - gift_card_discount

            # 商品情報（Item配下 - 複数ある場合はリスト）
            items = order.get("Item", [])
            if not isinstance(items, list):
                items = [items] if items else []

            if items:
                for item in items:
                    if not item:
                        continue
                    item_name = item.get("Title", "")
                    quantity = int(item.get("Quantity", 1) or 1)
                    unit_price = int(item.get("UnitPrice", 0) or 0)
                    item_total = int(item.get("SubTotal", unit_price * quantity) or 0)

                    records.append({
                        "order_number": order_id,
                        "order_date": order_date,
                        "item_name": item_name,
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "subtotal": item_total,
                        "order_net_sales": net_sales,
                        "source": "Yahoo",
                    })
            else:
                # 商品明細がない場合でも注文レコードは作成
                records.append({
                    "order_number": order_id,
                    "order_date": order_date,
                    "item_name": "",
                    "quantity": 1,
                    "unit_price": total_price,
                    "subtotal": total_price,
                    "order_net_sales": net_sales,
                    "source": "Yahoo",
                })

        except Exception:
            continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    return df


# 旧関数名を互換性のため残す
def load_sales_data_cached(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """売上データを読み込み（楽天のみ・後方互換用）"""
    return load_rakuten_sales_cached(start_date, end_date)


def get_period_sales(df: pd.DataFrame, start_date, end_date) -> dict:
    """指定期間の売上を計算"""
    if df.empty:
        return {"sales": 0, "orders": 0, "items": 0}

    mask = (df["order_date"].dt.date >= start_date) & (df["order_date"].dt.date <= end_date)
    period_df = df[mask]

    if period_df.empty:
        return {"sales": 0, "orders": 0, "items": 0}

    # 注文単位の実売上を取得
    order_sales = period_df.drop_duplicates(subset=["order_number"])

    return {
        "sales": order_sales["order_net_sales"].sum(),
        "orders": len(order_sales),
        "items": period_df["quantity"].sum(),
    }


def format_currency(value):
    """金額をフォーマット"""
    if value >= 1000000:
        return f"¥{value/1000000:.1f}M"
    elif value >= 1000:
        return f"¥{value/1000:.0f}K"
    else:
        return f"¥{value:,.0f}"


def format_delta(current, previous):
    """差額と増減率を計算"""
    if previous == 0:
        return 0, 0
    diff = current - previous
    rate = (diff / previous) * 100
    return diff, rate


def check_license_expiry(env_path: Path) -> list:
    """ライセンスキーの期限をチェック（発行から約3ヶ月）"""
    warnings = []

    # ライセンスキー発行日を保存するファイル
    license_file = env_path.parent / "config" / "license_dates.json"

    import json
    license_dates = {}

    if license_file.exists():
        try:
            with open(license_file, "r") as f:
                license_dates = json.load(f)
        except:
            pass

    # 各店舗のライセンスキーをチェック
    for store in RAKUTEN_STORES:
        store_name = store["name"]
        license_key = store["license_key"]

        if license_key:
            # 既存の発行日を確認、なければ今日を登録
            if license_key not in license_dates:
                license_dates[license_key] = datetime.now().isoformat()
                # 保存
                try:
                    license_file.parent.mkdir(parents=True, exist_ok=True)
                    with open(license_file, "w") as f:
                        json.dump(license_dates, f, indent=2)
                except:
                    pass

            # 期限計算（発行から90日）
            try:
                issued_date = datetime.fromisoformat(license_dates[license_key])
                expiry_date = issued_date + timedelta(days=90)
                days_left = (expiry_date.date() - datetime.now().date()).days

                if days_left <= 0:
                    warnings.append({
                        "store": store_name,
                        "status": "expired",
                        "message": f"⚠️ {store_name}: ライセンスキー期限切れ",
                        "days": days_left
                    })
                elif days_left <= 14:
                    warnings.append({
                        "store": store_name,
                        "status": "warning",
                        "message": f"⚠️ {store_name}: ライセンスキー期限まで{days_left}日",
                        "days": days_left
                    })
            except:
                pass

    return warnings


def save_env_file(env_path: Path, env_vars: dict):
    """環境変数ファイルを保存"""
    lines = [
        "# 楽天RMS API 認証情報",
        f"RAKUTEN_SERVICE_SECRET={env_vars.get('RAKUTEN_SERVICE_SECRET', '')}",
        f"RAKUTEN_LICENSE_KEY={env_vars.get('RAKUTEN_LICENSE_KEY', '')}",
        f"RAKUTEN_SHOP_URL={env_vars.get('RAKUTEN_SHOP_URL', '')}",
        "",
        "# Yahoo!ショッピング API 認証情報",
        f"YAHOO_CLIENT_ID={env_vars.get('YAHOO_CLIENT_ID', '')}",
        f"YAHOO_CLIENT_SECRET={env_vars.get('YAHOO_CLIENT_SECRET', '')}",
        f"YAHOO_SELLER_ID={env_vars.get('YAHOO_SELLER_ID', '')}",
        "",
        "# Google Sheets 設定",
        f"GOOGLE_CREDENTIALS_PATH={env_vars.get('GOOGLE_CREDENTIALS_PATH', 'config/credentials.json')}",
        f"SPREADSHEET_ID={env_vars.get('SPREADSHEET_ID', '')}",
        "",
    ]
    with open(env_path, "w") as f:
        f.write("\n".join(lines))


def _get_auth_cookie() -> str:
    """認証Cookieのトークンを生成"""
    # パスワードハッシュ + 固定ソルトで認証トークンを作成
    return hashlib.sha256(f"{DASHBOARD_PASSWORD}_dashboard_auth".encode()).hexdigest()[:32]


def check_password() -> bool:
    """パスワード認証チェック。認証済みならTrueを返す。"""
    if not DASHBOARD_PASSWORD:
        return True

    # session_stateで認証済み
    if st.session_state.get("authenticated"):
        return True

    # クエリパラメータでの認証トークン確認（永続ログイン用）
    query_params = st.query_params
    auth_token = query_params.get("auth")
    if auth_token and auth_token == _get_auth_cookie():
        st.session_state["authenticated"] = True
        return True

    st.markdown("#### 🔐 ログイン")
    st.markdown("ダッシュボードを表示するにはパスワードを入力してください。")

    password = st.text_input("パスワード", type="password", key="login_password")
    remember = st.checkbox("ログイン状態を保持する", value=True, key="remember_login")

    if st.button("ログイン"):
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        if password_hash == DASHBOARD_PASSWORD:
            st.session_state["authenticated"] = True
            if remember:
                # 認証トークンをURLパラメータとして保持
                st.query_params["auth"] = _get_auth_cookie()
            st.rerun()
        else:
            st.error("パスワードが正しくありません。")

    return False


def main():
    # Yahoo OAuthコールバック処理（URLに?code=がある場合）
    query_params = st.query_params
    if "code" in query_params:
        yahoo_code = query_params.get("code")
        if yahoo_code:
            try:
                yahoo_api = YahooShoppingAPI()
                redirect_uri = "http://localhost:8503/"
                yahoo_api.get_token_from_code(yahoo_code, redirect_uri)
                st.success("✅ Yahoo認証が完了しました！")
                # URLパラメータをクリア
                st.query_params.clear()
                st.cache_data.clear()
                st.rerun()
            except YahooAPIError as e:
                st.error(f"Yahoo認証エラー: {e}")
                st.query_params.clear()

    st.markdown("#### 📊 EC売上ダッシュボード")

    # サイドバー
    st.sidebar.header("⚙️ 設定")

    # ログアウトボタン
    if DASHBOARD_PASSWORD and st.sidebar.button("🔓 ログアウト"):
        st.session_state["authenticated"] = False
        st.query_params.clear()
        st.rerun()

    # キャッシュクリアボタン
    if st.sidebar.button("🔄 データ再取得"):
        st.cache_data.clear()
        _clear_all_disk_cache()
        # session_stateのキャッシュデータをクリア
        for key in list(st.session_state.keys()):
            if key.startswith("sales_"):
                del st.session_state[key]
        st.session_state.pop("yahoo_api_failed", None)
        st.rerun()

    # ライセンスキー期限アラート
    env_path = Path(__file__).parent.parent / ".env"
    license_warnings = check_license_expiry(env_path)
    if license_warnings:
        st.sidebar.markdown("---")
        for warn in license_warnings:
            if warn["status"] == "expired":
                st.sidebar.error(warn["message"])
            else:
                st.sidebar.warning(warn["message"])

    # API認証情報の設定
    st.sidebar.markdown("---")

    # 楽天API設定
    with st.sidebar.expander("🔑 楽天API設定"):
        st.caption("ライセンスキーは約3ヶ月で更新が必要です")

        # 現在の設定を読み込み
        env_path = Path(__file__).parent.parent / ".env"
        env_vars = {}

        if env_path.exists():
            with open(env_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        key, value = line.split("=", 1)
                        env_vars[key] = value

        new_secret = st.text_input(
            "サービスシークレット",
            value=env_vars.get("RAKUTEN_SERVICE_SECRET", ""),
            type="password",
            key="rakuten_secret"
        )
        new_key = st.text_input(
            "ライセンスキー",
            value=env_vars.get("RAKUTEN_LICENSE_KEY", ""),
            type="password",
            key="rakuten_key"
        )
        new_shop = st.text_input(
            "ショップURL",
            value=env_vars.get("RAKUTEN_SHOP_URL", ""),
            key="rakuten_shop"
        )

        if st.button("💾 楽天認証情報を保存", key="save_rakuten"):
            env_vars["RAKUTEN_SERVICE_SECRET"] = new_secret
            env_vars["RAKUTEN_LICENSE_KEY"] = new_key
            env_vars["RAKUTEN_SHOP_URL"] = new_shop
            save_env_file(env_path, env_vars)
            st.cache_data.clear()
            st.success("✅ 保存しました")
            st.rerun()

    # Yahoo設定
    with st.sidebar.expander("🔶 Yahoo設定"):
        # CSVインポーターの状態確認
        yahoo_importer = YahooCSVImporter()
        csv_summary = yahoo_importer.get_data_summary()
        yahoo_api = YahooShoppingAPI()
        is_yahoo_auth = yahoo_api.is_authenticated()

        # データ取得方法の選択
        yahoo_method = st.radio(
            "データ取得方法",
            ["📁 CSVインポート（推奨）", "🔐 API連携"],
            key="yahoo_method",
            help="CSVインポートはAPI認証不要で簡単に使えます"
        )

        if yahoo_method == "📁 CSVインポート（推奨）":
            st.markdown("---")
            st.markdown("**Yahoo CSVインポート**")

            # 現在のデータ状況
            if csv_summary["count"] > 0:
                st.success(f"✅ {csv_summary['count']}件のデータあり")
                st.caption(f"期間: {csv_summary['start'].strftime('%Y/%m/%d')} 〜 {csv_summary['end'].strftime('%Y/%m/%d')}")

            st.caption("""
**CSVエクスポート手順：**
1. ストアクリエイターProにログイン
2. 注文管理 → 注文検索
3. 期間を指定して検索
4. 「CSVダウンロード」
            """)

            # ファイルアップロード
            uploaded_file = st.file_uploader(
                "Yahoo注文CSVをアップロード",
                type=["csv"],
                key="yahoo_csv_upload"
            )

            if uploaded_file:
                try:
                    content = uploaded_file.read()
                    df = yahoo_importer.parse_order_csv(content)
                    st.write(f"読み込み: {len(df)}件")

                    if not df.empty:
                        st.dataframe(df.head(), height=150)
                        if st.button("💾 データを保存", key="save_yahoo_csv"):
                            yahoo_importer.save_imported_data(df)
                            st.success("✅ 保存しました！")
                            st.cache_data.clear()
                            st.rerun()
                except Exception as e:
                    st.error(f"CSVパースエラー: {e}")

        else:  # API連携
            st.markdown("---")

            if is_yahoo_auth:
                st.success("✅ Yahoo認証済み")
                if st.button("🔓 認証解除", key="yahoo_logout"):
                    yahoo_api.clear_token()
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.info("認証コードがYahoo画面上に表示されます。それをコピーして下の欄に貼り付けてください。")

                redirect_uri = "oob"
                auth_url = yahoo_api.get_auth_url(redirect_uri)
                st.markdown(f"**[1. Yahoo認証ページを開く]({auth_url})**")

                auth_code = st.text_input(
                    "2. 表示された認証コードを貼り付け",
                    key="yahoo_auth_code",
                    placeholder="認証コードをここにペースト"
                )

                if auth_code and st.button("🔑 認証完了", key="yahoo_complete"):
                    try:
                        code = auth_code.strip()
                        if code:
                            yahoo_api.get_token_from_code(code, redirect_uri)
                            st.success("✅ 認証完了！")
                            st.cache_data.clear()
                            st.rerun()
                    except YahooAPIError as e:
                        st.error(f"エラー: {e}")

    # 現在の日時
    now = datetime.now()
    today = now.date()
    yesterday = today - timedelta(days=1)

    # 今月の期間（前日まで）
    month_start = today.replace(day=1)

    # 昨年同時期（前日までで比較）
    last_year_yesterday = yesterday.replace(year=yesterday.year - 1)
    last_year_month_start = month_start.replace(year=month_start.year - 1)

    # Yahooデータ状態を確認（API認証 または CSVデータあり）
    yahoo_api = YahooShoppingAPI()
    yahoo_importer = YahooCSVImporter()
    yahoo_csv_summary = yahoo_importer.get_data_summary()
    is_yahoo_enabled = yahoo_api.is_authenticated() or yahoo_csv_summary["count"] > 0

    # データ取得期間
    current_start = datetime.combine(month_start, datetime.min.time())
    current_end = datetime.combine(yesterday, datetime.max.time())
    ly_start = datetime.combine(last_year_month_start, datetime.min.time())
    ly_end = datetime.combine(last_year_yesterday, datetime.max.time())

    # session_stateキャッシュキー（日付が変わるとキーも変わる）
    _cache_key = f"sales_{month_start}_{yesterday}"

    if _cache_key in st.session_state:
        # キャッシュヒット → API呼び出しなしで即表示
        df_current = st.session_state[_cache_key]["current"]
        df_last_year = st.session_state[_cache_key]["last_year"]
    else:
        # 初回取得: プログレスバー付きで並列取得
        _progress = st.progress(0, text="楽天APIに接続中...")

        with ThreadPoolExecutor(max_workers=3) as executor:
            f_current = executor.submit(_fetch_rakuten_sales, current_start, current_end)
            f_last_year = executor.submit(_fetch_rakuten_sales, ly_start, ly_end)
            f_yahoo = executor.submit(_fetch_yahoo_sales, current_start, current_end) if is_yahoo_enabled else None

            _progress.progress(10, text="今月の売上データを取得中...")
            df_rakuten_current = f_current.result()
            if not df_rakuten_current.empty and "source" not in df_rakuten_current.columns:
                df_rakuten_current["source"] = "楽天"

            _progress.progress(50, text="昨年の比較データを取得中...")
            df_last_year = f_last_year.result()
            if not df_last_year.empty and "source" not in df_last_year.columns:
                df_last_year["source"] = "楽天"

            _progress.progress(80, text="Yahooデータを確認中...")
            df_yahoo_current = f_yahoo.result() if f_yahoo else pd.DataFrame()

        _progress.progress(100, text="完了!")
        _progress.empty()

        # データを統合
        dfs_current = [df for df in [df_rakuten_current, df_yahoo_current] if not df.empty]
        df_current = pd.concat(dfs_current, ignore_index=True) if dfs_current else pd.DataFrame()

        # session_stateに保存（次回リロード時は即表示）
        st.session_state[_cache_key] = {
            "current": df_current,
            "last_year": df_last_year,
        }

    processor = DataProcessor()

    # ===== メインダッシュボード =====

    # 昨日の売上
    yesterday_stats = get_period_sales(df_current, yesterday, yesterday)
    last_year_yesterday_stats = get_period_sales(df_last_year, last_year_yesterday, last_year_yesterday)

    # 今月の売上（前日まで）
    month_stats = get_period_sales(df_current, month_start, yesterday)
    last_year_month_stats = get_period_sales(df_last_year, last_year_month_start, last_year_yesterday)

    # モール別集計
    def get_source_sales(df, start_date, end_date, source):
        if df.empty or "source" not in df.columns:
            return {"sales": 0, "orders": 0}
        mask = (df["order_date"].dt.date >= start_date) & (df["order_date"].dt.date <= end_date) & (df["source"] == source)
        source_df = df[mask]
        if source_df.empty:
            return {"sales": 0, "orders": 0}
        order_sales = source_df.drop_duplicates(subset=["order_number"])
        return {"sales": order_sales["order_net_sales"].sum(), "orders": len(order_sales)}

    # 昨日のモール別
    rakuten_yesterday = get_source_sales(df_current, yesterday, yesterday, "楽天")
    yahoo_yesterday = get_source_sales(df_current, yesterday, yesterday, "Yahoo")

    # 今月のモール別
    rakuten_month = get_source_sales(df_current, month_start, yesterday, "楽天")
    yahoo_month = get_source_sales(df_current, month_start, yesterday, "Yahoo")

    # ===== ヘッダーセクション：主要KPI =====
    st.markdown("---")

    # データソース表示
    sources_active = ["🛒 楽天"]
    if is_yahoo_enabled:
        sources_active.append("🔶 Yahoo")
    st.caption(f"データソース: {' / '.join(sources_active)}")

    st.subheader("📈 売上サマリー")

    col1, col2, col3 = st.columns(3)

    # 昨日の売上
    with col1:
        st.markdown(f"### 🗓️ 昨日 ({yesterday.strftime('%m/%d')})")

        diff, rate = format_delta(yesterday_stats["sales"], last_year_yesterday_stats["sales"])

        st.metric(
            label="売上合計",
            value=f"¥{yesterday_stats['sales']:,.0f}",
            delta=f"{rate:+.1f}% (¥{diff:+,.0f})" if last_year_yesterday_stats["sales"] > 0 else None,
            delta_color="normal"
        )

        # モール内訳
        if rakuten_yesterday["sales"] > 0 or yahoo_yesterday["sales"] > 0:
            st.caption(f"🛒 楽天: ¥{rakuten_yesterday['sales']:,.0f}")
            if is_yahoo_enabled:
                st.caption(f"🔶 Yahoo: ¥{yahoo_yesterday['sales']:,.0f}")

        st.metric("注文数", f"{yesterday_stats['orders']}件")
        st.metric("商品数", f"{yesterday_stats['items']}個")

        if last_year_yesterday_stats["sales"] > 0:
            st.caption(f"昨年同日: ¥{last_year_yesterday_stats['sales']:,.0f}")

    # 今月の売上（前日まで）
    with col2:
        st.markdown(f"### 📅 今月 ({month_start.strftime('%m/01')}〜{yesterday.strftime('%m/%d')})")

        diff, rate = format_delta(month_stats["sales"], last_year_month_stats["sales"])

        st.metric(
            label="売上累計",
            value=f"¥{month_stats['sales']:,.0f}",
            delta=f"{rate:+.1f}% (¥{diff:+,.0f})" if last_year_month_stats["sales"] > 0 else None,
            delta_color="normal"
        )

        # モール内訳
        if rakuten_month["sales"] > 0 or yahoo_month["sales"] > 0:
            st.caption(f"🛒 楽天: ¥{rakuten_month['sales']:,.0f}")
            if is_yahoo_enabled:
                st.caption(f"🔶 Yahoo: ¥{yahoo_month['sales']:,.0f}")

        days_count = (yesterday - month_start).days + 1
        avg_daily = month_stats["sales"] / max(days_count, 1)

        st.metric("注文数", f"{month_stats['orders']}件")
        st.metric("日平均売上", f"¥{avg_daily:,.0f}")

        if last_year_month_stats["sales"] > 0:
            st.caption(f"昨年同時期: ¥{last_year_month_stats['sales']:,.0f}")

    # 前年比サマリー
    with col3:
        st.markdown("### 📊 前年比較")

        if last_year_month_stats["sales"] > 0:
            diff, rate = format_delta(month_stats["sales"], last_year_month_stats["sales"])

            if diff >= 0:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
                            padding: 1.5rem; border-radius: 1rem; color: white; text-align: center;">
                    <div style="font-size: 0.9rem; opacity: 0.9;">今月の前年比</div>
                    <div style="font-size: 2.5rem; font-weight: bold;">+{rate:.1f}%</div>
                    <div style="font-size: 1.1rem;">+¥{diff:,.0f}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #dc3545 0%, #fd7e14 100%);
                            padding: 1.5rem; border-radius: 1rem; color: white; text-align: center;">
                    <div style="font-size: 0.9rem; opacity: 0.9;">今月の前年比</div>
                    <div style="font-size: 2.5rem; font-weight: bold;">{rate:.1f}%</div>
                    <div style="font-size: 1.1rem;">¥{diff:,.0f}</div>
                </div>
                """, unsafe_allow_html=True)

            # 注文数の前年比
            order_diff, order_rate = format_delta(month_stats["orders"], last_year_month_stats["orders"])
            st.metric(
                "注文数 前年比",
                f"{month_stats['orders']}件",
                f"{order_rate:+.1f}% ({order_diff:+.0f}件)",
                delta_color="normal"
            )
        else:
            st.info("昨年のデータがありません")

    # ===== 店舗別売上セクション =====
    st.markdown("---")
    st.subheader("🏪 店舗別売上")

    # 店舗名リストを取得
    store_names = [store["name"] for store in RAKUTEN_STORES] if RAKUTEN_STORES else ["楽天"]

    # 店舗ごとのカラムを作成
    store_cols = st.columns(len(store_names))

    for idx, store_name in enumerate(store_names):
        with store_cols[idx]:
            st.markdown(f"### 🛒 {store_name}")

            # 今年の店舗別売上
            store_yesterday = get_source_sales(df_current, yesterday, yesterday, store_name)
            store_month = get_source_sales(df_current, month_start, yesterday, store_name)

            # 昨年の店舗別売上
            store_yesterday_ly = get_source_sales(df_last_year, last_year_yesterday, last_year_yesterday, store_name)
            store_month_ly = get_source_sales(df_last_year, last_year_month_start, last_year_yesterday, store_name)

            # 昨日
            st.markdown(f"**昨日** ({yesterday.strftime('%m/%d')})")
            diff_y, rate_y = format_delta(store_yesterday['sales'], store_yesterday_ly['sales'])
            st.metric(
                label="売上",
                value=f"¥{store_yesterday['sales']:,.0f}",
                delta=f"{rate_y:+.1f}% (¥{diff_y:+,.0f})" if store_yesterday_ly['sales'] > 0 else None,
                delta_color="normal"
            )
            st.caption(f"注文数: {store_yesterday['orders']}件")
            if store_yesterday_ly['sales'] > 0:
                st.caption(f"昨年同日: ¥{store_yesterday_ly['sales']:,.0f}")

            st.markdown("---")

            # 今月
            st.markdown(f"**今月** ({month_start.strftime('%m/01')}〜{yesterday.strftime('%m/%d')})")
            diff_m, rate_m = format_delta(store_month['sales'], store_month_ly['sales'])
            st.metric(
                label="売上累計",
                value=f"¥{store_month['sales']:,.0f}",
                delta=f"{rate_m:+.1f}% (¥{diff_m:+,.0f})" if store_month_ly['sales'] > 0 else None,
                delta_color="normal"
            )
            st.caption(f"注文数: {store_month['orders']}件")

            # 日平均
            days_count = (yesterday - month_start).days + 1
            store_avg = store_month["sales"] / max(days_count, 1)
            st.caption(f"日平均: ¥{store_avg:,.0f}")

            if store_month_ly['sales'] > 0:
                st.caption(f"昨年同時期: ¥{store_month_ly['sales']:,.0f}")

    st.markdown("---")

    # ===== 詳細分析セクション =====
    st.subheader("📉 詳細分析")

    # 日付範囲選択
    col1, col2 = st.columns([1, 3])
    with col1:
        analysis_period = st.selectbox(
            "分析期間",
            ["今月", "過去7日", "過去30日", "カスタム"]
        )

    if analysis_period == "今月":
        start_date = month_start
        end_date = today
    elif analysis_period == "過去7日":
        start_date = today - timedelta(days=7)
        end_date = today
    elif analysis_period == "過去30日":
        start_date = today - timedelta(days=30)
        end_date = today
    else:
        with col2:
            date_col1, date_col2 = st.columns(2)
            with date_col1:
                start_date = st.date_input("開始日", value=month_start)
            with date_col2:
                end_date = st.date_input("終了日", value=today)

    # 分析用データ取得（今月なら既存データを再利用）
    if analysis_period == "今月" and not df_current.empty:
        df_analysis = df_current
    else:
        with st.spinner(f"分析データを取得中... ({start_date} 〜 {end_date})"):
            try:
                df_rakuten_analysis = load_rakuten_sales_cached(
                    datetime.combine(start_date, datetime.min.time()),
                    datetime.combine(end_date, datetime.max.time())
                )
                if not df_rakuten_analysis.empty and "source" not in df_rakuten_analysis.columns:
                    df_rakuten_analysis["source"] = "楽天"
            except Exception as e:
                st.error(f"楽天データ取得エラー: {e}")
                df_rakuten_analysis = pd.DataFrame()

            df_yahoo_analysis = pd.DataFrame()
            if is_yahoo_enabled:
                try:
                    df_yahoo_analysis = load_yahoo_sales_cached(
                        datetime.combine(start_date, datetime.min.time()),
                        datetime.combine(end_date, datetime.max.time())
                    )
                except Exception as e:
                    st.warning(f"Yahooデータ取得エラー: {e}")

            dfs_analysis = [df for df in [df_rakuten_analysis, df_yahoo_analysis] if not df.empty]
            df_analysis = pd.concat(dfs_analysis, ignore_index=True) if dfs_analysis else pd.DataFrame()

    if df_analysis.empty:
        st.warning("データがありません。期間を変更してください。")
        return

    # 日付フィルタ
    df_filtered = df_analysis[
        (df_analysis["order_date"].dt.date >= start_date) &
        (df_analysis["order_date"].dt.date <= end_date)
    ]

    if df_filtered.empty:
        st.warning("選択期間にデータがありません")
        return

    # 集計
    daily_df = processor.aggregate_daily_sales(df_filtered)
    product_df = processor.aggregate_product_sales(df_filtered)
    hourly_df = processor.aggregate_hourly_sales(df_filtered)
    weekday_df = processor.aggregate_weekday_sales(df_filtered)

    # ===== グラフセクション =====
    tab1, tab2, tab3 = st.tabs(["📈 売上推移", "🏷️ 商品分析", "⏰ 時間帯分析"])

    with tab1:
        if not daily_df.empty:
            # 店舗選択
            store_names = [store["name"] for store in RAKUTEN_STORES] if RAKUTEN_STORES else []
            view_options = ["全店舗合算"] + store_names
            selected_view = st.radio("表示", view_options, horizontal=True, key="sales_view")

            if selected_view == "全店舗合算":
                # 全店舗合算グラフ
                fig = go.Figure()

                # 店舗別に色分けして積み上げ
                colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]
                for idx, store_name in enumerate(store_names):
                    store_df = df_filtered[df_filtered["source"] == store_name].copy()
                    if not store_df.empty:
                        store_daily = store_df.groupby(store_df["order_date"].dt.date).apply(
                            lambda x: x.drop_duplicates(subset=["order_number"])["order_net_sales"].sum()
                        ).reset_index()
                        store_daily.columns = ["date", "total_sales"]

                        fig.add_trace(go.Bar(
                            x=store_daily["date"],
                            y=store_daily["total_sales"],
                            name=store_name,
                            marker_color=colors[idx % len(colors)],
                            hovertemplate=f"{store_name}<br>日付: %{{x}}<br>売上: ¥%{{y:,.0f}}<extra></extra>"
                        ))

                fig.update_layout(
                    title="日別売上推移（店舗別）",
                    xaxis_title="日付",
                    yaxis_title="売上（円）",
                    yaxis_tickformat=",",
                    barmode="stack",
                    hovermode="x unified",
                    height=400,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )

                st.plotly_chart(fig, use_container_width=True)

                # 合計線グラフも表示
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(
                    x=daily_df["date"],
                    y=daily_df["total_sales"],
                    mode="lines+markers",
                    name="合計",
                    line=dict(color="#1f77b4", width=3),
                    marker=dict(size=8),
                    hovertemplate="日付: %{x}<br>合計売上: ¥%{y:,.0f}<extra></extra>"
                ))

                fig2.update_layout(
                    title="日別売上推移（合計）",
                    xaxis_title="日付",
                    yaxis_title="売上（円）",
                    yaxis_tickformat=",",
                    hovermode="x unified",
                    height=300,
                )

                st.plotly_chart(fig2, use_container_width=True)

            else:
                # 個別店舗グラフ
                store_df = df_filtered[df_filtered["source"] == selected_view].copy()
                if not store_df.empty:
                    store_daily = store_df.groupby(store_df["order_date"].dt.date).apply(
                        lambda x: x.drop_duplicates(subset=["order_number"])["order_net_sales"].sum()
                    ).reset_index()
                    store_daily.columns = ["date", "total_sales"]

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=store_daily["date"],
                        y=store_daily["total_sales"],
                        mode="lines+markers",
                        name=selected_view,
                        line=dict(color="#1f77b4", width=3),
                        marker=dict(size=8),
                        hovertemplate="日付: %{x}<br>売上: ¥%{y:,.0f}<extra></extra>"
                    ))

                    fig.update_layout(
                        title=f"日別売上推移 - {selected_view}",
                        xaxis_title="日付",
                        yaxis_title="売上（円）",
                        yaxis_tickformat=",",
                        hovermode="x unified",
                        height=400,
                    )

                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"{selected_view}のデータがありません")

            # 日別テーブル
            with st.expander("日別データを表示"):
                display_df = daily_df[["date", "order_count", "total_sales"]].copy()
                display_df.columns = ["日付", "注文数", "売上"]
                display_df["売上"] = display_df["売上"].apply(lambda x: f"¥{x:,.0f}")
                st.dataframe(display_df, use_container_width=True)

    with tab2:
        if not product_df.empty:
            # 店舗選択
            store_names_p = [store["name"] for store in RAKUTEN_STORES] if RAKUTEN_STORES else []
            view_options_p = ["全店舗合算"] + store_names_p
            selected_view_p = st.radio("表示", view_options_p, horizontal=True, key="product_view")

            if selected_view_p == "全店舗合算":
                target_df = df_filtered
                title_suffix = "（全店舗）"
            else:
                target_df = df_filtered[df_filtered["source"] == selected_view_p]
                title_suffix = f"（{selected_view_p}）"

            if not target_df.empty:
                target_product_df = processor.aggregate_product_sales(target_df)

                col1, col2 = st.columns([2, 1])

                with col1:
                    # 商品別売上TOP10
                    top_products = target_product_df.head(10).copy()
                    top_products["short_name"] = top_products["item_name"].str[:30] + "..."

                    fig = px.bar(
                        top_products,
                        x="total_sales",
                        y="short_name",
                        orientation="h",
                        color="total_sales",
                        color_continuous_scale="Blues",
                    )
                    fig.update_layout(
                        title=f"商品別売上 TOP10 {title_suffix}",
                        xaxis_title="売上（円）",
                        yaxis_title="",
                        xaxis_tickformat=",",
                        yaxis=dict(autorange="reversed"),
                        showlegend=False,
                        coloraxis_showscale=False,
                        height=400,
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    # 円グラフ
                    fig = px.pie(
                        top_products,
                        values="total_sales",
                        names="short_name",
                        hole=0.4,
                    )
                    fig.update_layout(
                        title="売上構成比",
                        showlegend=False,
                        height=400,
                    )
                    fig.update_traces(textposition='inside', textinfo='percent')
                    st.plotly_chart(fig, use_container_width=True)

                # 商品テーブル
                with st.expander("商品別データを表示"):
                    display_df = target_product_df[["item_name", "quantity", "total_sales", "order_count"]].copy()
                    display_df.columns = ["商品名", "販売数", "売上", "注文数"]
                    display_df["売上"] = display_df["売上"].apply(lambda x: f"¥{x:,.0f}")
                    st.dataframe(display_df, use_container_width=True)
            else:
                st.info(f"{selected_view_p}のデータがありません")

    with tab3:
        # 店舗選択
        store_names_t = [store["name"] for store in RAKUTEN_STORES] if RAKUTEN_STORES else []
        view_options_t = ["全店舗合算"] + store_names_t
        selected_view_t = st.radio("表示", view_options_t, horizontal=True, key="time_view")

        if selected_view_t == "全店舗合算":
            target_df_t = df_filtered
            title_suffix_t = "（全店舗）"
        else:
            target_df_t = df_filtered[df_filtered["source"] == selected_view_t]
            title_suffix_t = f"（{selected_view_t}）"

        if target_df_t.empty:
            st.info(f"{selected_view_t}のデータがありません")
        else:
            # 店舗別に集計
            target_hourly_df = processor.aggregate_hourly_sales(target_df_t)
            target_weekday_df = processor.aggregate_weekday_sales(target_df_t)

            col1, col2 = st.columns(2)

            with col1:
                if not target_hourly_df.empty:
                    fig = px.bar(
                        target_hourly_df,
                        x="hour",
                        y="total_sales",
                        color="total_sales",
                        color_continuous_scale="Blues",
                    )
                    fig.update_layout(
                        title=f"時間帯別売上 {title_suffix_t}",
                        xaxis_title="時間",
                        yaxis_title="売上（円）",
                        xaxis=dict(tickmode="linear", dtick=2),
                        yaxis_tickformat=",",
                        showlegend=False,
                        coloraxis_showscale=False,
                        height=350,
                    )
                    st.plotly_chart(fig)

            with col2:
                if not target_weekday_df.empty:
                    fig = px.bar(
                        target_weekday_df,
                        x="weekday_name",
                        y="total_sales",
                        color="total_sales",
                        color_continuous_scale="Greens",
                    )
                    fig.update_layout(
                        title=f"曜日別売上 {title_suffix_t}",
                        xaxis_title="曜日",
                        yaxis_title="売上（円）",
                        yaxis_tickformat=",",
                        showlegend=False,
                        coloraxis_showscale=False,
                        height=350,
                    )
                    st.plotly_chart(fig)

            # ヒートマップ
            heatmap_df = processor.create_hourly_weekday_heatmap(target_df_t)
            if not heatmap_df.empty:
                fig = go.Figure(data=go.Heatmap(
                    z=heatmap_df.values,
                    x=heatmap_df.columns,
                    y=heatmap_df.index,
                    colorscale="YlOrRd",
                    hovertemplate="時間: %{y}時<br>曜日: %{x}<br>売上: ¥%{z:,.0f}<extra></extra>",
                ))
                fig.update_layout(
                    title=f"時間帯×曜日 ヒートマップ {title_suffix_t}",
                    xaxis_title="曜日",
                    yaxis_title="時間",
                    yaxis=dict(tickmode="linear", dtick=2),
                    height=400,
                )
                st.plotly_chart(fig)

    # ===== データエクスポート =====
    st.markdown("---")
    st.subheader("📥 データエクスポート")

    col1, col2, col3 = st.columns(3)

    with col1:
        if not daily_df.empty:
            csv = daily_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "📊 日別売上CSV",
                csv,
                "daily_sales.csv",
                "text/csv",
            )

    with col2:
        if not product_df.empty:
            csv = product_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "🏷️ 商品別売上CSV",
                csv,
                "product_sales.csv",
                "text/csv",
            )

    with col3:
        if not df_filtered.empty:
            csv = df_filtered.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "📋 全データCSV",
                csv,
                "all_sales.csv",
                "text/csv",
            )

    # フッター
    st.markdown("---")
    sources_text = "楽天RMS API"
    if is_yahoo_enabled:
        sources_text += " / Yahoo!ショッピング API"
    st.caption(f"最終更新: {datetime.now().strftime('%Y-%m-%d %H:%M')} | データソース: {sources_text}")


if __name__ == "__main__":
    if check_password():
        main()
