"""Chatwork日次売上通知モジュール"""
import logging
from datetime import datetime, timedelta, timezone

import requests

import sys
sys.path.insert(0, ".")
from config.settings import CHATWORK_API_TOKEN, CHATWORK_ROOM_ID

# ロギング設定
logger = logging.getLogger(__name__)

CHATWORK_API_BASE = "https://api.chatwork.com/v2"


class ChatworkError(Exception):
    """Chatwork API例外クラス"""
    pass


class ChatworkClient:
    """Chatwork APIクライアント"""

    def __init__(self, api_token=None, room_id=None):
        self.api_token = api_token or CHATWORK_API_TOKEN
        self.room_id = room_id or CHATWORK_ROOM_ID

        if not self.api_token:
            raise ChatworkError(
                "CHATWORK_API_TOKENが設定されていません。"
                "環境変数CHATWORK_API_TOKENを設定してください。"
            )
        if not self.room_id:
            raise ChatworkError(
                "CHATWORK_ROOM_IDが設定されていません。"
                "環境変数CHATWORK_ROOM_IDを設定してください。"
            )

    def _get_headers(self):
        return {"X-ChatWorkToken": self.api_token}

    def send_message(self, message):
        """メッセージを送信"""
        url = f"{CHATWORK_API_BASE}/rooms/{self.room_id}/messages"
        try:
            response = requests.post(
                url,
                headers=self._get_headers(),
                data={"body": message},
                timeout=15,
            )
            if response.status_code != 200:
                raise ChatworkError(f"送信失敗 HTTP {response.status_code}: {response.text}")
            return response.json()
        except requests.RequestException as e:
            raise ChatworkError(f"通信エラー: {e}")

    def test_connection(self):
        """接続テスト（GET /me）"""
        url = f"{CHATWORK_API_BASE}/me"
        try:
            response = requests.get(
                url,
                headers=self._get_headers(),
                timeout=15,
            )
            if response.status_code == 200:
                data = response.json()
                return data.get("name", "OK")
            raise ChatworkError(f"接続失敗 HTTP {response.status_code}: {response.text}")
        except requests.RequestException as e:
            raise ChatworkError(f"通信エラー: {e}")


def _format_yoy(current, last_year):
    """前年比の文字列を返す"""
    if last_year is None or last_year == 0:
        return "前年データなし"
    diff = current - last_year
    pct = (diff / last_year) * 100
    sign = "+" if diff >= 0 else ""
    return f"¥{last_year:,.0f} → ¥{current:,.0f}（{sign}{pct:.1f}%）"


def format_daily_report(
    daily_stats, daily_store_stats,
    monthly_stats, monthly_store_stats,
    ly_daily_stats, ly_monthly_stats,
    target_date,
):
    """日次レポートメッセージを整形

    Args:
        daily_stats: 当日全体集計
        daily_store_stats: 当日店舗別 [{"name": str, "stats": dict}, ...]
        monthly_stats: 当月累計全体集計
        monthly_store_stats: 当月累計店舗別
        ly_daily_stats: 前年同日全体集計（Noneなら非表示）
        ly_monthly_stats: 前年同期累計全体集計（Noneなら非表示）
        target_date: 対象日（datetime.date）
    """
    weekday_names = ["月", "火", "水", "木", "金", "土", "日"]
    weekday = weekday_names[target_date.weekday()]
    date_str = target_date.strftime(f"%Y/%m/%d({weekday})")
    month = target_date.month
    day = target_date.day

    lines = [f"[info][title]売上レポート {date_str}[/title]"]

    # ── 前日売上セクション ──
    lines.append(f"■ {month}/{day}({weekday}) の売上")
    lines.append("━━━━━━━━━━━━━━━")
    lines.append("【全店舗合計】")
    lines.append(f"  注文件数: {daily_stats['total_orders']}件")
    lines.append(f"  売上合計: ¥{daily_stats['total_sales']:,.0f}")
    if daily_stats["total_orders"] > 0:
        lines.append(f"  客単価:   ¥{daily_stats['avg_order_value']:,.0f}")
    lines.append(f"  商品数:   {daily_stats['total_items']}個")

    # 前年同日比
    if ly_daily_stats:
        lines.append(f"  前年同日比: {_format_yoy(daily_stats['total_sales'], ly_daily_stats['total_sales'])}")

    # 店舗別（日次）
    for store in daily_store_stats:
        s = store["stats"]
        avg_str = f" / 客単価 ¥{s['avg_order_value']:,.0f}" if s["total_orders"] > 0 else ""
        lines.append(f"  - {store['name']}: {s['total_orders']}件 / ¥{s['total_sales']:,.0f}{avg_str}")

    # ── 当月累計セクション ──
    lines.append("")
    lines.append(f"■ {month}月累計（{month}/1〜{month}/{day}）")
    lines.append("━━━━━━━━━━━━━━━")
    lines.append("【全店舗合計】")
    lines.append(f"  注文件数: {monthly_stats['total_orders']}件")
    lines.append(f"  売上合計: ¥{monthly_stats['total_sales']:,.0f}")

    # 前年同期比
    if ly_monthly_stats:
        lines.append(f"  前年同期比: {_format_yoy(monthly_stats['total_sales'], ly_monthly_stats['total_sales'])}")

    # 店舗別（月累計）
    for store in monthly_store_stats:
        s = store["stats"]
        lines.append(f"  - {store['name']}: {s['total_orders']}件 / ¥{s['total_sales']:,.0f}")

    lines.append("[/info]")
    return "\n".join(lines)


def _collect_stats(processor, orders):
    """注文リストから全体・店舗別の集計を返す"""
    df = processor.parse_orders(orders)
    stats = processor.get_summary_stats(df)
    store_stats = []
    if not df.empty:
        for name in df["source"].unique():
            s = processor.get_summary_stats(df[df["source"] == name])
            store_stats.append({"name": name, "stats": s})
    return stats, store_stats


def send_daily_report(target_date=None):
    """前日の売上レポートをChatworkに送信

    Args:
        target_date: 対象日（datetime.date）。Noneの場合は前日。
    """
    from src.rakuten_api import get_all_stores_sales_data
    from src.data_processor import DataProcessor

    if target_date is None:
        jst = timezone(timedelta(hours=9))
        target_date = (datetime.now(jst) - timedelta(days=1)).date()

    processor = DataProcessor()

    # 期間を計算
    day_start = datetime.combine(target_date, datetime.min.time())
    day_end = day_start + timedelta(days=1)
    month_start = datetime.combine(target_date.replace(day=1), datetime.min.time())

    # 前年同日・同期
    try:
        ly_date = target_date.replace(year=target_date.year - 1)
        ly_day_start = datetime.combine(ly_date, datetime.min.time())
        ly_day_end = ly_day_start + timedelta(days=1)
        ly_month_start = datetime.combine(ly_date.replace(day=1), datetime.min.time())
    except ValueError:
        # 2/29 → 前年に2/29がない場合
        ly_date = None

    logger.info(f"Chatwork日次通知: {target_date} のデータ取得中")

    # 4期間を順次取得（並列だと楽天APIのレート制限に引っかかるため）
    fetch_tasks = [
        ("daily", day_start, day_end),
        ("monthly", month_start, day_end),
    ]
    if ly_date:
        fetch_tasks.append(("ly_daily", ly_day_start, ly_day_end))
        fetch_tasks.append(("ly_monthly", ly_month_start, ly_day_end.replace(year=ly_date.year)))

    results = {}
    for key, s, e in fetch_tasks:
        try:
            results[key] = get_all_stores_sales_data(s, e)
        except Exception as ex:
            logger.warning(f"{key}データ取得失敗: {ex}")
            results[key] = []

    # 集計
    daily_stats, daily_store_stats = _collect_stats(processor, results["daily"])
    monthly_stats, monthly_store_stats = _collect_stats(processor, results["monthly"])

    ly_daily_stats = None
    ly_monthly_stats = None
    if ly_date:
        if results.get("ly_daily"):
            ly_daily_stats, _ = _collect_stats(processor, results["ly_daily"])
        else:
            ly_daily_stats = {"total_orders": 0, "total_sales": 0}
        if results.get("ly_monthly"):
            ly_monthly_stats, _ = _collect_stats(processor, results["ly_monthly"])
        else:
            ly_monthly_stats = {"total_orders": 0, "total_sales": 0}

    # メッセージ送信
    message = format_daily_report(
        daily_stats, daily_store_stats,
        monthly_stats, monthly_store_stats,
        ly_daily_stats, ly_monthly_stats,
        target_date,
    )
    client = ChatworkClient()
    client.send_message(message)

    logger.info(f"Chatwork送信完了: {daily_stats['total_orders']}件, ¥{daily_stats['total_sales']:,.0f}")


def main():
    """CLI"""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="Chatwork日次売上通知")
    parser.add_argument("--test", action="store_true", help="接続テスト")
    parser.add_argument("--send-now", action="store_true", help="即時送信（前日分）")
    parser.add_argument("--date", type=str, help="対象日（YYYY-MM-DD）")
    args = parser.parse_args()

    if args.test:
        print("Chatwork 接続テスト...")
        try:
            client = ChatworkClient()
            name = client.test_connection()
            print(f"接続成功: {name}")
        except ChatworkError as e:
            print(f"接続失敗: {e}")

    elif args.send_now:
        target = None
        if args.date:
            target = datetime.strptime(args.date, "%Y-%m-%d").date()
        try:
            send_daily_report(target)
            print("送信完了")
        except ChatworkError as e:
            print(f"送信失敗: {e}")
        except Exception as e:
            print(f"エラー: {e}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
