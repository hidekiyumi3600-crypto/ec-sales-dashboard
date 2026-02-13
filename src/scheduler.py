"""スケジューラーモジュール"""
import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

import sys
sys.path.insert(0, ".")
from src.rakuten_api import RakutenAPI, RakutenAPIError
from src.data_processor import DataProcessor
from src.google_sheet import GoogleSheetClient, GoogleSheetError

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_daily_aggregation():
    """
    日次集計ジョブ
    前日の売上データを取得して集計・保存
    """
    logger.info("日次集計ジョブ開始")

    try:
        # 前日のデータを取得
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=1)

        # 楽天APIからデータ取得
        api = RakutenAPI()
        orders = api.get_sales_data(start_date, end_date)

        if not orders:
            logger.info("注文データなし")
            return

        # データ処理
        processor = DataProcessor()
        df = processor.parse_orders(orders)

        # ローカルに保存
        filename = f"orders_{start_date.strftime('%Y%m%d')}.json"
        processor.save_orders_json(orders, filename)

        # 集計
        daily_df = processor.aggregate_daily_sales(df)
        product_df = processor.aggregate_product_sales(df)
        stats = processor.get_summary_stats(df)

        logger.info(f"日次集計完了: {stats['total_orders']}件, ¥{stats['total_sales']:,.0f}")

        # Googleスプレッドシートに更新
        try:
            sheet_client = GoogleSheetClient()

            # 日別売上に追記
            sheet_client.append_dataframe(daily_df, "日別売上")

            logger.info("Googleスプレッドシート更新完了")

        except GoogleSheetError as e:
            logger.warning(f"Googleスプレッドシート更新失敗: {e}")

    except RakutenAPIError as e:
        logger.error(f"楽天API エラー: {e}")
    except Exception as e:
        logger.error(f"予期しないエラー: {e}")


def run_weekly_aggregation():
    """
    週次集計ジョブ
    過去7日間の売上データを集計してレポート作成
    """
    logger.info("週次集計ジョブ開始")

    try:
        # 過去7日間のデータを取得
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

        # 楽天APIからデータ取得
        api = RakutenAPI()
        orders = api.get_sales_data(start_date, end_date)

        if not orders:
            logger.info("注文データなし")
            return

        # データ処理
        processor = DataProcessor()
        df = processor.parse_orders(orders)

        # 各種集計
        daily_df = processor.aggregate_daily_sales(df)
        monthly_df = processor.aggregate_monthly_sales(df)
        product_df = processor.aggregate_product_sales(df)
        stats = processor.get_summary_stats(df)

        logger.info(f"週次集計完了: {stats['total_orders']}件, ¥{stats['total_sales']:,.0f}")

        # Googleスプレッドシートに全シート更新
        try:
            sheet_client = GoogleSheetClient()
            sheet_client.update_summary_sheet(daily_df, monthly_df, product_df, stats)

            logger.info("Googleスプレッドシート更新完了")

        except GoogleSheetError as e:
            logger.warning(f"Googleスプレッドシート更新失敗: {e}")

    except RakutenAPIError as e:
        logger.error(f"楽天API エラー: {e}")
    except Exception as e:
        logger.error(f"予期しないエラー: {e}")


def run_monthly_aggregation():
    """
    月次集計ジョブ
    前月の売上データを集計して保存
    """
    logger.info("月次集計ジョブ開始")

    try:
        # 前月のデータを取得
        today = datetime.now()
        first_day_this_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day_last_month = first_day_this_month - timedelta(days=1)
        first_day_last_month = last_day_last_month.replace(day=1)

        # 楽天APIからデータ取得
        api = RakutenAPI()
        orders = api.get_sales_data(first_day_last_month, last_day_last_month)

        if not orders:
            logger.info("注文データなし")
            return

        # データ処理
        processor = DataProcessor()
        df = processor.parse_orders(orders)

        # ローカルに月次データを保存
        filename = f"orders_{first_day_last_month.strftime('%Y%m')}.json"
        processor.save_orders_json(orders, filename)

        # 集計
        monthly_df = processor.aggregate_monthly_sales(df)
        product_df = processor.aggregate_product_sales(df)
        stats = processor.get_summary_stats(df)

        logger.info(f"月次集計完了: {stats['total_orders']}件, ¥{stats['total_sales']:,.0f}")

        # CSV保存
        processor.save_to_csv(
            df,
            f"sales_{first_day_last_month.strftime('%Y%m')}.csv"
        )

    except RakutenAPIError as e:
        logger.error(f"楽天API エラー: {e}")
    except Exception as e:
        logger.error(f"予期しないエラー: {e}")


def start_scheduler():
    """スケジューラーを開始"""
    scheduler = BlockingScheduler()

    # 日次集計: 毎日午前6時に実行
    scheduler.add_job(
        run_daily_aggregation,
        trigger=CronTrigger(hour=6, minute=0),
        id="daily_aggregation",
        name="日次売上集計",
    )

    # 週次集計: 毎週月曜日午前7時に実行
    scheduler.add_job(
        run_weekly_aggregation,
        trigger=CronTrigger(day_of_week="mon", hour=7, minute=0),
        id="weekly_aggregation",
        name="週次売上集計",
    )

    # 月次集計: 毎月1日午前8時に実行
    scheduler.add_job(
        run_monthly_aggregation,
        trigger=CronTrigger(day=1, hour=8, minute=0),
        id="monthly_aggregation",
        name="月次売上集計",
    )

    logger.info("スケジューラー開始")
    logger.info("登録済みジョブ:")
    for job in scheduler.get_jobs():
        logger.info(f"  - {job.name}: {job.trigger}")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("スケジューラー停止")
        scheduler.shutdown()


def main():
    """メイン関数"""
    import argparse

    parser = argparse.ArgumentParser(description="売上集計スケジューラー")
    parser.add_argument("--run-daily", action="store_true", help="日次集計を即時実行")
    parser.add_argument("--run-weekly", action="store_true", help="週次集計を即時実行")
    parser.add_argument("--run-monthly", action="store_true", help="月次集計を即時実行")
    parser.add_argument("--start", action="store_true", help="スケジューラーを開始")
    args = parser.parse_args()

    if args.run_daily:
        run_daily_aggregation()
    elif args.run_weekly:
        run_weekly_aggregation()
    elif args.run_monthly:
        run_monthly_aggregation()
    elif args.start:
        start_scheduler()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
