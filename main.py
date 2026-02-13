"""売り上げチェックシステム - エントリーポイント"""
import argparse
from datetime import datetime, timedelta

from src.rakuten_api import RakutenAPI, RakutenAPIError
from src.data_processor import DataProcessor
from src.google_sheet import GoogleSheetClient, GoogleSheetError


def fetch_and_aggregate(start_date: datetime, end_date: datetime, update_sheets: bool = True):
    """
    売上データを取得して集計

    Args:
        start_date: 開始日時
        end_date: 終了日時
        update_sheets: Googleスプレッドシートを更新するか
    """
    print("=" * 50)
    print("楽天市場 売上自動集計システム")
    print("=" * 50)
    print(f"期間: {start_date.date()} 〜 {end_date.date()}")
    print()

    # Step 1: 楽天APIからデータ取得
    print("[1/4] 楽天RMS APIからデータ取得中...")
    try:
        api = RakutenAPI()
        orders = api.get_sales_data(start_date, end_date)
    except RakutenAPIError as e:
        print(f"エラー: {e}")
        return

    if not orders:
        print("注文データがありません")
        return

    # Step 2: データ処理・集計
    print("\n[2/4] データ集計中...")
    processor = DataProcessor()
    df = processor.parse_orders(orders)

    daily_df = processor.aggregate_daily_sales(df)
    monthly_df = processor.aggregate_monthly_sales(df)
    product_df = processor.aggregate_product_sales(df)
    hourly_df = processor.aggregate_hourly_sales(df)
    stats = processor.get_summary_stats(df)

    # Step 3: ローカル保存
    print("\n[3/4] ローカルファイル保存中...")
    date_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

    processor.save_orders_json(orders, f"orders_{date_suffix}.json")
    processor.save_to_csv(df, f"sales_{date_suffix}.csv")
    processor.save_to_csv(daily_df, f"daily_sales_{date_suffix}.csv")
    processor.save_to_csv(product_df, f"product_sales_{date_suffix}.csv")

    print(f"  保存先: data/")

    # Step 4: Googleスプレッドシート更新
    if update_sheets:
        print("\n[4/4] Googleスプレッドシート更新中...")
        try:
            sheet_client = GoogleSheetClient()
            sheet_client.update_summary_sheet(daily_df, monthly_df, product_df, stats)
        except GoogleSheetError as e:
            print(f"警告: Googleスプレッドシート更新失敗 - {e}")
    else:
        print("\n[4/4] Googleスプレッドシート更新: スキップ")

    # 結果サマリー
    print("\n" + "=" * 50)
    print("集計結果サマリー")
    print("=" * 50)
    print(f"期間: {stats['period_start']} 〜 {stats['period_end']}")
    print(f"総注文数: {stats['total_orders']:,}件")
    print(f"総売上: ¥{stats['total_sales']:,.0f}")
    print(f"総商品数: {stats['total_items']:,}個")
    print(f"平均注文単価: ¥{stats['avg_order_value']:,.0f}")
    print()

    # 売上上位商品
    if not product_df.empty:
        print("売上上位5商品:")
        for i, row in product_df.head(5).iterrows():
            print(f"  {row['item_name']}: ¥{row['total_sales']:,.0f} ({row['quantity']}個)")

    print()
    print("処理完了")


def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(
        description="楽天市場 売上自動集計システム",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python main.py                      # 過去30日間のデータを取得
  python main.py --days 7             # 過去7日間のデータを取得
  python main.py --start 2024-01-01 --end 2024-01-31  # 期間指定
  python main.py --no-sheets          # Googleスプレッドシート更新なし
        """
    )

    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="取得する日数（デフォルト: 30日）"
    )
    parser.add_argument(
        "--start",
        type=str,
        help="開始日（YYYY-MM-DD形式）"
    )
    parser.add_argument(
        "--end",
        type=str,
        help="終了日（YYYY-MM-DD形式）"
    )
    parser.add_argument(
        "--no-sheets",
        action="store_true",
        help="Googleスプレッドシートへの更新をスキップ"
    )

    args = parser.parse_args()

    # 日付範囲の決定
    if args.start and args.end:
        try:
            start_date = datetime.strptime(args.start, "%Y-%m-%d")
            end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59
            )
        except ValueError:
            print("エラー: 日付はYYYY-MM-DD形式で指定してください")
            return
    else:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)

    # 実行
    fetch_and_aggregate(
        start_date,
        end_date,
        update_sheets=not args.no_sheets
    )


if __name__ == "__main__":
    main()
