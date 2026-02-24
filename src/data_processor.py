"""データ処理モジュール"""
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

import sys
sys.path.insert(0, ".")
from config.settings import DATA_DIR


class DataProcessor:
    """売上データ処理クラス"""

    def __init__(self):
        self.data_dir = Path(DATA_DIR)
        self.data_dir.mkdir(exist_ok=True)

    def parse_orders(self, orders: list) -> pd.DataFrame:
        """
        注文データをDataFrameに変換

        Args:
            orders: 楽天APIから取得した注文リスト

        Returns:
            整形されたDataFrame
        """
        if not orders:
            return pd.DataFrame()

        records = []

        for order in orders:
            # キャンセル注文（ステータス900）は除外
            order_status = order.get("orderProgress", 0)
            if order_status == 900:
                continue

            order_number = order.get("orderNumber", "")
            order_datetime_str = order.get("orderDatetime", "")
            order_datetime = self._parse_datetime(order_datetime_str)
            store_name = order.get("_store_name", "楽天")

            # 注文者情報
            orderer_model = order.get("ordererModel", {})

            # 配送先情報
            package_models = order.get("PackageModelList", [])

            for package in package_models:
                # 商品情報
                item_models = package.get("ItemModelList", [])

                # 注文レベルの金額（値引き後の実売上計算用）
                order_goods_price = order.get("goodsPrice", 0)
                order_coupon = order.get("couponAllTotalPrice", 0)
                order_net_sales = order_goods_price - order_coupon  # 実売上

                for item in item_models:
                    record = {
                        "order_number": order_number,
                        "order_datetime": order_datetime,
                        "order_date": order_datetime.date() if order_datetime else None,
                        "order_hour": order_datetime.hour if order_datetime else None,
                        "order_month": order_datetime.strftime("%Y-%m") if order_datetime else None,
                        "order_weekday": order_datetime.weekday() if order_datetime else None,
                        "item_id": item.get("itemId", ""),
                        "item_name": item.get("itemName", ""),
                        "item_number": item.get("itemNumber", ""),
                        "unit_price": item.get("price", 0),
                        "quantity": item.get("units", 1),
                        "subtotal": item.get("price", 0) * item.get("units", 1),
                        "point_used": order.get("pointAmount", 0),
                        "coupon_used": order_coupon,
                        "shipping_charge": package.get("postagePrice", 0),
                        "total_price": order_goods_price,
                        "order_net_sales": order_net_sales,  # 実売上（値引き後）
                        "payment_method": order.get("settlementMethodName", ""),
                        "status": order.get("orderProgress", 0),
                        "source": store_name,  # 店舗名
                    }
                    records.append(record)

        df = pd.DataFrame(records)

        if not df.empty:
            df["order_datetime"] = pd.to_datetime(df["order_datetime"])
            df["order_date"] = pd.to_datetime(df["order_date"])

        return df

    def _parse_datetime(self, datetime_str: str) -> Optional[datetime]:
        """日時文字列をパース"""
        if not datetime_str:
            return None

        try:
            # 楽天APIの日時フォーマット: "2024-01-15T10:30:00+0900"
            return datetime.fromisoformat(datetime_str.replace("+0900", "+09:00"))
        except ValueError:
            return None

    def aggregate_daily_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        日別売上集計

        Returns:
            日別集計DataFrame（日付、注文件数、売上金額）
        """
        if df.empty:
            return pd.DataFrame(columns=["date", "order_count", "total_sales", "gross_sales"])

        # 注文単位の実売上を取得（重複除去）
        order_sales = df.drop_duplicates(subset=["order_number"])[
            ["order_date", "order_number", "order_net_sales", "total_price", "coupon_used"]
        ]

        # 日別集計（実売上ベース）
        daily = order_sales.groupby("order_date").agg(
            order_count=("order_number", "nunique"),
            total_sales=("order_net_sales", "sum"),  # 実売上（値引き後）
            gross_sales=("total_price", "sum"),  # 総売上（値引き前）
            coupon_total=("coupon_used", "sum"),  # クーポン合計
        ).reset_index()

        # 商品数は元のdfから集計
        item_counts = df.groupby("order_date").agg(
            item_count=("quantity", "sum"),
        ).reset_index()

        daily = daily.merge(item_counts, on="order_date", how="left")
        daily.columns = ["date", "order_count", "total_sales", "gross_sales", "coupon_total", "item_count"]
        daily = daily.sort_values("date")

        return daily

    def aggregate_monthly_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        月別売上集計

        Returns:
            月別集計DataFrame（年月、注文件数、売上金額）
        """
        if df.empty:
            return pd.DataFrame(columns=["month", "order_count", "total_sales"])

        # 注文単位の実売上を取得（重複除去）
        order_sales = df.drop_duplicates(subset=["order_number"])[
            ["order_month", "order_number", "order_net_sales"]
        ]

        monthly = order_sales.groupby("order_month").agg(
            order_count=("order_number", "nunique"),
            total_sales=("order_net_sales", "sum"),  # 実売上（値引き後）
        ).reset_index()

        # 商品数は元のdfから集計
        item_counts = df.groupby("order_month").agg(
            item_count=("quantity", "sum"),
        ).reset_index()

        monthly = monthly.merge(item_counts, on="order_month", how="left")
        monthly.columns = ["month", "order_count", "total_sales", "item_count"]
        monthly = monthly.sort_values("month")

        return monthly

    def aggregate_product_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        商品別売上集計

        Returns:
            商品別集計DataFrame（商品名、販売数、売上金額）
        """
        if df.empty:
            return pd.DataFrame(columns=["item_name", "quantity", "total_sales"])

        product = df.groupby(["item_number", "item_id", "item_name"]).agg(
            quantity=("quantity", "sum"),
            total_sales=("subtotal", "sum"),
            order_count=("order_number", "nunique"),
        ).reset_index()

        product = product.sort_values("total_sales", ascending=False)

        return product

    def aggregate_hourly_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        時間帯別売上集計

        Returns:
            時間帯別集計DataFrame（時間、注文件数、売上金額）
        """
        if df.empty:
            return pd.DataFrame(columns=["hour", "order_count", "total_sales"])

        # 注文単位の実売上を取得（重複除去）
        order_sales = df.drop_duplicates(subset=["order_number"])[
            ["order_hour", "order_number", "order_net_sales"]
        ]

        hourly = order_sales.groupby("order_hour").agg(
            order_count=("order_number", "nunique"),
            total_sales=("order_net_sales", "sum"),  # 実売上（値引き後）
        ).reset_index()

        hourly.columns = ["hour", "order_count", "total_sales"]
        hourly = hourly.sort_values("hour")

        return hourly

    def aggregate_weekday_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        曜日別売上集計

        Returns:
            曜日別集計DataFrame（曜日、注文件数、売上金額）
        """
        if df.empty:
            return pd.DataFrame(columns=["weekday", "weekday_name", "order_count", "total_sales"])

        weekday_names = ["月", "火", "水", "木", "金", "土", "日"]

        # 注文単位の実売上を取得（重複除去）
        order_sales = df.drop_duplicates(subset=["order_number"])[
            ["order_weekday", "order_number", "order_net_sales"]
        ]

        weekday = order_sales.groupby("order_weekday").agg(
            order_count=("order_number", "nunique"),
            total_sales=("order_net_sales", "sum"),  # 実売上（値引き後）
        ).reset_index()

        weekday.columns = ["weekday", "order_count", "total_sales"]
        weekday["weekday_name"] = weekday["weekday"].apply(lambda x: weekday_names[x])
        weekday = weekday.sort_values("weekday")

        return weekday

    def create_hourly_weekday_heatmap(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        時間帯×曜日のヒートマップ用データ

        Returns:
            ピボットテーブル（行: 時間、列: 曜日、値: 売上）
        """
        if df.empty:
            return pd.DataFrame()

        weekday_names = ["月", "火", "水", "木", "金", "土", "日"]

        # 注文単位の実売上を取得（重複除去）
        order_sales = df.drop_duplicates(subset=["order_number"])[
            ["order_hour", "order_weekday", "order_number", "order_net_sales"]
        ]

        heatmap_data = order_sales.groupby(["order_hour", "order_weekday"]).agg(
            total_sales=("order_net_sales", "sum"),  # 実売上（値引き後）
        ).reset_index()

        heatmap_data["weekday_name"] = heatmap_data["order_weekday"].apply(
            lambda x: weekday_names[x]
        )

        pivot = heatmap_data.pivot(
            index="order_hour",
            columns="weekday_name",
            values="total_sales"
        ).fillna(0)

        # 曜日順に並べ替え
        pivot = pivot.reindex(columns=weekday_names, fill_value=0)

        return pivot

    def get_summary_stats(self, df: pd.DataFrame) -> dict:
        """
        サマリー統計を計算

        Returns:
            統計情報辞書
        """
        if df.empty:
            return {
                "total_orders": 0,
                "total_sales": 0,
                "gross_sales": 0,
                "total_items": 0,
                "avg_order_value": 0,
                "period_start": None,
                "period_end": None,
            }

        # 注文単位の実売上を取得（重複除去）
        order_sales = df.drop_duplicates(subset=["order_number"])[
            ["order_number", "order_net_sales", "total_price"]
        ]

        total_net_sales = order_sales["order_net_sales"].sum()
        total_gross_sales = order_sales["total_price"].sum()

        return {
            "total_orders": df["order_number"].nunique(),
            "total_sales": total_net_sales,  # 実売上（値引き後）
            "gross_sales": total_gross_sales,  # 総売上（値引き前）
            "total_items": df["quantity"].sum(),
            "avg_order_value": order_sales["order_net_sales"].mean(),
            "period_start": df["order_date"].min(),
            "period_end": df["order_date"].max(),
        }

    def save_to_csv(self, df: pd.DataFrame, filename: str) -> Path:
        """DataFrameをCSVファイルに保存"""
        filepath = self.data_dir / filename
        df.to_csv(filepath, index=False, encoding="utf-8-sig")
        return filepath

    def load_from_csv(self, filename: str) -> pd.DataFrame:
        """CSVファイルからDataFrameを読み込み"""
        filepath = self.data_dir / filename
        if not filepath.exists():
            return pd.DataFrame()

        return pd.read_csv(filepath, encoding="utf-8-sig")

    def save_orders_json(self, orders: list, filename: str) -> Path:
        """注文データをJSONファイルに保存"""
        filepath = self.data_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(orders, f, ensure_ascii=False, indent=2, default=str)
        return filepath

    def load_orders_json(self, filename: str) -> list:
        """JSONファイルから注文データを読み込み"""
        filepath = self.data_dir / filename
        if not filepath.exists():
            return []

        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)


def main():
    """テスト実行"""
    # サンプルデータでテスト
    processor = DataProcessor()

    # サンプル注文データ
    sample_orders = [
        {
            "orderNumber": "12345",
            "orderDatetime": "2024-01-15T10:30:00+0900",
            "goodsPrice": 5000,
            "pointAmount": 100,
            "packageModelList": [
                {
                    "postagePrice": 500,
                    "itemModelList": [
                        {
                            "itemId": "item001",
                            "itemName": "テスト商品A",
                            "itemNumber": "A001",
                            "price": 2500,
                            "units": 2,
                        }
                    ]
                }
            ]
        }
    ]

    df = processor.parse_orders(sample_orders)
    print("パース結果:")
    print(df)

    daily = processor.aggregate_daily_sales(df)
    print("\n日別売上:")
    print(daily)


if __name__ == "__main__":
    main()
