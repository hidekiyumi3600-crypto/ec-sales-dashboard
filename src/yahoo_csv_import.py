"""Yahoo!ショッピング CSVインポートモジュール"""
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Union
import io


class YahooCSVImporter:
    """Yahoo!ショッピングのCSVデータをインポート"""

    # CSVファイル保存先
    DATA_DIR = Path(__file__).parent.parent / "data" / "yahoo"

    def __init__(self):
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)

    def parse_order_csv(self, csv_content: Union[str, bytes], encoding: str = "cp932") -> pd.DataFrame:
        """
        Yahoo!ショッピングの注文CSVをパース

        Args:
            csv_content: CSVファイルの内容
            encoding: 文字エンコーディング（デフォルトはcp932/Shift-JIS）

        Returns:
            注文データのDataFrame
        """
        try:
            # バイトの場合はデコード
            if isinstance(csv_content, bytes):
                csv_content = csv_content.decode(encoding)

            df = pd.read_csv(io.StringIO(csv_content), encoding="utf-8")
        except UnicodeDecodeError:
            # エンコーディングエラーの場合、別のエンコーディングを試す
            if isinstance(csv_content, bytes):
                csv_content = csv_content.decode("utf-8", errors="ignore")
            df = pd.read_csv(io.StringIO(csv_content))

        if df.empty:
            return pd.DataFrame()

        # カラム名の正規化（Yahoo CSVの形式に対応）
        column_mapping = {
            "注文ID": "order_number",
            "オーダーID": "order_number",
            "OrderId": "order_number",
            "注文日時": "order_date",
            "注文日": "order_date",
            "OrderTime": "order_date",
            "商品名": "item_name",
            "Title": "item_name",
            "商品タイトル": "item_name",
            "数量": "quantity",
            "Quantity": "quantity",
            "個数": "quantity",
            "単価": "unit_price",
            "UnitPrice": "unit_price",
            "商品単価": "unit_price",
            "小計": "subtotal",
            "SubTotal": "subtotal",
            "商品小計": "subtotal",
            "合計金額": "total_price",
            "TotalPrice": "total_price",
            "請求金額": "total_price",
            "ポイント利用": "use_point",
            "UsePoint": "use_point",
            "クーポン割引": "coupon_discount",
            "CouponDiscount": "coupon_discount",
        }

        # 存在するカラムのみリネーム
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_dict)

        # 必須カラムの確認
        required_cols = ["order_number", "order_date"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"必須カラムがありません: {missing}")

        # 日付パース
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

        # 数値カラムの変換
        numeric_cols = ["quantity", "unit_price", "subtotal", "total_price", "use_point", "coupon_discount"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int)

        # デフォルト値設定
        if "quantity" not in df.columns:
            df["quantity"] = 1
        if "item_name" not in df.columns:
            df["item_name"] = "不明"
        if "unit_price" not in df.columns:
            df["unit_price"] = 0
        if "subtotal" not in df.columns:
            df["subtotal"] = df.get("unit_price", 0) * df.get("quantity", 1)

        # 実売上計算
        if "total_price" in df.columns:
            use_point = df.get("use_point", 0)
            coupon = df.get("coupon_discount", 0)
            df["order_net_sales"] = df["total_price"] - use_point - coupon
        else:
            df["order_net_sales"] = df["subtotal"]

        # ソース追加
        df["source"] = "Yahoo"

        return df

    def save_imported_data(self, df: pd.DataFrame, filename: str = None) -> Path:
        """インポートしたデータを保存"""
        if filename is None:
            filename = f"yahoo_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        filepath = self.DATA_DIR / filename
        df.to_csv(filepath, index=False, encoding="utf-8-sig")
        return filepath

    def load_saved_data(self, start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
        """保存済みデータを読み込み"""
        all_dfs = []

        for csv_file in self.DATA_DIR.glob("yahoo_orders_*.csv"):
            try:
                df = pd.read_csv(csv_file, encoding="utf-8-sig")
                df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
                all_dfs.append(df)
            except Exception:
                continue

        if not all_dfs:
            return pd.DataFrame()

        combined = pd.concat(all_dfs, ignore_index=True)

        # 重複削除
        if "order_number" in combined.columns:
            combined = combined.drop_duplicates(subset=["order_number", "item_name"], keep="last")

        # 日付フィルタ
        if start_date:
            combined = combined[combined["order_date"] >= start_date]
        if end_date:
            combined = combined[combined["order_date"] <= end_date]

        return combined

    def get_data_summary(self) -> dict:
        """保存済みデータのサマリーを取得"""
        df = self.load_saved_data()
        if df.empty:
            return {"count": 0, "start": None, "end": None}

        return {
            "count": len(df),
            "start": df["order_date"].min(),
            "end": df["order_date"].max(),
        }
