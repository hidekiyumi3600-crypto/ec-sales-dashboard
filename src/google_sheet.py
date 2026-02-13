"""Googleスプレッドシート連携モジュール"""
from datetime import datetime
from pathlib import Path
from typing import Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

import sys
sys.path.insert(0, ".")
from config.settings import GOOGLE_CREDENTIALS_PATH, SPREADSHEET_ID


class GoogleSheetError(Exception):
    """Google Sheets例外クラス"""
    pass


class GoogleSheetClient:
    """Googleスプレッドシート クライアント"""

    SCOPES = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    def __init__(
        self,
        credentials_path: Optional[str] = None,
        spreadsheet_id: Optional[str] = None
    ):
        self.credentials_path = credentials_path or GOOGLE_CREDENTIALS_PATH
        self.spreadsheet_id = spreadsheet_id or SPREADSHEET_ID
        self._client = None
        self._spreadsheet = None

    def _get_client(self) -> gspread.Client:
        """認証済みクライアントを取得"""
        if self._client is None:
            creds_path = Path(self.credentials_path)

            if not creds_path.exists():
                raise GoogleSheetError(
                    f"認証ファイルが見つかりません: {self.credentials_path}\n"
                    "Google Cloud Platformでサービスアカウントを作成し、"
                    "credentials.jsonを配置してください。"
                )

            try:
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    str(creds_path),
                    self.SCOPES
                )
                self._client = gspread.authorize(credentials)
            except Exception as e:
                raise GoogleSheetError(f"認証エラー: {str(e)}")

        return self._client

    def _get_spreadsheet(self) -> gspread.Spreadsheet:
        """スプレッドシートを取得"""
        if self._spreadsheet is None:
            if not self.spreadsheet_id:
                raise GoogleSheetError(
                    "スプレッドシートIDが設定されていません。"
                    "環境変数SPREADSHEET_IDを設定してください。"
                )

            try:
                client = self._get_client()
                self._spreadsheet = client.open_by_key(self.spreadsheet_id)
            except gspread.SpreadsheetNotFound:
                raise GoogleSheetError(
                    f"スプレッドシートが見つかりません: {self.spreadsheet_id}\n"
                    "スプレッドシートIDが正しいか、サービスアカウントに"
                    "共有設定がされているか確認してください。"
                )
            except Exception as e:
                raise GoogleSheetError(f"スプレッドシート取得エラー: {str(e)}")

        return self._spreadsheet

    def get_or_create_worksheet(
        self,
        sheet_name: str,
        rows: int = 1000,
        cols: int = 20
    ) -> gspread.Worksheet:
        """ワークシートを取得（存在しない場合は作成）"""
        spreadsheet = self._get_spreadsheet()

        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows=rows,
                cols=cols
            )

        return worksheet

    def write_dataframe(
        self,
        df: pd.DataFrame,
        sheet_name: str,
        include_header: bool = True,
        clear_first: bool = True
    ) -> None:
        """
        DataFrameをワークシートに書き込み

        Args:
            df: 書き込むDataFrame
            sheet_name: シート名
            include_header: ヘッダーを含めるか
            clear_first: 書き込み前にクリアするか
        """
        if df.empty:
            print(f"警告: 空のDataFrameです。シート '{sheet_name}' への書き込みをスキップ")
            return

        worksheet = self.get_or_create_worksheet(sheet_name)

        if clear_first:
            worksheet.clear()

        # DataFrameを書き込み用リストに変換
        data = df.copy()

        # 日付型を文字列に変換
        for col in data.columns:
            if pd.api.types.is_datetime64_any_dtype(data[col]):
                data[col] = data[col].dt.strftime("%Y-%m-%d")

        values = data.values.tolist()

        if include_header:
            values.insert(0, data.columns.tolist())

        # 一括書き込み
        worksheet.update(
            range_name="A1",
            values=values,
            value_input_option="USER_ENTERED"
        )

        print(f"シート '{sheet_name}' に {len(values)} 行を書き込みました")

    def append_dataframe(
        self,
        df: pd.DataFrame,
        sheet_name: str
    ) -> None:
        """
        DataFrameをワークシートに追記

        Args:
            df: 追記するDataFrame
            sheet_name: シート名
        """
        if df.empty:
            return

        worksheet = self.get_or_create_worksheet(sheet_name)

        # DataFrameを書き込み用リストに変換
        data = df.copy()

        for col in data.columns:
            if pd.api.types.is_datetime64_any_dtype(data[col]):
                data[col] = data[col].dt.strftime("%Y-%m-%d")

        values = data.values.tolist()

        worksheet.append_rows(values, value_input_option="USER_ENTERED")

        print(f"シート '{sheet_name}' に {len(values)} 行を追記しました")

    def read_worksheet(self, sheet_name: str) -> pd.DataFrame:
        """
        ワークシートをDataFrameとして読み込み

        Args:
            sheet_name: シート名

        Returns:
            DataFrame
        """
        try:
            worksheet = self._get_spreadsheet().worksheet(sheet_name)
            data = worksheet.get_all_records()
            return pd.DataFrame(data)
        except gspread.WorksheetNotFound:
            return pd.DataFrame()

    def update_summary_sheet(
        self,
        daily_df: pd.DataFrame,
        monthly_df: pd.DataFrame,
        product_df: pd.DataFrame,
        stats: dict
    ) -> None:
        """
        集計結果をまとめて書き込み

        Args:
            daily_df: 日別売上DataFrame
            monthly_df: 月別売上DataFrame
            product_df: 商品別売上DataFrame
            stats: サマリー統計
        """
        # 日別売上
        self.write_dataframe(daily_df, "日別売上")

        # 月別売上
        self.write_dataframe(monthly_df, "月別売上")

        # 商品別売上（上位50件）
        if not product_df.empty:
            self.write_dataframe(product_df.head(50), "商品別売上")

        # サマリー
        summary_df = pd.DataFrame([
            ["集計期間", f"{stats.get('period_start', '-')} 〜 {stats.get('period_end', '-')}"],
            ["総注文数", stats.get("total_orders", 0)],
            ["総売上", f"¥{stats.get('total_sales', 0):,.0f}"],
            ["総商品数", stats.get("total_items", 0)],
            ["平均注文単価", f"¥{stats.get('avg_order_value', 0):,.0f}"],
            ["更新日時", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        ], columns=["項目", "値"])

        self.write_dataframe(summary_df, "サマリー", include_header=True)

        print("全シートの更新が完了しました")

    def test_connection(self) -> bool:
        """接続テスト"""
        try:
            spreadsheet = self._get_spreadsheet()
            print(f"スプレッドシート名: {spreadsheet.title}")
            print(f"シート一覧: {[ws.title for ws in spreadsheet.worksheets()]}")
            return True
        except GoogleSheetError as e:
            print(f"接続エラー: {e}")
            return False


def main():
    """テスト実行"""
    import argparse

    parser = argparse.ArgumentParser(description="Googleスプレッドシート連携テスト")
    parser.add_argument("--test", action="store_true", help="接続テストを実行")
    args = parser.parse_args()

    if args.test:
        print("Googleスプレッドシート 接続テスト...")
        try:
            client = GoogleSheetClient()
            if client.test_connection():
                print("✓ 接続成功")
            else:
                print("✗ 接続失敗")
        except GoogleSheetError as e:
            print(f"✗ エラー: {e}")


if __name__ == "__main__":
    main()
