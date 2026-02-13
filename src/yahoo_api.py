"""Yahoo!ショッピング API連携モジュール"""
import base64
import hashlib
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode, parse_qs

import requests

import sys
sys.path.insert(0, ".")
from config.settings import (
    YAHOO_CLIENT_ID,
    YAHOO_CLIENT_SECRET,
    YAHOO_SELLER_ID,
)


class YahooAPIError(Exception):
    """Yahoo API例外クラス"""
    pass


class YahooShoppingAPI:
    """Yahoo!ショッピング API クライアント"""

    # APIエンドポイント
    AUTH_URL = "https://auth.login.yahoo.co.jp/yconnect/v2/authorization"
    TOKEN_URL = "https://auth.login.yahoo.co.jp/yconnect/v2/token"
    ORDER_LIST_URL = "https://circus.shopping.yahooapis.jp/ShoppingWebService/V1/orderList"
    ORDER_INFO_URL = "https://circus.shopping.yahooapis.jp/ShoppingWebService/V1/orderInfo"

    # トークン保存ファイル
    TOKEN_FILE = Path(__file__).parent.parent / "config" / "yahoo_token.json"

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        seller_id: Optional[str] = None,
    ):
        self.client_id = client_id or YAHOO_CLIENT_ID
        self.client_secret = client_secret or YAHOO_CLIENT_SECRET
        self.seller_id = seller_id or YAHOO_SELLER_ID
        self._access_token = None
        self._refresh_token = None
        self._token_expires = None

        # 保存済みトークンを読み込み
        self._load_token()

    def _load_token(self):
        """保存済みトークンを読み込み"""
        if self.TOKEN_FILE.exists():
            try:
                with open(self.TOKEN_FILE, "r") as f:
                    data = json.load(f)
                    self._access_token = data.get("access_token")
                    self._refresh_token = data.get("refresh_token")
                    expires_str = data.get("expires_at")
                    if expires_str:
                        self._token_expires = datetime.fromisoformat(expires_str)
            except Exception:
                pass

    def _save_token(self):
        """トークンを保存"""
        try:
            data = {
                "access_token": self._access_token,
                "refresh_token": self._refresh_token,
                "expires_at": self._token_expires.isoformat() if self._token_expires else None,
            }
            with open(self.TOKEN_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    def get_auth_url(self, redirect_uri: str = "oob", state: str = "state") -> str:
        """OAuth認証URLを生成

        Args:
            redirect_uri: コールバックURL。"oob"の場合は画面に認証コードを表示
            state: CSRF対策用の状態パラメータ
        """
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": redirect_uri,
            "scope": "openid profile",
            "state": state,
        }
        # oob以外の場合はbailパラメータを追加
        if redirect_uri != "oob":
            params["bail"] = "1"
        return f"{self.AUTH_URL}?{urlencode(params)}"

    def get_token_from_code(self, code: str, redirect_uri: str) -> dict:
        """認証コードからトークンを取得"""
        # Basic認証ヘッダー
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }

        try:
            response = requests.post(
                self.TOKEN_URL,
                headers=headers,
                data=data,
                timeout=30
            )

            if response.status_code != 200:
                error_data = response.json() if response.text else {}
                error_msg = error_data.get("error_description", f"HTTP {response.status_code}")
                raise YahooAPIError(f"トークン取得エラー: {error_msg}")

            token_data = response.json()
            self._access_token = token_data.get("access_token")
            self._refresh_token = token_data.get("refresh_token")
            expires_in = token_data.get("expires_in", 3600)
            self._token_expires = datetime.now() + timedelta(seconds=expires_in - 60)

            self._save_token()
            return token_data

        except requests.RequestException as e:
            raise YahooAPIError(f"通信エラー: {str(e)}")

    def refresh_access_token(self) -> dict:
        """リフレッシュトークンでアクセストークンを更新"""
        if not self._refresh_token:
            raise YahooAPIError("リフレッシュトークンがありません。再認証してください。")

        credentials = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
        }

        try:
            response = requests.post(
                self.TOKEN_URL,
                headers=headers,
                data=data,
                timeout=30
            )

            if response.status_code != 200:
                error_data = response.json() if response.text else {}
                error_msg = error_data.get("error_description", f"HTTP {response.status_code}")
                # リフレッシュトークンが無効な場合
                if "invalid_grant" in str(error_data):
                    self._access_token = None
                    self._refresh_token = None
                    self._save_token()
                    raise YahooAPIError("トークンの有効期限が切れました。再認証してください。")
                raise YahooAPIError(f"トークン更新エラー: {error_msg}")

            token_data = response.json()
            self._access_token = token_data.get("access_token")
            if token_data.get("refresh_token"):
                self._refresh_token = token_data.get("refresh_token")
            expires_in = token_data.get("expires_in", 3600)
            self._token_expires = datetime.now() + timedelta(seconds=expires_in - 60)

            self._save_token()
            return token_data

        except requests.RequestException as e:
            raise YahooAPIError(f"通信エラー: {str(e)}")

    def _get_access_token(self) -> str:
        """有効なアクセストークンを取得"""
        # トークンがない場合
        if not self._access_token:
            raise YahooAPIError("認証が必要です。Yahoo!認証を行ってください。")

        # トークンが期限切れの場合、リフレッシュを試行
        if self._token_expires and datetime.now() >= self._token_expires:
            self.refresh_access_token()

        return self._access_token

    def is_authenticated(self) -> bool:
        """認証済みかどうかを確認"""
        if not self._access_token:
            return False
        # リフレッシュトークンがあれば、期限切れでも更新可能
        if self._refresh_token:
            return True
        # アクセストークンのみの場合、期限をチェック
        if self._token_expires and datetime.now() >= self._token_expires:
            return False
        return True

    def clear_token(self):
        """トークンをクリア"""
        self._access_token = None
        self._refresh_token = None
        self._token_expires = None
        if self.TOKEN_FILE.exists():
            self.TOKEN_FILE.unlink()

    def _make_request(self, url: str, params: dict) -> dict:
        """APIリクエストを実行"""
        access_token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        # セラーIDを追加
        if self.seller_id:
            params["sellerId"] = self.seller_id

        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=60
            )

            if response.status_code != 200:
                raise YahooAPIError(f"APIエラー: HTTP {response.status_code}")

            # XMLレスポンスをパース
            return self._parse_xml_response(response.text)

        except requests.RequestException as e:
            raise YahooAPIError(f"通信エラー: {str(e)}")

    def _parse_xml_response(self, xml_text: str) -> dict:
        """XMLレスポンスを辞書に変換"""
        import xml.etree.ElementTree as ET

        try:
            root = ET.fromstring(xml_text)

            # エラーチェック
            error = root.find(".//Error")
            if error is not None:
                code = error.find("Code")
                message = error.find("Message")
                error_msg = message.text if message is not None else "Unknown error"
                raise YahooAPIError(f"APIエラー: {error_msg}")

            return self._xml_to_dict(root)

        except ET.ParseError as e:
            raise YahooAPIError(f"XMLパースエラー: {str(e)}")

    def _xml_to_dict(self, element) -> dict:
        """XML要素を辞書に変換"""
        result = {}

        for child in element:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag

            if len(child) > 0:
                value = self._xml_to_dict(child)
            else:
                value = child.text

            if tag in result:
                if not isinstance(result[tag], list):
                    result[tag] = [result[tag]]
                result[tag].append(value)
            else:
                result[tag] = value

        return result

    def search_orders(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list:
        """
        注文検索API

        Args:
            start_date: 検索開始日
            end_date: 検索終了日

        Returns:
            注文IDリスト
        """
        all_orders = []
        start = 1
        results_per_page = 100

        while True:
            params = {
                "OrderTimeFrom": start_date.strftime("%Y%m%d%H%M%S"),
                "OrderTimeTo": end_date.strftime("%Y%m%d%H%M%S"),
                "Result": results_per_page,
                "Start": start,
            }

            result = self._make_request(self.ORDER_LIST_URL, params)

            # 注文リストを取得
            search_result = result.get("Search", {})
            order_info = search_result.get("OrderInfo", [])

            if not order_info:
                break

            if not isinstance(order_info, list):
                order_info = [order_info]

            all_orders.extend(order_info)

            # ページング確認
            total_count = int(search_result.get("TotalCount", 0))
            if start + results_per_page > total_count:
                break

            start += results_per_page
            time.sleep(0.5)

        return all_orders

    def get_order_details(self, order_ids: list) -> list:
        """
        注文詳細取得API

        Args:
            order_ids: 注文IDリスト

        Returns:
            注文詳細リスト
        """
        if not order_ids:
            return []

        all_orders = []

        # 100件ずつ処理
        batch_size = 100
        for i in range(0, len(order_ids), batch_size):
            batch = order_ids[i:i + batch_size]

            params = {
                "OrderId": ",".join(batch),
            }

            result = self._make_request(self.ORDER_INFO_URL, params)

            order_info = result.get("OrderInfo", [])
            if not isinstance(order_info, list):
                order_info = [order_info]

            all_orders.extend(order_info)
            time.sleep(0.5)

        return all_orders

    def get_sales_data(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> list:
        """
        指定期間の売上データを取得

        Args:
            start_date: 開始日
            end_date: 終了日

        Returns:
            注文詳細リスト
        """
        print(f"Yahoo売上データ取得中: {start_date.date()} ~ {end_date.date()}")

        # 注文を検索
        orders = self.search_orders(start_date, end_date)
        print(f"  注文件数: {len(orders)}件")

        if not orders:
            return []

        # 注文IDを抽出
        order_ids = []
        for order in orders:
            order_id = order.get("OrderId")
            if order_id:
                order_ids.append(order_id)

        # 詳細を取得
        details = self.get_order_details(order_ids)
        print(f"  詳細取得完了: {len(details)}件")

        return details

    def test_connection(self) -> bool:
        """API接続テスト"""
        try:
            self._get_access_token()
            return True
        except YahooAPIError:
            return False


def main():
    """テスト実行"""
    import argparse

    parser = argparse.ArgumentParser(description="Yahoo!ショッピング API連携テスト")
    parser.add_argument("--test", action="store_true", help="接続テストを実行")
    args = parser.parse_args()

    if args.test:
        print("Yahoo!ショッピング API 接続テスト...")
        try:
            api = YahooShoppingAPI()
            if api.test_connection():
                print("✓ 接続成功")
            else:
                print("✗ 接続失敗")
        except YahooAPIError as e:
            print(f"✗ エラー: {e}")


if __name__ == "__main__":
    main()
