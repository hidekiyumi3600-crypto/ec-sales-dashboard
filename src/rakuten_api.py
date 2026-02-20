"""楽天RMS API連携モジュール"""
import base64
import json
import time
from datetime import datetime, timedelta
from typing import Optional

import requests

import sys
sys.path.insert(0, ".")
from config.settings import (
    RAKUTEN_SERVICE_SECRET,
    RAKUTEN_LICENSE_KEY,
    RAKUTEN_STORES,
    RAKUTEN_SEARCH_ORDER_URL,
    RAKUTEN_GET_ORDER_URL,
    MAX_ORDERS_PER_REQUEST,
    API_RETRY_COUNT,
    API_RETRY_DELAY,
)


class RakutenAPIError(Exception):
    """楽天API例外クラス"""
    pass


class RakutenAPI:
    """楽天RMS API クライアント"""

    def __init__(
        self,
        service_secret: Optional[str] = None,
        license_key: Optional[str] = None,
        store_name: Optional[str] = None
    ):
        self.service_secret = service_secret or RAKUTEN_SERVICE_SECRET
        self.license_key = license_key or RAKUTEN_LICENSE_KEY
        self.store_name = store_name or "楽天"

        if not self.service_secret or not self.license_key:
            raise RakutenAPIError(
                "認証情報が設定されていません。"
                "環境変数RAKUTEN_SERVICE_SECRETとRAKUTEN_LICENSE_KEYを設定してください。"
            )

        self._auth_header = self._create_auth_header()

    def _create_auth_header(self) -> str:
        """ESA認証ヘッダーを生成"""
        credentials = f"{self.service_secret}:{self.license_key}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"ESA {encoded}"

    def _get_headers(self) -> dict:
        """リクエストヘッダーを取得"""
        return {
            "Authorization": self._auth_header,
            "Content-Type": "application/json; charset=utf-8",
        }

    def _make_request(
        self,
        url: str,
        payload: dict,
        retry_count: int = API_RETRY_COUNT
    ) -> dict:
        """APIリクエストを実行（リトライ処理付き）"""
        last_error = None

        for attempt in range(retry_count):
            try:
                response = requests.post(
                    url,
                    headers=self._get_headers(),
                    json=payload,
                    timeout=15
                )

                if response.status_code == 200:
                    return response.json()

                # エラーレスポンスの処理
                try:
                    error_data = response.json() if response.text else {}
                    # MessageModelListからエラーメッセージを取得
                    messages = error_data.get("MessageModelList", [])
                    if messages:
                        error_msg = "; ".join([m.get("message", "") for m in messages])
                    else:
                        error_msg = error_data.get("message", f"HTTP {response.status_code}")
                except:
                    error_msg = f"HTTP {response.status_code}"

                # リトライ不可能なエラー
                if response.status_code in [400, 401, 403]:
                    raise RakutenAPIError(f"HTTP {response.status_code}: {error_msg}")

                last_error = RakutenAPIError(f"APIエラー: {error_msg}")

            except requests.RequestException as e:
                last_error = RakutenAPIError(f"通信エラー: {str(e)}")

            if attempt < retry_count - 1:
                time.sleep(API_RETRY_DELAY * (attempt + 1))

        raise last_error

    def search_orders(
        self,
        start_date: datetime,
        end_date: datetime,
        order_progress: Optional[list] = None
    ) -> list:
        """
        注文検索API (searchOrder)
        期間全体を一括検索し、ページネーションで全件取得する。

        Args:
            start_date: 検索開始日
            end_date: 検索終了日
            order_progress: 注文進捗リスト（デフォルト: 全件）

        Returns:
            注文番号リスト
        """
        all_order_numbers = []
        page = 1

        while True:
            payload = {
                "dateType": 1,  # 注文日
                "startDatetime": start_date.strftime("%Y-%m-%dT%H:%M:%S+0900"),
                "endDatetime": end_date.strftime("%Y-%m-%dT%H:%M:%S+0900"),
                "PaginationRequestModel": {
                    "requestRecordsAmount": MAX_ORDERS_PER_REQUEST,
                    "requestPage": page,
                }
            }

            if order_progress:
                payload["orderProgressList"] = order_progress

            result = self._make_request(RAKUTEN_SEARCH_ORDER_URL, payload)

            order_model_list = result.get("orderNumberList", [])
            if not order_model_list:
                break

            all_order_numbers.extend(order_model_list)

            pagination = result.get("PaginationResponseModel", {})
            total_pages = pagination.get("totalPages", 1)

            if page >= total_pages:
                break

            page += 1
            time.sleep(0.2)

        return all_order_numbers

    def get_orders(self, order_numbers: list) -> list:
        """
        注文詳細取得API (getOrder)
        100件ずつのバッチを並列実行して高速化。

        Args:
            order_numbers: 注文番号リスト

        Returns:
            注文詳細リスト
        """
        if not order_numbers:
            return []

        from concurrent.futures import ThreadPoolExecutor

        batch_size = 100
        batches = [
            order_numbers[i:i + batch_size]
            for i in range(0, len(order_numbers), batch_size)
        ]

        def _fetch_batch(batch):
            payload = {
                "orderNumberList": batch,
                "version": 7,
            }
            result = self._make_request(RAKUTEN_GET_ORDER_URL, payload)
            return result.get("OrderModelList", [])

        all_orders = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_fetch_batch, b) for b in batches]
            for future in futures:
                all_orders.extend(future.result())

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
        print(f"売上データ取得中: {start_date.date()} ~ {end_date.date()}")

        # 注文番号を検索
        order_numbers = self.search_orders(start_date, end_date)
        print(f"  注文件数: {len(order_numbers)}件")

        if not order_numbers:
            return []

        # 注文詳細を取得
        orders = self.get_orders(order_numbers)
        print(f"  詳細取得完了: {len(orders)}件")

        return orders

    def test_connection(self) -> bool:
        """API接続テスト"""
        try:
            # 直近1日のデータで接続テスト
            end_date = datetime.now()
            start_date = end_date - timedelta(days=1)

            payload = {
                "dateType": 1,
                "startDatetime": start_date.strftime("%Y-%m-%dT%H:%M:%S+0900"),
                "endDatetime": end_date.strftime("%Y-%m-%dT%H:%M:%S+0900"),
                "PaginationRequestModel": {
                    "requestRecordsAmount": 1,
                    "requestPage": 1,
                }
            }

            self._make_request(RAKUTEN_SEARCH_ORDER_URL, payload)
            return True

        except RakutenAPIError:
            return False


def get_all_rakuten_apis(test_connection: bool = False) -> list:
    """設定済みの全店舗のAPIクライアントを取得

    Args:
        test_connection: Trueの場合、接続テストに成功した店舗のみ返す
    """
    apis = []
    for store in RAKUTEN_STORES:
        try:
            api = RakutenAPI(
                service_secret=store["service_secret"],
                license_key=store["license_key"],
                store_name=store["name"]
            )
            if test_connection:
                if api.test_connection():
                    apis.append(api)
            else:
                apis.append(api)
        except RakutenAPIError:
            continue
        except Exception:
            continue
    return apis


def _fetch_store_sales(api, start_date: datetime, end_date: datetime) -> list:
    """1店舗分の売上データを取得（並列処理用）"""
    try:
        print(f"[{api.store_name}] ", end="")
        orders = api.get_sales_data(start_date, end_date)
        for order in orders:
            order["_store_name"] = api.store_name
        return orders
    except RakutenAPIError as e:
        print(f"[{api.store_name}] エラー: {e}")
        return []
    except Exception as e:
        print(f"[{api.store_name}] 予期しないエラー: {e}")
        return []


def get_all_stores_sales_data(start_date: datetime, end_date: datetime) -> list:
    """全店舗の売上データを並列取得"""
    from concurrent.futures import ThreadPoolExecutor

    apis = get_all_rakuten_apis()

    if not apis:
        print("警告: 設定された店舗がありません")
        return []

    all_orders = []

    # 2店舗以上なら並列取得
    if len(apis) >= 2:
        with ThreadPoolExecutor(max_workers=len(apis)) as executor:
            futures = [
                executor.submit(_fetch_store_sales, api, start_date, end_date)
                for api in apis
            ]
            for future in futures:
                all_orders.extend(future.result())
    else:
        for api in apis:
            all_orders.extend(_fetch_store_sales(api, start_date, end_date))

    return all_orders


def main():
    """テスト実行"""
    import argparse

    parser = argparse.ArgumentParser(description="楽天RMS API連携テスト")
    parser.add_argument("--test", action="store_true", help="接続テストを実行")
    args = parser.parse_args()

    if args.test:
        print("楽天RMS API 接続テスト...")
        apis = get_all_rakuten_apis()
        if not apis:
            print("✗ 設定された店舗がありません")
        else:
            for api in apis:
                try:
                    if api.test_connection():
                        print(f"✓ {api.store_name}: 接続成功")
                    else:
                        print(f"✗ {api.store_name}: 接続失敗")
                except RakutenAPIError as e:
                    print(f"✗ {api.store_name}: エラー - {e}")


if __name__ == "__main__":
    main()
