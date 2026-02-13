"""メルカリShops API連携モジュール"""
import json
from datetime import datetime, timedelta
from typing import Optional
import requests

import sys
sys.path.insert(0, ".")
from config.settings import MERCARI_ACCESS_TOKEN


class MercariAPIError(Exception):
    """Mercari API例外クラス"""
    pass


class MercariShopsAPI:
    """メルカリShops API クライアント (GraphQL)"""

    API_ENDPOINT = "https://api.mercari-shops.com/v1/graphql"

    def __init__(self, access_token: Optional[str] = None):
        self.access_token = access_token or MERCARI_ACCESS_TOKEN

    def _make_request(self, query: str, variables: dict = None) -> dict:
        """GraphQLリクエストを実行"""
        if not self.access_token:
            raise MercariAPIError("アクセストークンが設定されていません")

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "User-Agent": "SalesChecker/1.0",
        }

        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        try:
            response = requests.post(
                self.API_ENDPOINT,
                headers=headers,
                json=payload,
                timeout=60
            )

            if response.status_code == 404:
                raise MercariAPIError("APIアクセスが拒否されました。IPアドレスの登録が必要な可能性があります。")

            if response.status_code == 401:
                raise MercariAPIError("認証エラー: アクセストークンが無効です")

            if response.status_code != 200:
                raise MercariAPIError(f"APIエラー: HTTP {response.status_code}")

            result = response.json()

            if "errors" in result:
                error_msg = result["errors"][0].get("message", "Unknown error")
                raise MercariAPIError(f"GraphQLエラー: {error_msg}")

            return result.get("data", {})

        except requests.RequestException as e:
            raise MercariAPIError(f"通信エラー: {str(e)}")

    def test_connection(self) -> bool:
        """API接続テスト"""
        try:
            # シンプルなクエリでテスト
            query = """
            query {
                shop {
                    id
                    name
                }
            }
            """
            result = self._make_request(query)
            return "shop" in result
        except MercariAPIError:
            return False

    def get_shop_info(self) -> dict:
        """ショップ情報を取得"""
        query = """
        query {
            shop {
                id
                name
                description
            }
        }
        """
        result = self._make_request(query)
        return result.get("shop", {})

    def get_orders(
        self,
        start_date: datetime = None,
        end_date: datetime = None,
        status: str = None,
        limit: int = 100,
        cursor: str = None
    ) -> dict:
        """
        注文一覧を取得

        Args:
            start_date: 検索開始日
            end_date: 検索終了日
            status: 注文ステータス (WAITING_FOR_PAYMENT, WAITING_FOR_SHIPMENT, SHIPPED, COMPLETED, CANCELLED)
            limit: 取得件数（最大100）
            cursor: ページングカーソル

        Returns:
            注文データ
        """
        query = """
        query GetOrders($first: Int, $after: String, $filter: OrderFilterInput) {
            orders(first: $first, after: $after, filter: $filter) {
                edges {
                    node {
                        id
                        orderNumber
                        status
                        createdAt
                        paidAt
                        shippedAt
                        completedAt
                        product {
                            id
                            name
                            price
                        }
                        shippingInfo {
                            name
                            prefecture
                        }
                        payment {
                            productPrice
                            shippingFee
                            totalPrice
                            platformFee
                            settlementAmount
                        }
                    }
                    cursor
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        """

        variables = {
            "first": min(limit, 100),
        }

        if cursor:
            variables["after"] = cursor

        # フィルター条件
        filter_input = {}
        if status:
            filter_input["status"] = status

        if filter_input:
            variables["filter"] = filter_input

        result = self._make_request(query, variables)
        return result.get("orders", {})

    def get_all_orders(
        self,
        start_date: datetime = None,
        end_date: datetime = None,
        status: str = None
    ) -> list:
        """
        全注文を取得（ページング対応）

        Args:
            start_date: 検索開始日
            end_date: 検索終了日
            status: 注文ステータス

        Returns:
            注文リスト
        """
        all_orders = []
        cursor = None
        has_next = True

        while has_next:
            result = self.get_orders(
                start_date=start_date,
                end_date=end_date,
                status=status,
                cursor=cursor
            )

            edges = result.get("edges", [])
            for edge in edges:
                order = edge.get("node", {})

                # 日付フィルタリング（API側でサポートされていない場合）
                if start_date or end_date:
                    order_date_str = order.get("createdAt") or order.get("paidAt")
                    if order_date_str:
                        try:
                            order_date = datetime.fromisoformat(order_date_str.replace("Z", "+00:00"))
                            order_date = order_date.replace(tzinfo=None)

                            if start_date and order_date < start_date:
                                continue
                            if end_date and order_date > end_date:
                                continue
                        except ValueError:
                            pass

                all_orders.append(order)

            page_info = result.get("pageInfo", {})
            has_next = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")

            # 無限ループ防止
            if not edges:
                break

        return all_orders

    def get_sales_data(self, start_date: datetime, end_date: datetime) -> list:
        """
        指定期間の売上データを取得

        Args:
            start_date: 開始日
            end_date: 終了日

        Returns:
            注文リスト
        """
        print(f"メルカリShops売上データ取得中: {start_date.date()} ~ {end_date.date()}")

        # 完了済み注文を取得
        orders = self.get_all_orders(
            start_date=start_date,
            end_date=end_date,
            status="COMPLETED"
        )

        # 発送済み注文も含める
        shipped_orders = self.get_all_orders(
            start_date=start_date,
            end_date=end_date,
            status="SHIPPED"
        )

        # 入金待ち以外の注文も含める
        paid_orders = self.get_all_orders(
            start_date=start_date,
            end_date=end_date,
            status="WAITING_FOR_SHIPMENT"
        )

        all_orders = orders + shipped_orders + paid_orders

        # 重複除去
        seen = set()
        unique_orders = []
        for order in all_orders:
            order_id = order.get("id")
            if order_id and order_id not in seen:
                seen.add(order_id)
                unique_orders.append(order)

        print(f"  注文件数: {len(unique_orders)}件")
        return unique_orders

    def is_configured(self) -> bool:
        """APIが設定済みかどうか"""
        return bool(self.access_token)


def main():
    """テスト実行"""
    import argparse

    parser = argparse.ArgumentParser(description="メルカリShops API連携テスト")
    parser.add_argument("--test", action="store_true", help="接続テストを実行")
    args = parser.parse_args()

    if args.test:
        print("メルカリShops API 接続テスト...")
        try:
            api = MercariShopsAPI()
            if api.test_connection():
                print("✓ 接続成功")
                shop = api.get_shop_info()
                print(f"  ショップ名: {shop.get('name', 'N/A')}")
            else:
                print("✗ 接続失敗")
        except MercariAPIError as e:
            print(f"✗ エラー: {e}")


if __name__ == "__main__":
    main()
