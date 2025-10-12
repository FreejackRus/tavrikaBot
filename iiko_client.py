import requests
import logging
from typing import Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class IikoClient:
    def __init__(self, base_url: str, login: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.login = login
        self.password = password
        self.token: Optional[str] = None
        self.session = requests.Session()
        # Настраиваем ретраи для устойчивости к сетевым ошибкам и 5xx
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def auth(self) -> str:
        url = f"{self.base_url}/resto/api/auth"
        params = {"login": self.login, "pass": self.password}
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        # iiko returns token as plain text
        self.token = resp.text.strip()
        return self.token

    def ensure_token(self) -> str:
        if not self.token:
            return self.auth()
        return self.token

    def fetch_olap_transactions(self, date_from: str, date_to: str) -> Dict[str, Any]:
        """Запрос OLAP (TRANSACTIONS) через прямой POST, без presetId.

        Формирует свод по ДДС: балансы и операционные движения по счетам
        "Главная касса" и "Торговые кассы" за указанный период.
        """
        token = self.ensure_token()
        url = f"{self.base_url}/resto/api/v2/reports/olap"
        params = {"key": token, "format": "json"}
        payload = {
            "reportType": "TRANSACTIONS",
            "buildSummary": True,
            "groupByRowFields": [
                "CashFlowCategory.Type",
                "CashFlowCategory.HierarchyLevel1",
                "CashFlowCategory.HierarchyLevel2",
                "CashFlowCategory.HierarchyLevel3",
            ],
            "groupByColFields": ["Account.Name"],
            "aggregateFields": [
                "Sum.Incoming",
                "Sum.Outgoing",
                "StartBalance.Money",
                "FinalBalance.Money",
            ],
            "filters": {
                "DateTime.DateTyped": {
                    "filterType": "DateRange",
                    "from": date_from,
                    "to": date_to,
                    "includeLow": True,
                    "includeHigh": True,
                },
                "Account.IsCashFlowAccount": {
                    "filterType": "IncludeValues",
                    "values": ["CASH_FLOW"],
                },
                "Account.Name": {
                    "filterType": "IncludeValues",
                    "values": ["Главная касса", "Торговые кассы"],
                },
            },
        }
        try:
            logging.info("OLAP POST %s", url)
        except Exception:
            pass
        resp = self.session.post(url, json=payload, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def fetch_olap_by_preset(self, preset_id: str, date_from: str, date_to: str) -> Dict[str, Any]:
        """
        Запрос OLAP по сохранённому пресету (GET byPresetId).

        Возвращает структуру, аналогичную POST OLAP, за указанный интервал дат.
        """
        token = self.ensure_token()
        url = f"{self.base_url}/resto/api/v2/reports/olap/byPresetId/{preset_id}"
        params = {
            "key": token,
            "dateFrom": date_from,
            "dateTo": date_to,
            # формат обычно JSON по умолчанию, но явно зададим при наличии поддержки
            # некоторые инсталляции игнорируют параметр format для byPresetId
            # оставляем без format, чтобы не ломать совместимость
        }
        try:
            logging.info("OLAP GET byPresetId %s", url)
        except Exception:
            pass
        resp = self.session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()