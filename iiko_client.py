import requests
import logging
from typing import Dict, Any, Optional


class IikoClient:
    def __init__(self, base_url: str, login: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.login = login
        self.password = password
        self.token: Optional[str] = None
        self.session = requests.Session()

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