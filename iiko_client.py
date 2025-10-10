from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List

import requests


class IikoClient:
    def __init__(self, base_url: str, login: str | None, password: str | None):
        self.base_url = base_url.rstrip("/")
        self.login = login
        self.password = password
        self.token: str | None = None
        self.token_expires_at: float = 0

    def auth(self) -> None:
        if not self.login or not self.password:
            raise RuntimeError("IIKO_LOGIN/IIKO_PASSWORD не заданы")
        url = f"{self.base_url}/api/0/auth/access_token"
        resp = requests.post(url, data={"user_id": self.login, "user_secret": self.password}, timeout=30)
        resp.raise_for_status()
        self.token = resp.text.strip('"')
        # token lifetime ~ 10 minutes by docs; renew earlier
        self.token_expires_at = time.time() + 8 * 60

    def ensure_token(self) -> None:
        if not self.token or time.time() >= self.token_expires_at:
            self.auth()

    def fetch_olap_transactions(self, dt_from: datetime, dt_to: datetime) -> List[Dict]:
        self.ensure_token()
        url = f"{self.base_url}/api/0/olap_report/report"
        payload = {
            "reportType": "TRANSACTIONS",
            "groupByRowFields": ["date", "category", "account"],
            "aggregateFields": ["amount", "isExpense"],
            "filters": {
                "filterType": "and",
                "operation": "AND",
                "filters": [
                    {
                        "filterType": "date",
                        "field": "date",
                        "operation": "RANGE",
                        "valueFrom": dt_from.strftime("%Y-%m-%dT00:00:00"),
                        "valueTo": dt_to.strftime("%Y-%m-%dT23:59:59"),
                    }
                ],
            },
        }
        headers = {"Authorization": f"Bearer {self.token}"}
        resp = requests.post(url, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("data", [])
        # Normalize field names
        normalized = []
        for r in rows:
            normalized.append(
                {
                    "date": r.get("date"),
                    "categoryName": r.get("category"),
                    "accountName": r.get("account"),
                    "amount": float(r.get("amount", 0) or 0),
                    "isExpense": bool(r.get("isExpense")),
                }
            )
        return normalized
