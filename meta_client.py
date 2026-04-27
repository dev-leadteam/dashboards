"""
meta_client.py
─────────────────────────────────────────────────────────────
Cliente ligero para la Graph API de Meta (Facebook Ads).
Solo los endpoints que necesita el dashboard — no pretende
ser una SDK completa.

Requiere:
    META_ACCESS_TOKEN   (token long-lived o system user)
    META_API_VERSION    (ej. v21.0)
"""
from __future__ import annotations
import os
import time
import requests
from typing import Iterable


BASE = "https://graph.facebook.com"


class MetaClient:
    def __init__(self, access_token: str | None = None, api_version: str | None = None):
        # Acepta META_TOKEN (convención del repo) o META_ACCESS_TOKEN (legado local)
        self.token = (
            access_token
            or os.environ.get("META_TOKEN")
            or os.environ.get("META_ACCESS_TOKEN")
        )
        if not self.token:
            raise RuntimeError(
                "Missing Meta token — set META_TOKEN (o META_ACCESS_TOKEN) en el entorno."
            )
        self.version = api_version or os.environ.get("META_API_VERSION", "v21.0")
        self.session = requests.Session()

    # ── low-level ──────────────────────────────────────────────────
    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{BASE}/{self.version}/{path.lstrip('/')}"
        params = {**(params or {}), "access_token": self.token}
        r = self.session.get(url, params=params, timeout=60)
        if r.status_code == 429 or (r.status_code == 400 and "rate limit" in r.text.lower()):
            time.sleep(30)
            r = self.session.get(url, params=params, timeout=60)
        if not r.ok:
            try:
                err = r.json().get("error", {})
                detail = (
                    f"Meta API {r.status_code}: "
                    f"code={err.get('code')} subcode={err.get('error_subcode')} "
                    f"type={err.get('type')} msg={err.get('message')!r} "
                    f"user_msg={err.get('error_user_msg')!r} "
                    f"fbtrace={err.get('fbtrace_id')}"
                )
            except Exception:
                detail = f"Meta API {r.status_code}: {r.text[:400]}"
            raise RuntimeError(detail)
        return r.json()

    def _paginate(self, path: str, params: dict | None = None) -> Iterable[dict]:
        data = self._get(path, params)
        while True:
            for row in data.get("data", []):
                yield row
            nxt = data.get("paging", {}).get("next")
            if not nxt:
                break
            r = self.session.get(nxt, timeout=60)
            r.raise_for_status()
            data = r.json()

    # ── high-level helpers ────────────────────────────────────────
    def list_ad_sets(self, campaign_id: str) -> list[dict]:
        return list(self._paginate(
            f"{campaign_id}/adsets",
            params={"fields": "id,name,status,campaign_id,daily_budget,lifetime_budget",
                    "limit": 200},
        ))

    def list_ads(self, campaign_id: str) -> list[dict]:
        return list(self._paginate(
            f"{campaign_id}/ads",
            params={"fields": "id,name,adset_id,status",
                    "limit": 500},
        ))

    def get_insights(
        self,
        object_id: str,
        *,
        level: str = "adset",
        date_start: str,
        date_end: str,
        breakdowns: str | None = None,
        time_increment: int | str | None = None,
    ) -> list[dict]:
        params = {
            "level": level,
            "time_range": f'{{"since":"{date_start}","until":"{date_end}"}}',
            "fields": ",".join([
                # date_start/date_stop sólo se llenan cuando time_increment
                # está activo; cuando no, Meta simplemente los ignora.
                "date_start", "date_stop",
                "adset_id", "adset_name",
                "ad_id", "ad_name",
                "campaign_id", "campaign_name",
                "impressions", "clicks", "spend", "reach",
                "ctr", "cpm",
                "actions",
                # ↓ 'conversions' es DISTINTO de 'actions'. Aquí viven los
                # eventos de conversión optimizados por Meta (incl. offline
                # conversions como start_trial_offline / start_trial_total).
                # Es el campo que usa la UI de Ads Manager para "Start Trial".
                "conversions",
            ]),
            "use_unified_attribution_setting": "true",
            "limit": 500,
        }
        if breakdowns:
            params["breakdowns"] = breakdowns
        if time_increment is not None:
            # time_increment=1 → una fila por día por objeto (ad/adset/campaign).
            params["time_increment"] = str(time_increment)
        return list(self._paginate(f"{object_id}/insights", params=params))

    def get_daily_insights(
        self,
        campaign_id: str,
        *,
        date_start: str,
        date_end: str,
    ) -> list[dict]:
        params = {
            "level": "campaign",
            "time_range": f'{{"since":"{date_start}","until":"{date_end}"}}',
            "time_increment": 1,
            "fields": "date_start,date_stop,impressions,clicks,spend,reach,actions,conversions",
            "use_unified_attribution_setting": "true",
            "limit": 200,
        }
        return list(self._paginate(f"{campaign_id}/insights", params=params))


# ─── helpers para extraer "actions" ────────────────────────────────
LEAD_ACTION_TYPES = {"onsite_conversion.lead_grouped", "leadgen_grouped", "leadgen.other"}
PURCHASE_ACTION_TYPES = {"omni_purchase"}

# Trials — SWEAT440 los trackea como OFFLINE CONVERSIONS (CRM → Meta CAPI).
# Meta los reporta en `conversions[]`, NO en `actions[]`.
# start_trial_total es el agregado oficial y es la cifra que muestra
# la UI de Ads Manager en "Start Trial".
TRIAL_ACTION_TYPES = {
    "start_trial_total",          # agregado oficial desde conversions[]
    "omni_start_trial",           # fallbacks (raramente disparan en SWEAT440)
    "start_trial",
    "offsite_conversion.fb_pixel_start_trial",
    "onsite_conversion.start_trial",
}


def count_actions(actions: list[dict] | None, wanted: set[str]) -> int:
    if not actions:
        return 0
    total = 0
    for a in actions:
        if a.get("action_type") in wanted:
            try:
                total += int(float(a.get("value", 0)))
            except (TypeError, ValueError):
                pass
    return total


def leads_of(row: dict) -> int:
    return count_actions(row.get("actions"), LEAD_ACTION_TYPES)


def purchases_of(row: dict) -> int:
    return count_actions(row.get("actions"), PURCHASE_ACTION_TYPES)


def trials_of(row: dict) -> int:
    """
    Trials se leen PRIMERO de `conversions[]` (start_trial_total — incluye
    offline conversions del CRM). Fallback a `actions[]` si `conversions[]`
    no devuelve nada.
    """
    via_conversions = count_actions(row.get("conversions"), TRIAL_ACTION_TYPES)
    if via_conversions > 0:
        return via_conversions
    return count_actions(row.get("actions"), TRIAL_ACTION_TYPES)
