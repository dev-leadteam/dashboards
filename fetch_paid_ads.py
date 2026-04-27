"""
fetch_paid_ads.py
─────────────────────────────────────────────────────────────
ETL Meta Ads → paid-ads-data.json

Diseñado para correr en GitHub Actions (vía refresh-paid-ads.yml).

- Reutiliza MetaClient y los helpers (leads_of, purchases_of, trials_of)
  del proyecto original (ver meta_client.py adjunto).
- Lee la misma config.yaml.
- NO escribe a Google Sheets. Emite un único JSON en la raíz del repo
  (paid-ads-data.json) que consume paid-ads.html.

Trials se leen desde el campo `conversions[]` (start_trial_total), que es
donde Meta reporta las offline conversions que sube el CRM de SWEAT440.
Esta es la cifra que coincide con la UI de Ads Manager.

Variables de entorno requeridas (GitHub Secrets en prod):
    META_ACCESS_TOKEN
    META_API_VERSION   (opcional, default v21.0)
"""
from __future__ import annotations
import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from datetime import datetime, timezone
from pathlib import Path

import yaml

# local (mismo folder)
from meta_client import MetaClient, leads_of, purchases_of, trials_of


# ── paths ────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = REPO_ROOT / "config.yaml"
OUT_PATH = REPO_ROOT / "paid-ads-data.json"

# Ventana rolling para las series de tiempo diarias (por dimensión).
# Los totales / tablas agregadas siguen usando el rango completo de la
# campaña (config.yaml: date_start → date_end). Sólo el `daily_series`
# queda acotado a esta ventana.
DAILY_WINDOW_DAYS = 90


# ── logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("paid-ads-etl")


# ── helpers de clasificación (copiados de meta_to_sheets.py) ─────────
def match_studio(name: str, studios: list[dict]) -> dict | None:
    n = (name or "").lower()
    for s in studios:
        if s.get("match") and s["match"].lower() in n:
            return s
    return None


def _has_token(name: str, token: str) -> bool:
    if not name or not token:
        return False
    norm = re.sub(r"[_\-/|]+", " ", name.upper())
    tok = token.upper()
    pattern = r"(?:(?<=^)|(?<=\s))" + re.escape(tok) + r"(?=$|\s)"
    return re.search(pattern, norm) is not None


def match_audience(name: str, tokens_by_aud: dict[str, list[str]]) -> str | None:
    for aud, tokens in tokens_by_aud.items():
        for tok in tokens:
            if _has_token(name, tok):
                return aud
    return None


def match_pillar(name: str, tokens_by_pillar: dict[str, list[str]]) -> str | None:
    for pillar, tokens in tokens_by_pillar.items():
        for tok in tokens:
            if _has_token(name, tok):
                return pillar
    return None


_STOPWORDS = {
    "V1", "V2", "V3", "V4", "V5", "A", "B", "C", "TEST", "VER", "VERSION",
    "WAFM", "WIN", "FREE", "MONTH", "CLASS", "OPEN", "STUDIOS", "STUDIO",
    "PROMO", "AD", "ADS", "COPY", "CREATIVE",
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
    "ENE", "ABR", "AGO", "DIC",
}


def detect_concept(
    ad_name: str,
    *,
    studio_match: str | None,
    audience_tokens_flat: set[str],
    pillar_tokens_flat: set[str],
    state_code: str | None = None,
) -> str:
    if not ad_name:
        return "(other)"
    text = ad_name

    if studio_match:
        text = re.sub(re.escape(studio_match), " ", text, flags=re.IGNORECASE)

    text = re.sub(r"\b[A-Z]{2}[\-\s]?\d{2,3}\b", " ", text)
    text = re.sub(r"[_\-/|]+", " ", text)

    all_class_tokens = {t.upper() for t in audience_tokens_flat} | {t.upper() for t in pillar_tokens_flat}
    words_out = []
    for raw in re.split(r"\s+", text):
        w = raw.strip()
        if not w:
            continue
        upper = w.upper()
        if upper in all_class_tokens:
            continue
        if upper in _STOPWORDS:
            continue
        if len(w) == 2 and w.isalpha() and w.isupper():
            continue
        if re.fullmatch(r"\d+", w):
            continue
        if re.fullmatch(r"[Vv]\d+", w):
            continue
        if len(w) < 3:
            continue
        words_out.append(w)

    if not words_out:
        return "(other)"
    for w in words_out:
        if w[0].isupper():
            return w
    return words_out[0]


def safe_float(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


# ── núcleo: procesar 1 campaña ───────────────────────────────────────
def run_one(meta: MetaClient, campaign_key: str, c: dict) -> dict:
    """
    Extrae Meta Ads data de 1 campaña y devuelve un dict con TODA la
    estructura que consumirá el HTML (totals, studios, audiences, pillars,
    concepts, studio_pillars, studio_concepts, daily).
    """
    log.info(f"── Campaign: {c['display_name']} ({c['period_label']}) [{campaign_key}]")

    ad_sets = meta.list_ad_sets(c["campaign_id"])
    log.info(f"  {len(ad_sets)} ad sets")
    adset_by_id = {a["id"]: a for a in ad_sets}

    ads = meta.list_ads(c["campaign_id"])
    log.info(f"  {len(ads)} ads")

    # Insights a nivel AD — pedimos por AD SET para evitar subcode=1504018
    ad_insights: list[dict] = []
    for adset in ad_sets:
        try:
            rows = meta.get_insights(
                adset["id"],
                level="ad",
                date_start=c["date_start"],
                date_end=c["date_end"],
            )
            ad_insights.extend(rows)
        except Exception as e:
            log.warning(f"  ad set {adset.get('name','?')} ({adset['id']}) failed: {e}")
    log.info(f"  {len(ad_insights)} ad-level insight rows")

    daily = meta.get_daily_insights(
        c["campaign_id"],
        date_start=c["date_start"],
        date_end=c["date_end"],
    )
    log.info(f"  {len(daily)} daily rows")

    studios_cfg = c["studios"]

    def _empty_bucket():
        return {"spend": 0.0, "impressions": 0, "leads": 0, "ads": []}

    studio_agg: dict[str, dict] = {
        s["code"]: {
            "code": s["code"], "name": s["name"], "state": s["state"],
            "impressions": 0, "clicks": 0, "spend": 0.0, "reach": 0,
            "leads": 0, "purchases": 0, "trials": 0,
            "_audiences": defaultdict(_empty_bucket),
            "_pillars":   defaultdict(_empty_bucket),
            "_concepts":  defaultdict(_empty_bucket),
        } for s in studios_cfg
    }

    global_aud:     dict[str, dict] = defaultdict(_empty_bucket)
    global_pillar:  dict[str, dict] = defaultdict(_empty_bucket)
    global_concept: dict[str, dict] = defaultdict(_empty_bucket)

    aud_tokens_cfg    = c.get("audience_tokens", {}) or {}
    pillar_tokens_cfg = c.get("pillar_tokens", {}) or {}
    aud_flat    = {t for toks in aud_tokens_cfg.values() for t in toks}
    pillar_flat = {t for toks in pillar_tokens_cfg.values() for t in toks}

    # Mapa ad_id → dimensiones (studio/audience/pillar/concept). Se llena
    # en este loop y luego se reutiliza para rebanar los insights daily
    # sin reclasificar ad por ad × día.
    ad_dims: dict[str, dict] = {}

    for ins in ad_insights:
        adset = adset_by_id.get(ins.get("adset_id"), {})
        studio = match_studio(adset.get("name", ""), studios_cfg)
        if not studio:
            continue

        ad_name = ins.get("ad_name", "")
        aud    = match_audience(ad_name, aud_tokens_cfg)
        pillar = match_pillar(ad_name, pillar_tokens_cfg)
        concept = detect_concept(
            ad_name,
            studio_match=studio.get("match"),
            audience_tokens_flat=aud_flat,
            pillar_tokens_flat=pillar_flat,
            state_code=studio.get("state"),
        )

        spend = safe_float(ins.get("spend"))
        impressions = int(safe_float(ins.get("impressions")))
        clicks = int(safe_float(ins.get("clicks")))
        reach = int(safe_float(ins.get("reach")))
        leads = leads_of(ins)
        purchases = purchases_of(ins)
        trials = trials_of(ins)

        agg = studio_agg[studio["code"]]
        agg["impressions"] += impressions
        agg["clicks"] += clicks
        agg["spend"] += spend
        agg["reach"] += reach
        agg["leads"] += leads
        agg["purchases"] += purchases
        agg["trials"] += trials

        def _bump(bucket: dict, ad: str):
            bucket["spend"] += spend
            bucket["impressions"] += impressions
            bucket["leads"] += leads
            if ad and ad not in bucket["ads"]:
                bucket["ads"].append(ad)

        if aud:
            _bump(agg["_audiences"][aud], ad_name)
            _bump(global_aud[aud], ad_name)
        if pillar:
            _bump(agg["_pillars"][pillar], ad_name)
            _bump(global_pillar[pillar], ad_name)
        if concept and concept != "(other)":
            _bump(agg["_concepts"][concept], ad_name)
            _bump(global_concept[concept], ad_name)

        ad_id = ins.get("ad_id")
        if ad_id:
            ad_dims[ad_id] = {
                "studio_code": studio["code"],
                "audience":    aud,
                "pillar":      pillar,
                "concept":     concept if concept and concept != "(other)" else None,
            }

    # Totales
    totals = {k: 0 for k in ["impressions", "clicks", "reach", "leads", "purchases", "trials"]}
    totals["spend"] = 0.0
    for s in studio_agg.values():
        for k in ["impressions", "clicks", "reach", "leads", "purchases", "trials"]:
            totals[k] += s[k]
        totals["spend"] += s["spend"]
    totals["spend"] = round(totals["spend"], 2)
    totals["ctr"] = round((totals["clicks"] / totals["impressions"] * 100), 2) if totals["impressions"] else 0
    totals["cpm"] = round((totals["spend"] / totals["impressions"] * 1000), 2) if totals["impressions"] else 0
    totals["cpl"] = round((totals["spend"] / totals["leads"]), 2) if totals["leads"] else 0

    log.info(
        f"  totals: spend=${totals['spend']:.2f}  leads={totals['leads']}  "
        f"trials={totals['trials']}  purchases={totals['purchases']}  "
        f"CPL=${totals['cpl']:.2f}"
    )

    # ── armar estructuras JSON ──────────────────────────────────────
    studios_out = []
    for s in studios_cfg:
        a = studio_agg[s["code"]]
        cpl = round(a["spend"] / a["leads"], 2) if a["leads"] else 0
        ctr = round(a["clicks"] / a["impressions"] * 100, 2) if a["impressions"] else 0
        cpm = round(a["spend"] / a["impressions"] * 1000, 2) if a["impressions"] else 0
        studios_out.append({
            "code": a["code"],
            "name": a["name"],
            "state": a["state"],
            "impressions": a["impressions"],
            "clicks": a["clicks"],
            "spend": round(a["spend"], 2),
            "reach": a["reach"],
            "ctr": ctr,
            "cpm": cpm,
            "leads": a["leads"],
            "cpl": cpl,
            "purchases": a["purchases"],
            "trials": a["trials"],
        })

    audiences_out = []
    for code, agg in studio_agg.items():
        for aud, v in agg["_audiences"].items():
            audiences_out.append({
                "studio_code": code,
                "audience": aud,
                "spend": round(v["spend"], 2),
                "impressions": v["impressions"],
                "leads": v["leads"],
                "ads": v["ads"],
            })

    pillars_out = []
    for pillar, v in global_pillar.items():
        cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
        pillars_out.append({
            "pillar": pillar,
            "spend": round(v["spend"], 2),
            "impressions": v["impressions"],
            "leads": v["leads"],
            "cpl": cpl,
            "ads": v["ads"][:20],
        })

    concepts_out = []
    for concept, v in global_concept.items():
        cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
        concepts_out.append({
            "concept": concept,
            "spend": round(v["spend"], 2),
            "impressions": v["impressions"],
            "leads": v["leads"],
            "cpl": cpl,
            "ads": v["ads"][:20],
        })

    studio_pillars_out = []
    for code, agg in studio_agg.items():
        for pillar, v in agg["_pillars"].items():
            cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
            studio_pillars_out.append({
                "studio_code": code,
                "pillar": pillar,
                "spend": round(v["spend"], 2),
                "impressions": v["impressions"],
                "leads": v["leads"],
                "cpl": cpl,
            })

    studio_concepts_out = []
    for code, agg in studio_agg.items():
        for concept, v in agg["_concepts"].items():
            cpl = round(v["spend"] / v["leads"], 2) if v["leads"] else 0
            studio_concepts_out.append({
                "studio_code": code,
                "concept": concept,
                "spend": round(v["spend"], 2),
                "impressions": v["impressions"],
                "leads": v["leads"],
                "cpl": cpl,
            })

    daily_out = []
    for d in daily:
        daily_out.append({
            "date": d.get("date_start"),
            "impressions": int(safe_float(d.get("impressions"))),
            "clicks": int(safe_float(d.get("clicks"))),
            "spend": round(safe_float(d.get("spend")), 2),
            "reach": int(safe_float(d.get("reach"))),
            "leads": leads_of(d),
            "purchases": purchases_of(d),
            "trials": trials_of(d),
        })

    # ── daily time series (rolling 90d por dimensión) ─────────────────
    # Traemos ad-level × día en una ventana rolling y agregamos en Python
    # por cada dimensión (studio / audience / pillar / concept y los
    # cruces con studio). Reusamos ad_dims[] construido arriba.
    today = date.today()
    window_start = (today - timedelta(days=DAILY_WINDOW_DAYS)).isoformat()
    today_iso = today.isoformat()
    daily_start = max(c["date_start"], window_start)
    daily_end   = min(c["date_end"], today_iso)

    daily_series: dict = {
        "window_start": daily_start,
        "window_end":   daily_end,
        "window_days":  DAILY_WINDOW_DAYS,
        "campaign":           [],
        "by_studio":          [],
        "by_audience":        [],
        "by_pillar":          [],
        "by_concept":         [],
        "by_studio_audience": [],
        "by_studio_pillar":   [],
        "by_studio_concept":  [],
    }

    if daily_start > daily_end:
        log.info(
            f"  daily series: ventana vacía "
            f"(start={daily_start} > end={daily_end}), skip."
        )
    else:
        log.info(
            f"  fetching daily ad×day insights "
            f"[{daily_start} → {daily_end}] …"
        )
        daily_ad_insights: list[dict] = []
        for adset in ad_sets:
            try:
                rows = meta.get_insights(
                    adset["id"],
                    level="ad",
                    date_start=daily_start,
                    date_end=daily_end,
                    time_increment=1,
                )
                daily_ad_insights.extend(rows)
            except Exception as e:
                log.warning(
                    f"  daily ad-level failed for adset "
                    f"{adset.get('name','?')} ({adset['id']}): {e}"
                )
        log.info(f"  {len(daily_ad_insights)} ad×day rows")

        def _empty_d():
            return {"spend": 0.0, "impressions": 0, "clicks": 0,
                    "reach": 0, "leads": 0, "trials": 0, "purchases": 0}

        camp_d        = defaultdict(_empty_d)   # key: date
        d_studio      = defaultdict(_empty_d)   # (studio, date)
        d_aud         = defaultdict(_empty_d)   # (aud, date)
        d_pillar      = defaultdict(_empty_d)   # (pillar, date)
        d_concept     = defaultdict(_empty_d)   # (concept, date)
        d_stu_aud     = defaultdict(_empty_d)   # (studio, aud, date)
        d_stu_pillar  = defaultdict(_empty_d)   # (studio, pillar, date)
        d_stu_concept = defaultdict(_empty_d)   # (studio, concept, date)

        def _bump_d(bucket, spend, impressions, clicks, reach, leads, trials, purchases):
            bucket["spend"]       += spend
            bucket["impressions"] += impressions
            bucket["clicks"]      += clicks
            bucket["reach"]       += reach
            bucket["leads"]       += leads
            bucket["trials"]      += trials
            bucket["purchases"]   += purchases

        for row in daily_ad_insights:
            ad_id = row.get("ad_id")
            dims = ad_dims.get(ad_id)
            if not dims:
                # Ads sin clasificar (no hacen match con ningún studio) se
                # ignoran — igual que en la agregación principal.
                continue
            d = row.get("date_start")
            if not d:
                continue

            spend       = safe_float(row.get("spend"))
            impressions = int(safe_float(row.get("impressions")))
            clicks      = int(safe_float(row.get("clicks")))
            reach       = int(safe_float(row.get("reach")))
            leads       = leads_of(row)
            trials      = trials_of(row)
            purchases   = purchases_of(row)

            _bump_d(camp_d[d], spend, impressions, clicks, reach, leads, trials, purchases)

            sc = dims["studio_code"]
            _bump_d(d_studio[(sc, d)], spend, impressions, clicks, reach, leads, trials, purchases)

            if dims["audience"]:
                a = dims["audience"]
                _bump_d(d_aud[(a, d)],          spend, impressions, clicks, reach, leads, trials, purchases)
                _bump_d(d_stu_aud[(sc, a, d)],  spend, impressions, clicks, reach, leads, trials, purchases)
            if dims["pillar"]:
                p = dims["pillar"]
                _bump_d(d_pillar[(p, d)],         spend, impressions, clicks, reach, leads, trials, purchases)
                _bump_d(d_stu_pillar[(sc, p, d)], spend, impressions, clicks, reach, leads, trials, purchases)
            if dims["concept"]:
                co = dims["concept"]
                _bump_d(d_concept[(co, d)],         spend, impressions, clicks, reach, leads, trials, purchases)
                _bump_d(d_stu_concept[(sc, co, d)], spend, impressions, clicks, reach, leads, trials, purchases)

        def _row_metrics(b: dict) -> dict:
            return {
                "spend":       round(b["spend"], 2),
                "impressions": b["impressions"],
                "clicks":      b["clicks"],
                "reach":       b["reach"],
                "leads":       b["leads"],
                "trials":      b["trials"],
                "purchases":   b["purchases"],
                "cpl": round(b["spend"] / b["leads"], 2)     if b["leads"]     else 0,
                "cpt": round(b["spend"] / b["trials"], 2)    if b["trials"]    else 0,
                "cpp": round(b["spend"] / b["purchases"], 2) if b["purchases"] else 0,
                "ctr": round(b["clicks"] / b["impressions"] * 100, 2) if b["impressions"] else 0,
                "cpm": round(b["spend"] / b["impressions"] * 1000, 2) if b["impressions"] else 0,
            }

        def _emit(data_dict, key_names):
            # key_names termina en "date"; los anteriores son las dimensiones.
            out = []
            for k in sorted(data_dict.keys()):
                if not isinstance(k, tuple):
                    k = (k,)
                row = dict(zip(key_names, k))
                row.update(_row_metrics(data_dict[k]))
                out.append(row)
            return out

        campaign_series = [
            {"date": dt, **_row_metrics(camp_d[dt])}
            for dt in sorted(camp_d.keys())
        ]

        daily_series.update({
            "campaign":           campaign_series,
            "by_studio":          _emit(d_studio,      ["studio_code", "date"]),
            "by_audience":        _emit(d_aud,         ["audience",    "date"]),
            "by_pillar":          _emit(d_pillar,      ["pillar",      "date"]),
            "by_concept":         _emit(d_concept,     ["concept",     "date"]),
            "by_studio_audience": _emit(d_stu_aud,     ["studio_code", "audience", "date"]),
            "by_studio_pillar":   _emit(d_stu_pillar,  ["studio_code", "pillar",   "date"]),
            "by_studio_concept":  _emit(d_stu_concept, ["studio_code", "concept",  "date"]),
        })

        log.info(
            f"  daily series: {len(campaign_series)} days | "
            f"{len(daily_series['by_studio'])} studio×day | "
            f"{len(daily_series['by_audience'])} aud×day | "
            f"{len(daily_series['by_pillar'])} pillar×day | "
            f"{len(daily_series['by_concept'])} concept×day | "
            f"{len(daily_series['by_studio_concept'])} studio×concept×day"
        )

    return {
        "display_name": c["display_name"],
        "period_label": c["period_label"],
        "date_start": c["date_start"],
        "date_end": c["date_end"],
        "totals": totals,
        "studios": studios_out,
        "audiences": audiences_out,
        "pillars": pillars_out,
        "concepts": concepts_out,
        "studio_pillars": studio_pillars_out,
        "studio_concepts": studio_concepts_out,
        "daily": daily_out,
        "daily_series": daily_series,
    }


# ── entry point ──────────────────────────────────────────────────────
def run():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    keys = cfg.get("campaigns_to_track") or [cfg["active_campaign"]]
    active = cfg.get("active_campaign", keys[0])

    meta = MetaClient()

    campaigns_data: dict[str, dict] = {}
    campaigns_index: list[dict] = []

    for key in keys:
        if key not in cfg["campaigns"]:
            log.warning(f"Skipping '{key}' — not in config.campaigns")
            continue
        try:
            data = run_one(meta, key, cfg["campaigns"][key])
        except Exception as e:
            log.exception(f"❌ Campaign '{key}' failed: {e}")
            continue

        campaigns_data[key] = data
        campaigns_index.append({
            "key": key,
            "display_name": data["display_name"],
            "period_label": data["period_label"],
            "date_start": data["date_start"],
            "date_end": data["date_end"],
            "leads": data["totals"]["leads"],
            "spend": data["totals"]["spend"],
            "is_default": key == active,
        })

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "active_campaign": active,
        "campaigns_index": campaigns_index,
        "campaigns": campaigns_data,
    }

    OUT_PATH.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info(f"✅ Wrote {OUT_PATH}  ({OUT_PATH.stat().st_size:,} bytes, "
             f"{len(campaigns_data)} campaign(s))")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        log.exception(f"❌ ETL failed: {e}")
        sys.exit(1)
