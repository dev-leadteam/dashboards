import snowflake.connector
import json
import os
import requests
from datetime import datetime, date, timedelta
from cryptography.hazmat.primitives import serialization

# ── Credentials ───────────────────────────────────────────────────────────────
SF_ACCOUNT   = os.getenv("SF_ACCOUNT",   "MINDBODYORG-PLAYLIST_DATA_MART_SWEAT440")
SF_USER      = os.getenv("SF_USER",      "SWEAT440")
SF_ROLE      = os.getenv("SF_ROLE",      "SYSADMIN")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE  = os.getenv("SF_DATABASE",  "MARKETING_REPORTS")
SF_SCHEMA    = os.getenv("SF_SCHEMA",    "PUBLIC")
SF_TOKEN     = os.getenv("SF_TOKEN")

META_TOKEN   = os.getenv("META_TOKEN")
META_ACT     = os.getenv("META_ACT", "act_1553887681409034")  # Corporate Studios
META_API     = "https://graph.facebook.com/v19.0"

def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def get_action(actions, *types):
    """Extract value for any of the given action types from Meta actions array."""
    for a in (actions or []):
        if a.get("action_type") in types:
            return int(float(a.get("value", 0)))
    return 0

# ════════════════════════════════════════════════════════════════════════════
# SNOWFLAKE
# ════════════════════════════════════════════════════════════════════════════
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account=SF_ACCOUNT, user=SF_USER, token=SF_TOKEN,
    authenticator="programmatic_access_token",
    role=SF_ROLE, warehouse=SF_WAREHOUSE, database=SF_DATABASE, schema=SF_SCHEMA
)
cur = conn.cursor()

# ── Daily: previous quarter start → today ─────────────────────────────────
cur.execute("""
    SELECT
        EVENT_DATE, STUDIO_NAME, SOURCE,
        SUM(SIGNUPS)            AS signups,
        SUM(FIRST_VISITS)       AS first_visits,
        SUM(FIRST_ACTIVATIONS)  AS first_activations,
        SUM(FIRST_SALES)        AS first_sales
    FROM MARKETING_REPORTS.PUBLIC.LEADS
    WHERE EVENT_DATE >= DATEADD('quarter', -1, DATE_TRUNC('quarter', CURRENT_DATE()))
      AND EVENT_DATE <= CURRENT_DATE()
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
""")
daily_detail = [
    {"date": json_serial(r[0]), "studio": r[1], "source": r[2],
     "signups": int(r[3] or 0), "first_visits": int(r[4] or 0),
     "first_activations": int(r[5] or 0), "first_sales": int(r[6] or 0)}
    for r in cur.fetchall()
]

# ── Monthly: older history, 3yr cap ───────────────────────────────────────
cur.execute("""
    SELECT
        DATE_TRUNC('month', EVENT_DATE) AS month,
        STUDIO_NAME, SOURCE,
        SUM(SIGNUPS)            AS signups,
        SUM(FIRST_VISITS)       AS first_visits,
        SUM(FIRST_ACTIVATIONS)  AS first_activations,
        SUM(FIRST_SALES)        AS first_sales
    FROM MARKETING_REPORTS.PUBLIC.LEADS
    WHERE EVENT_DATE <  DATEADD('quarter', -1, DATE_TRUNC('quarter', CURRENT_DATE()))
      AND EVENT_DATE >= DATEADD('year', -3, CURRENT_DATE())
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
""")
monthly_detail = [
    {"month": json_serial(r[0]), "studio": r[1], "source": r[2],
     "signups": int(r[3] or 0), "first_visits": int(r[4] or 0),
     "first_activations": int(r[5] or 0), "first_sales": int(r[6] or 0)}
    for r in cur.fetchall()
]

# ── Studio + source lists ──────────────────────────────────────────────────
cur.execute("SELECT DISTINCT STUDIO_NAME FROM MARKETING_REPORTS.PUBLIC.LEADS WHERE STUDIO_NAME IS NOT NULL ORDER BY 1")
studios = [r[0] for r in cur.fetchall()]

cur.execute("SELECT DISTINCT SOURCE FROM MARKETING_REPORTS.PUBLIC.LEADS WHERE SOURCE IS NOT NULL ORDER BY 1")
sources = [r[0] for r in cur.fetchall()]

conn.close()
print(f"  Snowflake: {len(daily_detail):,} daily rows, {len(monthly_detail):,} monthly rows")

# ════════════════════════════════════════════════════════════════════════════
# META ADS — Corporate Studios account only (all studios since Apr 1 2026)
# ════════════════════════════════════════════════════════════════════════════
meta_daily   = []
meta_monthly = []

if META_TOKEN:
    print("Fetching Meta Ads data...")

    def meta_get(url, params):
        params["access_token"] = META_TOKEN
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    # ── Daily: last 90 days ────────────────────────────────────────────────
    today     = date.today()
    since     = (today - timedelta(days=90)).isoformat()
    until     = today.isoformat()

    params = {
        "fields":         "spend,impressions,clicks,actions,cost_per_action_type",
        "time_range":     json.dumps({"since": since, "until": until}),
        "time_increment": "1",
        "limit":          "90",
        "level":          "account",
    }

    data = meta_get(f"{META_API}/{META_ACT}/insights", params)
    rows = data.get("data", [])

    # paginate if needed
    while "paging" in data and "next" in data.get("paging", {}).get("cursors", {}):
        data = meta_get(data["paging"]["next"], {})
        rows += data.get("data", [])

    for row in rows:
        actions = row.get("actions", [])
        cpa     = row.get("cost_per_action_type", [])

        leads      = get_action(actions, "lead", "onsite_conversion.lead_grouped")
        calls      = get_action(actions, "phone_call", "click_to_call")
        directions = get_action(actions, "get_directions")

        spend      = float(row.get("spend", 0))
        impressions= int(row.get("impressions", 0))
        clicks     = int(row.get("clicks", 0))
        opportunities = leads + calls + directions

        # Cost per lead from cost_per_action_type
        cpl = next((float(a["value"]) for a in cpa if a["action_type"] == "lead"), 0)
        cpo = round(spend / opportunities, 2) if opportunities else 0

        meta_daily.append({
            "date":          row["date_start"],
            "spend":         round(spend, 2),
            "impressions":   impressions,
            "clicks":        clicks,
            "leads":         leads,
            "calls":         calls,
            "directions":    directions,
            "opportunities": opportunities,
            "cpl":           round(cpl, 2),
            "cpo":           cpo,
        })

    # ── Monthly: roll up daily into monthly ───────────────────────────────
    monthly_map = {}
    for row in meta_daily:
        m = row["date"][:7] + "-01"
        if m not in monthly_map:
            monthly_map[m] = {"month": m, "spend": 0, "impressions": 0,
                              "clicks": 0, "leads": 0, "calls": 0,
                              "directions": 0, "opportunities": 0}
        for k in ["spend","impressions","clicks","leads","calls","directions","opportunities"]:
            monthly_map[m][k] += row[k]

    for m in monthly_map.values():
        m["spend"]  = round(m["spend"], 2)
        m["cpl"]    = round(m["spend"] / m["leads"], 2)    if m["leads"]        else 0
        m["cpo"]    = round(m["spend"] / m["opportunities"], 2) if m["opportunities"] else 0
    meta_monthly = sorted(monthly_map.values(), key=lambda x: x["month"])

    print(f"  Meta: {len(meta_daily)} daily rows, {len(meta_monthly)} monthly rows")
    print(f"  Meta: total leads={sum(r['leads'] for r in meta_daily)}, "
          f"spend=${sum(r['spend'] for r in meta_daily):,.2f}")
else:
    print("  Meta: no token, skipping")

# ════════════════════════════════════════════════════════════════════════════
# WRITE OUTPUT
# ════════════════════════════════════════════════════════════════════════════
output = {
    "generated_at":   datetime.utcnow().isoformat() + "Z",
    # Snowflake leads data
    "studios":        studios,
    "sources":        sources,
    "daily_detail":   daily_detail,
    "monthly_detail": monthly_detail,
    # Meta Ads data
    "meta": {
        "account":       "SWEAT440 - Corporate Studios",
        "account_id":    META_ACT,
        "daily":         meta_daily,
        "monthly":       meta_monthly,
    }
}

with open("data.json", "w") as f:
    json.dump(output, f, indent=2, default=json_serial)

size_kb = os.path.getsize("data.json") / 1024
print(f"\n✅  data.json written — {size_kb:.1f} KB")
print(f"    Snowflake: {len(daily_detail):,} daily + {len(monthly_detail):,} monthly rows")
print(f"    Meta:      {len(meta_daily)} daily + {len(meta_monthly)} monthly rows")

# ════════════════════════════════════════════════════════════════════════════
# META ADS — Campaign/Studio breakdown for Daniel's Paid Ads Dashboard
# ════════════════════════════════════════════════════════════════════════════
# Studio code mapping: derive from ad account name
STUDIO_MAP = {
    "SWEAT440 - Corporate Studios": {"code": "CORP", "state": "FL"},
    "SWEAT440 - Chelsea":           {"code": "NYC-CH", "state": "NY"},
    "SWEAT440 - South Miami":       {"code": "S-MIA", "state": "FL"},
    "SWEAT440 - West Palm Beach":   {"code": "WPB", "state": "FL"},
    "SWEAT440 - Toms River":        {"code": "TOM-R", "state": "NJ"},
    "SWEAT440 - Park Slope":        {"code": "PK-SL", "state": "NY"},
    "SWEAT440 - Pinecrest":         {"code": "PINE", "state": "FL"},
    "SWEAT440 - Midtown":           {"code": "NYC-MT", "state": "NY"},
    "SWEAT440 - Austin":            {"code": "AUS", "state": "TX"},
    "SWEAT440 - NODA":              {"code": "NODA", "state": "NC"},
    "SWEAT440 - Ocean Township":    {"code": "OCN", "state": "NJ"},
    "SWEAT440 - Wall Township":     {"code": "WALL", "state": "NJ"},
    "SWEAT440 - Miami Lakes (US)":  {"code": "MIA-L", "state": "FL"},
    "SWEAT440 - Miramar":           {"code": "MIR", "state": "FL"},
    "SWEAT440 Pembroke Pines":      {"code": "PEM", "state": "FL"},
}

# Audience and pillar keywords to tag campaigns
AUDIENCE_TAGS = {
    "DINKS": ["dinks", "dual income", "couple", "couples"],
    "POC":   ["poc", "diversity", "multicultural", "urban"],
    "PYC":   ["youth", "young", "college", "student", "pyc"],
    "SI":    ["sport", "athlete", "active", "fitness", "si "],
    "SAHP":  ["parent", "mom", "dad", "family", "sahp"],
}
PILLAR_TAGS = {
    "DR":    ["dr", "direct response", "lead gen"],
    "DF":    ["df", "dynamic", "flexible"],
    "P&R":   ["p&r", "promo", "promotion", "offer", "deal"],
    "PS":    ["ps", "personal", "story"],
    "SSB":   ["ssb", "social", "brand"],
    "HYROX": ["hyrox"],
}

def tag_campaign(name):
    """Return audience and pillar tags for a campaign name."""
    lower = name.lower()
    aud = next((k for k, kws in AUDIENCE_TAGS.items() if any(w in lower for w in kws)), None)
    pil = next((k for k, kws in PILLAR_TAGS.items() if any(w in lower for w in kws)), None)
    return aud, pil

meta_campaigns = []

if META_TOKEN:
    print("Fetching Meta campaign-level data...")

    ALL_ACCOUNTS = [
        {"id": "act_1553887681409034", "name": "SWEAT440 - Corporate Studios"},
        {"id": "act_749102506206346",  "name": "SWEAT440 - Chelsea"},
        {"id": "act_1795229744165777", "name": "SWEAT440 - South Miami"},
        {"id": "act_1266906073888479", "name": "SWEAT440 - West Palm Beach"},
        {"id": "act_722137459480350",  "name": "SWEAT440 - Toms River"},
        {"id": "act_3592335067660902", "name": "SWEAT440 - Park Slope"},
        {"id": "act_549718637200114",  "name": "SWEAT440 - Pinecrest"},
        {"id": "act_525795066285979",  "name": "SWEAT440 - Midtown"},
        {"id": "act_1307376339906650", "name": "SWEAT440 - Austin"},
        {"id": "act_767334518751566",  "name": "SWEAT440 - NODA"},
        {"id": "act_1459694534903632", "name": "SWEAT440 - Ocean Township"},
        {"id": "act_1792866574556378", "name": "SWEAT440 - Wall Township"},
        {"id": "act_1636907403815262", "name": "SWEAT440 - Miami Lakes (US)"},
        {"id": "act_707802134775869",  "name": "SWEAT440 - Miramar"},
        {"id": "act_468747239241253",  "name": "SWEAT440 Pembroke Pines"},
    ]

    # Last 30 days
    until = date.today().isoformat()
    since = (date.today() - timedelta(days=30)).isoformat()

    studios_meta = []
    all_campaigns = []

    for acct in ALL_ACCOUNTS:
        acct_id   = acct["id"]
        acct_name = acct["name"]
        info      = STUDIO_MAP.get(acct_name, {"code": acct_name[:6].upper(), "state": "???"})

        try:
            # Account-level totals
            r = requests.get(f"{META_API}/{acct_id}/insights", params={
                "access_token": META_TOKEN,
                "fields": "spend,impressions,reach,clicks,actions",
                "time_range": json.dumps({"since": since, "until": until}),
                "limit": 1,
            }, timeout=20)
            acct_data = r.json().get("data", [{}])
            row = acct_data[0] if acct_data else {}
            actions = row.get("actions", [])

            spend       = float(row.get("spend", 0))
            impressions = int(row.get("impressions", 0))
            reach       = int(row.get("reach", 0))
            clicks      = int(row.get("clicks", 0))
            leads       = get_action(actions, "lead", "onsite_conversion.lead_grouped")
            purchases   = get_action(actions, "omni_purchase", "purchase")
            trials      = get_action(actions, "start_trial", "omni_activate_app")
            ctr         = round(clicks / impressions * 100, 2) if impressions else 0
            cpm         = round(spend / impressions * 1000, 2) if impressions else 0
            cpl         = round(spend / leads, 2) if leads else 0

            studio_obj = {
                "code":        info["code"],
                "name":        acct_name.replace("SWEAT440 - ", "").replace("SWEAT440 ", ""),
                "state":       info["state"],
                "spend":       round(spend, 2),
                "impressions": impressions,
                "reach":       reach,
                "clicks":      clicks,
                "leads":       leads,
                "purchases":   purchases,
                "trials":      trials,
                "ctr":         ctr,
                "cpm":         cpm,
                "cpl":         cpl,
                "aud":         {},   # audience breakdown below
                "pillars":     [],
            }

            # Campaign-level breakdown for audience/pillar tagging
            r2 = requests.get(f"{META_API}/{acct_id}/campaigns", params={
                "access_token": META_TOKEN,
                "fields": "id,name,status",
                "limit": 50,
            }, timeout=20)
            campaigns = r2.json().get("data", [])

            for camp in campaigns:
                if camp.get("status") not in ("ACTIVE", "PAUSED"):
                    continue
                # Get insights for this campaign
                r3 = requests.get(f"{META_API}/{camp['id']}/insights", params={
                    "access_token": META_TOKEN,
                    "fields": "spend,impressions,clicks,actions",
                    "time_range": json.dumps({"since": since, "until": until}),
                    "limit": 1,
                }, timeout=20)
                cdata = r3.json().get("data", [{}])
                crow  = cdata[0] if cdata else {}
                cactions = crow.get("actions", [])

                c_spend  = float(crow.get("spend", 0))
                c_impr   = int(crow.get("impressions", 0))
                c_leads  = get_action(cactions, "lead", "onsite_conversion.lead_grouped")
                c_cpl    = round(c_spend / c_leads, 2) if c_leads else 0

                aud, pil = tag_campaign(camp["name"])

                all_campaigns.append({
                    "campaign":    camp["name"],
                    "studio_code": info["code"],
                    "studio_name": studio_obj["name"],
                    "spend":       round(c_spend, 2),
                    "impressions": c_impr,
                    "leads":       c_leads,
                    "cpl":         c_cpl,
                    "audience":    aud,
                    "pillar":      pil,
                    "status":      camp["status"],
                })

                # Accumulate audience spend/leads on studio
                if aud:
                    if aud not in studio_obj["aud"]:
                        studio_obj["aud"][aud] = {"spend": 0, "leads": 0, "impr": 0}
                    studio_obj["aud"][aud]["spend"] += c_spend
                    studio_obj["aud"][aud]["leads"] += c_leads
                    studio_obj["aud"][aud]["impr"]  += c_impr

            studios_meta.append(studio_obj)
            print(f"  {info['code']}: ${spend:.0f} spend, {leads} leads")

        except Exception as e:
            print(f"  ⚠ {acct_name}: {e}")
            continue

    # ── Aggregate pillars across all campaigns ────────────────────────────
    pillar_map = {}
    for c in all_campaigns:
        p = c.get("pillar")
        if not p:
            continue
        if p not in pillar_map:
            pillar_map[p] = {"pillar": p, "spend": 0, "impressions": 0, "leads": 0}
        pillar_map[p]["spend"]       += c["spend"]
        pillar_map[p]["impressions"] += c["impressions"]
        pillar_map[p]["leads"]       += c["leads"]

    for p in pillar_map.values():
        p["cpl"] = round(p["spend"] / p["leads"], 2) if p["leads"] else 0

    # ── Aggregate concepts (campaign name prefix before " — ") ───────────
    concept_map = {}
    for c in all_campaigns:
        concept = c["campaign"].split(" — ")[0].split(" - ")[0].strip()
        if concept not in concept_map:
            concept_map[concept] = {"concept": concept, "spend": 0, "impressions": 0, "leads": 0, "ads": []}
        concept_map[concept]["spend"]       += c["spend"]
        concept_map[concept]["impressions"] += c["impressions"]
        concept_map[concept]["leads"]       += c["leads"]
        concept_map[concept]["ads"].append(c["campaign"])

    for cv in concept_map.values():
        cv["cpl"] = round(cv["spend"] / cv["leads"], 2) if cv["leads"] else 0

    # ── Totals ────────────────────────────────────────────────────────────
    def sum_field(arr, field):
        return sum(s.get(field, 0) for s in arr)

    total_spend = sum_field(studios_meta, "spend")
    total_leads = sum_field(studios_meta, "leads")

    meta_campaigns = {
        "period":   f"{since} to {until}",
        "totals": {
            "spend":       round(total_spend, 2),
            "impressions": sum_field(studios_meta, "impressions"),
            "reach":       sum_field(studios_meta, "reach"),
            "clicks":      sum_field(studios_meta, "clicks"),
            "leads":       total_leads,
            "purchases":   sum_field(studios_meta, "purchases"),
            "trials":      sum_field(studios_meta, "trials"),
            "cpl":         round(total_spend / total_leads, 2) if total_leads else 0,
            "ctr":         0,
            "cpm":         0,
        },
        "studios":   studios_meta,
        "pillars":   sorted(pillar_map.values(), key=lambda x: -x["spend"]),
        "concepts":  sorted(concept_map.values(), key=lambda x: -x["leads"]),
        "campaigns": all_campaigns,
    }

    print(f"  Meta campaigns: {len(studios_meta)} studios, {len(all_campaigns)} campaigns")

# ── Merge into output ──────────────────────────────────────────────────────
output["meta_ads"] = meta_campaigns
