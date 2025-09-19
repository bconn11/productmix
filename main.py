import os
import json
import sqlite3
import datetime
from typing import Optional, Dict, Any, List, Set
from pathlib import Path
from urllib.parse import urlencode, quote

import httpx
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv, dotenv_values
from zoneinfo import ZoneInfo

# -----------------------------------------------------------------------------
# Env
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.resolve()
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

def env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if not v:
        vals = dotenv_values(str(ENV_PATH)) if ENV_PATH.exists() else {}
        v = vals.get(name, default)
    return (v or "").strip()

APP_URL = env("APP_URL", "http://127.0.0.1:8000").rstrip("/")
SHOPIFY_API_KEY = env("SHOPIFY_API_KEY", "")
SHOPIFY_API_SECRET = env("SHOPIFY_API_SECRET", "")
SCOPES = env("SCOPES", "read_orders,read_products,read_customers")
API_VER = "2025-01"
DB_PATH = env("DB_PATH", "data.sqlite")

# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)
if (BASE_DIR / "static").is_dir():
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates")) if (BASE_DIR / "templates").is_dir() else None

# -----------------------------------------------------------------------------
# DB
# -----------------------------------------------------------------------------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS shops (
          shop TEXT PRIMARY KEY,
          token TEXT,
          scopes TEXT,
          tz TEXT,
          created_at TEXT
        )
        """)
        # Ensure tz column exists (older DBs may not have it)
        rows = conn.execute("PRAGMA table_info(shops)").fetchall()
        colnames = [row[1] for row in rows]  # PRAGMA returns tuples; name is index 1
        if "tz" not in colnames:
            conn.execute("ALTER TABLE shops ADD COLUMN tz TEXT")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS product_types (
          shop TEXT NOT NULL,
          product_id INTEGER NOT NULL,
          product_type TEXT,
          updated_at TEXT,
          PRIMARY KEY (shop, product_id)
        )""")
        conn.commit()
init_db()

def save_token(shop: str, token: str, scopes: str, tz: Optional[str] = None):
    with db() as conn:
        conn.execute("""
          INSERT INTO shops (shop, token, scopes, tz, created_at)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(shop) DO UPDATE SET
            token=excluded.token, scopes=excluded.scopes,
            tz=COALESCE(excluded.tz, shops.tz)
        """, (shop, token, scopes, tz, datetime.datetime.utcnow().isoformat()))
        conn.commit()

def get_token(shop: str) -> Optional[str]:
    with db() as conn:
        r = conn.execute("SELECT token FROM shops WHERE shop=?", (shop,)).fetchone()
        return r["token"] if r else None

def get_shop_tz(shop: str) -> Optional[str]:
    with db() as conn:
        r = conn.execute("SELECT tz FROM shops WHERE shop=?", (shop,)).fetchone()
        return (r["tz"] or None) if r else None

def set_shop_tz(shop: str, tz: str):
    with db() as conn:
        conn.execute("UPDATE shops SET tz=? WHERE shop=?", (tz, shop))
        conn.commit()

def get_product_type_cache(shop: str, product_ids: List[int]) -> Dict[int, Optional[str]]:
    if not product_ids:
        return {}
    placeholders = ",".join("?" for _ in product_ids)
    with db() as conn:
        rows = conn.execute(f"""
            SELECT product_id, product_type FROM product_types
            WHERE shop=? AND product_id IN ({placeholders})
        """, [shop] + product_ids).fetchall()
    out: Dict[int, Optional[str]] = {pid: None for pid in product_ids}
    for row in rows:
        out[int(row["product_id"])] = row["product_type"]
    return out

def set_product_types(shop: str, mapping: Dict[int, Optional[str]]):
    if not mapping:
        return
    now = datetime.datetime.utcnow().isoformat()
    with db() as conn:
        for pid, ptype in mapping.items():
            conn.execute("""
                INSERT INTO product_types (shop, product_id, product_type, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(shop, product_id) DO UPDATE SET
                  product_type=excluded.product_type,
                  updated_at=excluded.updated_at
            """, (shop, int(pid), ptype, now))
        conn.commit()

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
HTTP_TIMEOUT = httpx.Timeout(connect=20.0, read=90.0, write=60.0, pool=60.0)

async def get_with_retries(client: httpx.AsyncClient, url: str, **kwargs) -> httpx.Response:
    for attempt in range(4):
        try:
            r = await client.get(url, **kwargs)
            return r
        except Exception:
            if attempt == 3:
                raise

async def shopify_rest(shop: str, token: str, path: str, params: Dict[str, Any] = None) -> httpx.Response:
    url = f"https://{shop}/admin/api/{API_VER}/{path}"
    headers = {"X-Shopify-Access-Token": token, "Accept": "application/json"}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        return await get_with_retries(client, url, headers=headers, params=params)

def parse_shopify_ts(ts: str) -> datetime.datetime:
    if ts.endswith("Z"):
        ts = ts.replace("Z", "+00:00")
    return datetime.datetime.fromisoformat(ts)

def to_store_day(ts: str, tz_name: str) -> str:
    aware = parse_shopify_ts(ts)
    loc = aware.astimezone(ZoneInfo(tz_name))
    return loc.date().isoformat()

def local_bounds_to_utc(start: datetime.date, end: datetime.date, tz_name: str) -> (str, str):
    tz = ZoneInfo(tz_name)
    start_local = datetime.datetime.combine(start, datetime.time(0, 0, 0), tzinfo=tz)
    end_local   = datetime.datetime.combine(end,   datetime.time(23, 59, 59), tzinfo=tz)
    return start_local.astimezone(datetime.timezone.utc).isoformat(), end_local.astimezone(datetime.timezone.utc).isoformat()

def parse_date_yyyymmdd(s: Optional[str]) -> Optional[datetime.date]:
    if not s:
        return None
    try:
        d = datetime.date.fromisoformat(s)
        return d if d.year >= 1900 else None
    except Exception:
        return None

# -----------------------------------------------------------------------------
# Routes: diag / oauth / dashboard
# -----------------------------------------------------------------------------
@app.get("/", response_class=PlainTextResponse)
async def root():
    return "OK"

@app.get("/diag", response_class=JSONResponse)
async def diag():
    return {
        "APP_URL": APP_URL,
        "SHOPIFY_API_KEY_present": bool(SHOPIFY_API_KEY),
        "computed_redirect_uri": f"{APP_URL}/auth/callback",
        "DB_PATH": DB_PATH,
        "API_VER": API_VER,
    }

@app.get("/diag/state", response_class=JSONResponse)
async def diag_state(shop: str):
    tok = get_token(shop)
    return {"shop": shop, "has_token": bool(tok), "token_tail": tok[-6:] if tok else None, "tz": get_shop_tz(shop)}

@app.get("/auth")
async def auth(shop: str):
    if not SHOPIFY_API_KEY or not SHOPIFY_API_SECRET or not APP_URL:
        return JSONResponse(status_code=500, content={"error":"missing SHOPIFY_API_KEY or APP_URL"})
    params = {
        "client_id": SHOPIFY_API_KEY,
        "scope": SCOPES,
        "redirect_uri": f"{APP_URL}/auth/callback",
        "state": "nonce",
    }
    return RedirectResponse(f"https://{shop}/admin/oauth/authorize?{urlencode(params)}", status_code=307)

@app.get("/auth/callback")
async def auth_cb(shop: str, code: str):
    token_url = f"https://{shop}/admin/oauth/access_token"
    payload = {"client_id": SHOPIFY_API_KEY, "client_secret": SHOPIFY_API_SECRET, "code": code}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(token_url, json=payload)
        if r.status_code != 200:
            return JSONResponse(status_code=r.status_code, content={"error":"oauth_exchange_failed","body":r.text})
        data = r.json()
        access_token = data.get("access_token")
        scope = data.get("scope", "")
        if not access_token:
            return JSONResponse(status_code=500, content={"error":"no_access_token_from_shopify","body":data})

        # fetch store timezone and cache it
        tz = None
        try:
            sh = await get_with_retries(client,
                    f"https://{shop}/admin/api/{API_VER}/shop.json",
                    headers={"X-Shopify-Access-Token": access_token, "Accept":"application/json"},
                    )
            if sh.status_code == 200:
                tz = (sh.json().get("shop", {}) or {}).get("iana_timezone")
        except Exception:
            tz = None

        save_token(shop, access_token, scope, tz)

    return RedirectResponse(f"/dashboard?shop={quote(shop)}", status_code=302)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, shop: Optional[str] = None):
    if not templates:
        return HTMLResponse("<h2>Templates directory missing.</h2>", status_code=500)
    return templates.TemplateResponse("dashboard.html", {"request": request, "shop": shop or ""})

# -----------------------------------------------------------------------------
# Product type backfill
# -----------------------------------------------------------------------------
async def backfill_product_types(shop: str, token: str, missing_ids: List[int]) -> Dict[int, Optional[str]]:
    if not missing_ids:
        return {}
    mapping: Dict[int, Optional[str]] = {}
    BATCH = 100
    headers = {"X-Shopify-Access-Token": token, "Accept": "application/json"}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        for i in range(0, len(missing_ids), BATCH):
            ids_param = ",".join(str(x) for x in missing_ids[i:i+BATCH])
            url = f"https://{shop}/admin/api/{API_VER}/products.json"
            r = await get_with_retries(client, url, headers=headers, params={"ids": ids_param, "fields": "id,product_type"})
            if r.status_code != 200:
                continue
            for p in r.json().get("products", []):
                pid = int(p.get("id"))
                ptype = (p.get("product_type") or "").strip() or None
                mapping[pid] = ptype
    set_product_types(shop, mapping)
    return mapping

# -----------------------------------------------------------------------------
# Orders fetch + aggregate (timezone-aware)
# -----------------------------------------------------------------------------
async def fetch_orders_all(shop: str, token: str, start_local: datetime.date, end_local: datetime.date, tz_name: str) -> List[Dict[str, Any]]:
    created_min_utc, created_max_utc = local_bounds_to_utc(start_local, end_local, tz_name)

    params = {
        "status": "any",
        "limit": 250,
        "created_at_min": created_min_utc,
        "created_at_max": created_max_utc,
        "fields": "id,created_at,total_price,customer,line_items,currency"
    }
    base = f"https://{shop}/admin/api/{API_VER}/orders.json"
    headers = {"X-Shopify-Access-Token": token, "Accept": "application/json"}

    results: List[Dict[str, Any]] = []
    next_url: Optional[str] = None

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        while True:
            if next_url:
                r = await get_with_retries(client, next_url, headers=headers)
            else:
                r = await get_with_retries(client, base, headers=headers, params=params)

            if r.status_code != 200:
                raise HTTPException(status_code=r.status_code, detail=f"shopify_orders_error: {r.text}")

            data = r.json()
            results.extend(data.get("orders", []))

            link = r.headers.get("Link") or r.headers.get("link")
            if not link or 'rel="next"' not in link:
                break
            try:
                segs = [s.strip() for s in link.split(",")]
                nxt = [s for s in segs if 'rel="next"' in s]
                if not nxt: break
                url_part = nxt[0].split(";")[0].strip()
                if url_part.startswith("<") and url_part.endswith(">"):
                    url_part = url_part[1:-1]
                next_url = url_part
            except Exception:
                break

    return results

async def fetch_sales_daily(shop: str, start_date: datetime.date, end_date: datetime.date, group_by: str, metric: str) -> Dict[str, Any]:
    token = get_token(shop)
    if not token:
        raise HTTPException(status_code=400, detail="no_token_for_shop")

    tz_name = get_shop_tz(shop) or "UTC"
    orders = await fetch_orders_all(shop, token, start_date, end_date, tz_name)

    # Collect product_ids for product_type mapping
    product_ids: Set[int] = set()
    currency: Optional[str] = None
    for o in orders:
        if not currency:
            currency = o.get("currency") or currency
        for li in o.get("line_items", []):
            pid = li.get("product_id")
            if isinstance(pid, int):
                product_ids.add(pid)

    cached = get_product_type_cache(shop, list(product_ids))
    missing = [pid for pid, ptype in cached.items() if ptype is None]
    if missing:
        fetched = await backfill_product_types(shop, token, missing)
        cached.update(fetched)

    # Aggregate by LOCAL (store) calendar day
    units_by_day: Dict[str, Dict[str, float]] = {}
    sales_by_day: Dict[str, Dict[str, float]] = {}
    orders_by_day: Dict[str, int] = {}

    for o in orders:
        d = to_store_day(o.get("created_at", ""), tz_name)
        orders_by_day[d] = orders_by_day.get(d, 0) + 1

        for li in o.get("line_items", []):
            qty = float(li.get("quantity") or 0)
            try:
                price = float(str(li.get("price") or "0").replace(",", ""))
            except Exception:
                price = 0.0
            revenue = qty * price

            if group_by == "product_type":
                pid = li.get("product_id")
                ptype = cached.get(pid) if isinstance(pid, int) else None
                key = (ptype or "unknown").strip() or "unknown"
            elif group_by == "product":
                key = (li.get("title") or "").strip() or "unknown"
            else:  # sku
                key = (li.get("sku") or "").strip() or "unknown"

            units_by_day.setdefault(d, {}).setdefault(key, 0.0)
            units_by_day[d][key] += qty

            sales_by_day.setdefault(d, {}).setdefault(key, 0.0)
            sales_by_day[d][key] += revenue

    rows: List[Dict[str, Any]] = []
    all_days = sorted(set(units_by_day.keys()) | set(sales_by_day.keys()) | set(orders_by_day.keys()))
    for d in all_days:
        u = units_by_day.get(d, {})
        s = sales_by_day.get(d, {})
        row: Dict[str, Any] = {"date": d}

        keys = set(u.keys()) | set(s.keys())
        for k in keys:
            row[k] = round(s[k], 2) if metric == "sales" else float(u[k])

        units_total = sum(u.values())
        sales_total = sum(s.values())
        row["total"] = round(sales_total if metric == "sales" else units_total, 2)
        row["units_total"] = round(units_total, 2)
        row["sales_total"] = round(sales_total, 2)
        row["orders_total"] = int(orders_by_day.get(d, 0))
        rows.append(row)

    return {"rows": rows, "currency": currency, "tz": tz_name}

# -----------------------------------------------------------------------------
# API: sales
# -----------------------------------------------------------------------------
@app.get("/api/sales", response_class=JSONResponse)
async def api_sales(
    shop: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    group_by: str = Query("product_type"),
    metric: str = Query("units")
):
    # defaults = last 14 whole local days
    today = datetime.date.today()
    s = parse_date_yyyymmdd(start_date) or (today - datetime.timedelta(days=13))
    e = parse_date_yyyymmdd(end_date)   or today

    if s > e:
        return JSONResponse(status_code=400, content={"error":"bad_dates","message":"start_date must be <= end_date (YYYY-MM-DD)"})

    if group_by not in ("product_type", "product", "sku"):
        group_by = "product_type"
    if metric not in ("units", "sales"):
        metric = "units"

    try:
        payload = await fetch_sales_daily(shop, s, e, group_by, metric)
        payload.update({"start_date": s.isoformat(), "end_date": e.isoformat(), "group_by": group_by, "metric": metric})
        return payload
    except HTTPException as he:
        return JSONResponse(status_code=he.status_code, content={"error":"shopify_orders_error","body":str(he.detail)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error":"api_sales","message":str(e)})

# -----------------------------------------------------------------------------
# Backfill product types (optional manual endpoint)
# -----------------------------------------------------------------------------
@app.get("/api/backfill_product_types", response_class=JSONResponse)
async def api_backfill(shop: str):
    token = get_token(shop)
    if not token:
        return JSONResponse(status_code=400, content={"error":"no_token"})
    headers = {"X-Shopify-Access-Token": token, "Accept":"application/json"}
    url = f"https://{shop}/admin/api/{API_VER}/products.json?limit=250&fields=id,product_type"
    mapping = {}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        next_url = url
        while next_url:
            r = await client.get(next_url, headers=headers)
            if r.status_code != 200:
                return JSONResponse(status_code=r.status_code, content={"error":"shopify_products_error","body":r.text})
            for p in r.json().get("products", []):
                pid = int(p["id"])
                mapping[pid] = (p.get("product_type") or "").strip() or None
            link = r.headers.get("Link")
            next_url = None
            if link and 'rel="next"' in link:
                seg = [s for s in link.split(",") if 'rel="next"' in s]
                if seg:
                    next_url = seg[0].split(";")[0].strip()[1:-1]
    set_product_types(shop, mapping)
    return {"count": len(mapping)}

# ---------------------- Shopify GDPR mandatory endpoints ----------------------
@app.post("/webhooks/customers/data_request")
async def gdpr_data_request(request: Request):
    # You must delete or export customer data if you store any PII.
    # We only store shop tokens, no customer PII. Acknowledge receipt.
    return JSONResponse({"status": "ok"})

@app.post("/webhooks/customers/redact")
async def gdpr_customers_redact(request: Request):
    return JSONResponse({"status": "ok"})

@app.post("/webhooks/shop/redact")
async def gdpr_shop_redact(request: Request):
    # If a shop uninstalls and requests redaction, purge its token.
    try:
        payload = await request.json()
        shop = (payload or {}).get("shop_domain")
        if shop:
            with db() as conn:
                conn.execute("DELETE FROM shops WHERE shop=?", (shop,))
                conn.commit()
    except Exception:
        pass
    return JSONResponse({"status": "ok"})
