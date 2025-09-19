import os
import hmac
import base64
import hashlib
import sqlite3
from urllib.parse import urlencode, quote, urlparse, parse_qs

from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple

import httpx
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse, PlainTextResponse, HTMLResponse

# -----------------------------
# Config helpers
# -----------------------------
def env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

APP_URL = env("APP_URL", "http://127.0.0.1:8000")
SHOPIFY_API_KEY = env("SHOPIFY_API_KEY")
SHOPIFY_API_SECRET = env("SHOPIFY_API_SECRET")
SCOPES = env("SCOPES", "read_orders,read_products,read_customers")
DB_PATH = env("DB_PATH", "/tmp/data.sqlite")
API_VER = "2025-01"

# -----------------------------
# App + DB
# -----------------------------
app = FastAPI(title="ProductMix")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shops (
            shop TEXT PRIMARY KEY,
            token TEXT,
            scopes TEXT,
            installed_at TEXT
        )
    """)
    return conn

def save_token(shop: str, token: str, scopes: str) -> None:
    conn = db()
    conn.execute(
        "INSERT INTO shops(shop, token, scopes, installed_at) VALUES(?,?,?,datetime('now')) "
        "ON CONFLICT(shop) DO UPDATE SET token=excluded.token, scopes=excluded.scopes",
        (shop, token, scopes),
    )
    conn.commit()
    conn.close()

def get_token(shop: str) -> Optional[str]:
    conn = db()
    cur = conn.execute("SELECT token FROM shops WHERE shop=?", (shop,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def get_scopes(shop: str) -> Optional[str]:
    conn = db()
    cur = conn.execute("SELECT scopes FROM shops WHERE shop=?", (shop,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

# -----------------------------
# Shopify OAuth helpers
# -----------------------------
def build_auth_url(shop: str) -> str:
    params = {
        "client_id": SHOPIFY_API_KEY,
        "scope": SCOPES,
        "redirect_uri": f"{APP_URL}/auth/callback",
        "state": "nonce",
    }
    return f"https://{shop}/admin/oauth/authorize?{urlencode(params)}"

def verify_hmac(params: Dict[str, str]) -> bool:
    """Verify HMAC from Shopify callback."""
    if "hmac" not in params:
        return False
    h = params.pop("hmac")
    # Shopify signs the sorted query string (excluding hmac/signature)
    message = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
    digest = hmac.new(
        SHOPIFY_API_SECRET.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(digest, h)

# -----------------------------
# Basic pages / diagnostics
# -----------------------------
@app.get("/", response_class=PlainTextResponse)
def root():
    return "OK"

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/diag")
def diag():
    return {
        "APP_URL": APP_URL,
        "DB_PATH": DB_PATH,
        "API_VER": API_VER,
        "SHOPIFY_API_KEY_present": bool(SHOPIFY_API_KEY),
    }

@app.get("/diag/state")
def diag_state(shop: str = Query(...)):
    tok = get_token(shop)
    scopes = get_scopes(shop)
    return {
        "shop": shop,
        "has_token": bool(tok),
        "token_tail": (tok[-6:] if tok else None),
        "scopes": scopes,
    }

# -----------------------------
# OAuth endpoints
# -----------------------------
@app.get("/auth")
def auth(shop: str = Query(...)):
    if not SHOPIFY_API_KEY or not SHOPIFY_API_SECRET or not APP_URL:
        raise HTTPException(status_code=500, detail="Missing SHOPIFY_API_KEY or SHOPIFY_API_SECRET or APP_URL")
    return RedirectResponse(build_auth_url(shop))

@app.get("/auth/callback")
def auth_callback(request: Request):
    # Extract params into a dict[str,str]
    q = dict(request.query_params)
    shop = q.get("shop")
    code = q.get("code")
    # Make a copy for HMAC verification (don't mutate original dict)
    verify_map = {k: v for k, v in q.items() if k != "signature"}
    if not verify_hmac(verify_map):
        raise HTTPException(status_code=400, detail="hmac_verification_failed")

    # Exchange code for token
    if not shop or not code:
        raise HTTPException(status_code=400, detail="missing shop/code")

    token_url = f"https://{shop}/admin/oauth/access_token"
    payload = {
        "client_id": SHOPIFY_API_KEY,
        "client_secret": SHOPIFY_API_SECRET,
        "code": code,
    }
    try:
        r = httpx.post(token_url, json=payload, timeout=20.0)
        r.raise_for_status()
        data = r.json()
        access_token = data.get("access_token")
        scopes = data.get("scope", "")
        if not access_token:
            raise HTTPException(status_code=400, detail="no_access_token_in_response")
        save_token(shop, access_token, scopes)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=400, detail=f"token_exchange_failed: {e}")

    # Send to a simple dashboard page
    return RedirectResponse(f"/dashboard?shop={shop}")

# -----------------------------
# Simple dashboard UI (placeholder)
# -----------------------------
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(shop: str = Query(...)):
    html = f"""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8"/>
      <title>ProductMix Dashboard</title>
      <style>
        body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; padding: 24px; }}
        .row {{ margin-bottom: 12px; }}
        pre {{ background:#f7f7f7; padding:12px; border-radius:6px; overflow:auto; }}
      </style>
    </head>
    <body>
      <h1>ProductMix</h1>
      <div class="row">Shop: <b>{shop}</b></div>
      <div class="row">
        <label>Days:
          <input id="days" type="number" min="1" max="60" value="7"/>
        </label>
        <button id="btn">Load Sales</button>
      </div>
      <pre id="out">Click "Load Sales"…</pre>

      <script>
        async function go() {{
          const days = document.getElementById('days').value || 7;
          const res = await fetch(`/api/sales?shop={shop}&days=${{days}}`);
          const txt = await res.text();
          document.getElementById('out').textContent = txt;
        }}
        document.getElementById('btn').addEventListener('click', go);
      </script>
    </body>
    </html>
    """
    return html

# -----------------------------
# Sales API (minimal)
# -----------------------------
def _parse_dt(s: str) -> datetime:
    # Shopify returns ISO8601; we normalize to UTC
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc)

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _day_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def _daterange_from_params(
    start: Optional[str],
    end: Optional[str],
    days: Optional[int]
) -> Tuple[datetime, datetime]:
    if start and end:
        # Interpret as inclusive days in local → convert to UTC start/end
        start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    else:
        d = int(days or 7)
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=d)
    # Clamp to whole-day bounds in UTC
    start_floor = datetime(start_dt.year, start_dt.month, start_dt.day, tzinfo=timezone.utc)
    end_ceil = datetime(end_dt.year, end_dt.month, end_dt.day, 23, 59, 59, tzinfo=timezone.utc)
    return start_floor, end_ceil

def _shopify_headers(token: str) -> Dict[str, str]:
    return {
        "X-Shopify-Access-Token": token,
        "Accept": "application/json",
    }

def _orders_url(shop: str) -> str:
    return f"https://{shop}/admin/api/{API_VER}/orders.json"

@app.get("/api/sales")
def api_sales(
    shop: str = Query(...),
    start: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD"),
    days: Optional[int] = Query(7, ge=1, le=60),
    status: str = Query("any"),
    limit: int = Query(250, ge=1, le=250),
):
    token = get_token(shop)
    if not token:
        return JSONResponse({"error":"no_token_for_shop"}, status_code=401)

    start_dt, end_dt = _daterange_from_params(start, end, days)
    params = {
        "status": status,
        "limit": str(limit),
        "created_at_min": _iso(start_dt),
        "created_at_max": _iso(end_dt),
        "fields": "id,created_at,total_price,currency,order_number,line_items",
    }

    client = httpx.Client(timeout=30.0)
    url = _orders_url(shop)
    all_orders: List[Dict[str, Any]] = []

    try:
        # Cursor pagination using page_info via Link header
        next_url = url
        next_params = params.copy()
        for _ in range(20):  # hard cap to avoid runaway loops
            r = client.get(next_url, headers=_shopify_headers(token), params=next_params)
            r.raise_for_status()
            data = r.json().get("orders", [])
            all_orders.extend(data)

            # Parse Link header
            link = r.headers.get("Link", "")
            # Example: <https://shop/admin/api/2025-01/orders.json?page_info=xxx&limit=250>; rel="next"
            if 'rel="next"' in link and "page_info=" in link:
                # Extract next page_info
                try:
                    # simple parse
                    seg = link.split(";")[0].strip().strip("<>")
                    parsed = urlparse(seg)
                    q = parse_qs(parsed.query)
                    page_info = q.get("page_info", [None])[0]
                    if not page_info:
                        break
                    next_url = f"https://{shop}{parsed.path}"
                    next_params = {
                        "limit": str(limit),
                        "status": status,
                        "created_at_min": _iso(start_dt),
                        "created_at_max": _iso(end_dt),
                        "fields": "id,created_at,total_price,currency,order_number,line_items",
                        "page_info": page_info,
                    }
                except Exception:
                    break
            else:
                break
    except httpx.HTTPStatusError as e:
        return JSONResponse({"error":"shopify_orders_error","status":e.response.status_code,"body":e.response.text}, status_code=502)
    except httpx.HTTPError as e:
        return JSONResponse({"error":"shopify_http_error","detail":str(e)}, status_code=502)
    finally:
        client.close()

    # Aggregate sales by day (UTC)
    by_day: Dict[str, Dict[str, Any]] = {}
    for o in all_orders:
        dt = _parse_dt(o["created_at"])
        k = _day_key(dt)
        total = float(o.get("total_price", "0") or 0)
        if k not in by_day:
            by_day[k] = {"date": k, "orders": 0, "sales": 0.0}
        by_day[k]["orders"] += 1
        by_day[k]["sales"] += total

    # Return sorted days
    rows = [by_day[k] for k in sorted(by_day.keys())]
    return {"shop": shop, "start": _iso(start_dt), "end": _iso(end_dt), "count_orders": len(all_orders), "rows": rows}

# -----------------------------
# END OF FILE
# -----------------------------
