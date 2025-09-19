import os
import hmac
import hashlib
import sqlite3
from urllib.parse import urlencode, urlparse, parse_qs
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple

import httpx
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse, PlainTextResponse, HTMLResponse

# ===== Env =====
def env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

APP_URL = env("APP_URL", "http://127.0.0.1:8000").rstrip("/")
SHOPIFY_API_KEY = env("SHOPIFY_API_KEY")
SHOPIFY_API_SECRET = env("SHOPIFY_API_SECRET")
SCOPES = env("SCOPES", "read_orders,read_products,read_customers")
DB_PATH = env("DB_PATH", "/tmp/data.sqlite")
API_VER = "2025-01"

# ===== App / DB =====
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

# ===== Helpers =====
def build_auth_url(shop: str) -> str:
    params = {
        "client_id": SHOPIFY_API_KEY,
        "scope": SCOPES,
        "redirect_uri": f"{APP_URL}/auth/callback",
        "state": "nonce",
    }
    return f"https://{shop}/admin/oauth/authorize?{urlencode(params)}"

def verify_hmac(params: Dict[str, str]) -> bool:
    p = dict(params)
    h = p.pop("hmac", None)
    if not h:
        return False
    message = "&".join([f"{k}={v}" for k, v in sorted(p.items())])
    digest = hmac.new(
        SHOPIFY_API_SECRET.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(digest, h)

def shopify_headers(token: str) -> Dict[str, str]:
    return {"X-Shopify-Access-Token": token, "Accept": "application/json"}

def orders_url(shop: str) -> str:
    return f"https://{shop}/admin/api/{API_VER}/orders.json"

# ===== Basic routes / diag =====
@app.get("/", response_class=PlainTextResponse)
def root_get():
    return "OK"

@app.head("/", response_class=PlainTextResponse)
def root_head():
    return PlainTextResponse("", status_code=200)

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
        "SHOPIFY_API_SECRET_present": bool(SHOPIFY_API_SECRET),
        "computed_redirect_uri": f"{APP_URL}/auth/callback",
    }

@app.get("/diag/state")
def diag_state(shop: str = Query(...)):
    tok = get_token(shop)
    scopes = get_scopes(shop)
    return {"shop": shop, "has_token": bool(tok), "token_tail": (tok[-6:] if tok else None), "scopes": scopes}

@app.get("/diag/shops")
def diag_shops():
    """Dump all rows from shops table to verify persistence."""
    conn = db()
    cur = conn.execute("SELECT shop, substr(token, length(token)-5, 6) as token_tail, scopes, installed_at FROM shops")
    rows = [dict(shop=r[0], token_tail=r[1], scopes=r[2], installed_at=r[3]) for r in cur.fetchall()]
    conn.close()
    return {"rows": rows}

@app.get("/diag/shopify")
def diag_shopify(shop: str = Query(...)):
    tok = get_token(shop)
    if not tok:
        return JSONResponse({"error": "no_token_for_shop"}, status_code=401)
    try:
        r = httpx.get(orders_url(shop), headers=shopify_headers(tok), params={"limit": 1}, timeout=20.0)
        return {"status": r.status_code, "ok": r.is_success, "link": r.headers.get("Link", ""), "sample": r.json().get("orders", [])[:1]}
    except httpx.HTTPError as e:
        return JSONResponse({"error": "shopify_probe_http", "detail": str(e)}, status_code=502)

# ===== OAuth UI helpers =====
@app.get("/install", response_class=HTMLResponse)
def install_page(shop: str = Query(...)):
    if not SHOPIFY_API_KEY or not SHOPIFY_API_SECRET:
        return HTMLResponse("<h1>Missing API key/secret</h1>", status_code=500)
    auth = build_auth_url(shop)
    html = f"""
    <!doctype html><meta charset="utf-8">
    <h2>Install ProductMix to <code>{shop}</code></h2>
    <p>Redirect URI we will send Shopify back to:</p>
    <pre>{APP_URL}/auth/callback</pre>
    <p><a href="{auth}" style="display:inline-block;padding:10px 14px;background:#0a5;color:#fff;border-radius:6px;text-decoration:none;">Install</a></p>
    """
    return html

@app.get("/auth")
def auth(shop: str = Query(...)):
    if not SHOPIFY_API_KEY or not SHOPIFY_API_SECRET or not APP_URL:
        raise HTTPException(status_code=500, detail="Missing SHOPIFY_API_KEY or SHOPIFY_API_SECRET or APP_URL")
    return RedirectResponse(build_auth_url(shop))

@app.get("/auth/callback", response_class=HTMLResponse)
def auth_callback(request: Request):
    q = dict(request.query_params)
    shop = q.get("shop")
    code = q.get("code")
    ok_hmac = verify_hmac(q)
    if not ok_hmac:
        return HTMLResponse(f"<h2>HMAC verification failed</h2><pre>{q}</pre>", status_code=400)
    if not shop or not code:
        return HTMLResponse("<h2>Missing shop or code</h2>", status_code=400)

    token_url = f"https://{shop}/admin/oauth/access_token"
    payload = {"client_id": SHOPIFY_API_KEY, "client_secret": SHOPIFY_API_SECRET, "code": code}

    try:
        r = httpx.post(token_url, json=payload, timeout=20.0)
        body = r.text
        r.raise_for_status()
        data = r.json()
        access_token = data.get("access_token")
        scopes = data.get("scope", "")
        if not access_token:
            return HTMLResponse(f"<h2>No access_token in response</h2><pre>{body}</pre>", status_code=400)
        save_token(shop, access_token, scopes)
    except httpx.HTTPError as e:
        return HTMLResponse(f"<h2>Token exchange failed</h2><pre>{str(e)}</pre>", status_code=400)

    # Show a clear success page instead of silent redirect
    html = f"""
    <!doctype html><meta charset="utf-8">
    <h2>Installed OK</h2>
    <p>Shop: <b>{shop}</b></p>
    <p>Scopes: <code>{scopes}</code></p>
    <p><a href="/diag/state?shop={shop}">View token state</a></p>
    <p><a href="/dashboard?shop={shop}">Go to Dashboard</a></p>
    """
    return HTMLResponse(html)

# ===== Debug dashboard =====
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(shop: str = Query(...)):
    html = f"""
    <!doctype html>
    <meta charset="utf-8"/>
    <title>ProductMix Dashboard</title>
    <style>
      body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; padding: 24px; }}
      .row {{ margin-bottom: 12px; }}
      pre {{ background:#f7f7f7; padding:12px; border-radius:6px; overflow:auto; }}
      button {{ padding:8px 12px; }}
      input, select {{ padding:6px; }}
    </style>
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
        const url = `/api/sales?shop={shop}&days=${{days}}`;
        const res = await fetch(url);
        const txt = await res.text();
        document.getElementById('out').textContent = txt;
      }}
      document.getElementById('btn').addEventListener('click', go);
    </script>
    """
    return HTMLResponse(html)

# ===== Sales API (cursor pagination correct) =====
def parse_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def day_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def daterange_from_params(start: Optional[str], end: Optional[str], days: Optional[int]) -> Tuple[datetime, datetime]:
    if start and end:
        start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    else:
        d = int(days or 7)
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=d)
    start_floor = datetime(start_dt.year, start_dt.month, start_dt.day, tzinfo=timezone.utc)
    end_ceil = datetime(end_dt.year, end_dt.month, end_dt.day, 23, 59, 59, tzinfo=timezone.utc)
    return start_floor, end_ceil

@app.get("/api/sales")
def api_sales(
    shop: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    days: Optional[int] = Query(7, ge=1, le=60),
    status: str = Query("any"),
    limit: int = Query(250, ge=1, le=250),
):
    token = get_token(shop)
    if not token:
        return JSONResponse({"error":"no_token_for_shop"}, status_code=401)

    start_dt, end_dt = daterange_from_params(start, end, days)

    base_params = {
        "status": status,
        "limit": str(limit),
        "created_at_min": iso(start_dt),
        "created_at_max": iso(end_dt),
        "fields": "id,created_at,total_price,currency,order_number,line_items",
    }

    client = httpx.Client(timeout=30.0)
    url = orders_url(shop)
    all_orders: List[Dict[str, Any]] = []

    try:
        # First page WITH filters
        r = client.get(url, headers=shopify_headers(token), params=base_params)
        r.raise_for_status()
        payload = r.json()
        all_orders.extend(payload.get("orders", []))

        # Next pages: ONLY page_info + limit (drop filters)
        link = r.headers.get("Link", "")
        for _ in range(100):
            if 'rel="next"' not in link or "page_info=" not in link:
                break
            seg = link.split(";")[0].strip().strip("<>")
            parsed = urlparse(seg)
            q = parse_qs(parsed.query)
            page_info = q.get("page_info", [None])[0]
            if not page_info:
                break
            next_url = f"https://{shop}{parsed.path}"
            next_params = {"limit": str(limit), "page_info": page_info}
            r = client.get(next_url, headers=shopify_headers(token), params=next_params)
            r.raise_for_status()
            payload = r.json()
            all_orders.extend(payload.get("orders", []))
            link = r.headers.get("Link", "")
    except httpx.HTTPStatusError as e:
        return JSONResponse({"error":"shopify_orders_error","status":e.response.status_code,"body":e.response.text}, status_code=502)
    except httpx.HTTPError as e:
        return JSONResponse({"error":"shopify_http_error","detail":str(e)}, status_code=502)
    finally:
        client.close()

    by_day: Dict[str, Dict[str, Any]] = {}
    for o in all_orders:
        try:
            dt = parse_dt(o["created_at"])
            k = day_key(dt)
            total = float(o.get("total_price", "0") or 0)
        except Exception:
            continue
        row = by_day.setdefault(k, {"date": k, "orders": 0, "sales": 0.0})
        row["orders"] += 1
        row["sales"] += total

    rows = [by_day[k] for k in sorted(by_day.keys())]
    return {"shop": shop, "start": iso(start_dt), "end": iso(end_dt), "count_orders": len(all_orders), "rows": rows}
