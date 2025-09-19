import os
import sqlite3
from pathlib import Path
from typing import Optional

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "data.sqlite"

def _conn():
    return sqlite3.connect(str(DB_PATH))

def init_db():
    with _conn() as cx:
        cx.execute("""
        CREATE TABLE IF NOT EXISTS shops (
            shop TEXT PRIMARY KEY,
            token TEXT NOT NULL,
            scopes TEXT DEFAULT ''
        )
        """)
        cx.commit()

def save_token(shop: str, token: str, scopes: str) -> None:
    with _conn() as cx:
        cx.execute("""
        INSERT INTO shops (shop, token, scopes) VALUES (?, ?, ?)
        ON CONFLICT(shop) DO UPDATE SET token=excluded.token, scopes=excluded.scopes
        """, (shop, token, scopes))
        cx.commit()

def get_token(shop: str) -> Optional[str]:
    with _conn() as cx:
        row = cx.execute("SELECT token FROM shops WHERE shop = ?", (shop,)).fetchone()
        return row[0] if row else None

def get_scopes(shop: str) -> str:
    with _conn() as cx:
        row = cx.execute("SELECT scopes FROM shops WHERE shop = ?", (shop,)).fetchone()
        return row[0] if row else ""
