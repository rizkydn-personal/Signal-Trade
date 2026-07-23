"""
VPS Algorithmic Trading Control Center
========================================
Dashboard monitoring akun & posisi MetaTrader 5 secara real-time via WebSocket.
"""

import os
import json
import time
import hmac
import hashlib
import secrets
import logging
import asyncio
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Optional

import MetaTrader5 as mt5
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from dotenv import load_dotenv

load_dotenv()

# ----------------------------------------------------------------------------
# 1. KONFIGURASI
# ----------------------------------------------------------------------------
MT5_LOGIN = os.getenv("MT5_LOGIN")
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")

SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "XAUUSD").split(",") if s.strip()]
REFRESH_RATE = float(os.getenv("REFRESH_RATE_SECONDS", "1"))
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "1"))
MT5_SERVER_UTC_OFFSET_HOURS = float(os.getenv("MT5_SERVER_UTC_OFFSET_HOURS", "0"))
EQUITY_CURVE_MAX_POINTS = int(os.getenv("EQUITY_CURVE_MAX_POINTS", "300"))
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("trading-dashboard")

# --- Admin Panel ("/admin") ---
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY", "")
ADMIN_SESSION_HOURS = float(os.getenv("ADMIN_SESSION_HOURS", "12"))
ADMIN_COOKIE_NAME = "admin_session"
# File log kegagalan order yang ditulis oleh EA (OrderExecutor.mq5) di folder
# MQL5/Files milik terminal MT5. Sesuaikan path ini ke lokasi file tersebut.
FAILED_ORDERS_LOG_PATH = os.getenv("FAILED_ORDERS_LOG_PATH", "failed_orders.json")
FAILED_ORDERS_MAX_ROWS = int(os.getenv("FAILED_ORDERS_MAX_ROWS", "200"))

if not ADMIN_PASSWORD:
    logger.warning(
        "ADMIN_PASSWORD belum diset di .env — halaman /admin tidak bisa login! "
        "Tambahkan ADMIN_PASSWORD=your_password_here ke file .env."
    )
if not ADMIN_SECRET_KEY:
    # Fallback acak: masih aman selama proses berjalan, tapi sesi akan hilang
    # setiap kali server di-restart. Disarankan set ADMIN_SECRET_KEY di .env.
    ADMIN_SECRET_KEY = secrets.token_hex(32)
    logger.warning(
        "ADMIN_SECRET_KEY belum diset di .env — menggunakan kunci acak sementara "
        "(sesi admin akan logout otomatis setiap restart server)."
    )

_mt5_connected = False
_reconnect_attempts = 0


# ----------------------------------------------------------------------------
# 2. FUNGSI DETEKSI MARKET CLOSED DARI MT5
# ----------------------------------------------------------------------------
def check_market_status_from_mt5() -> dict:
    """Deteksi status pasar dari MT5"""
    now_utc = datetime.utcnow()
    current_weekday = now_utc.weekday()
    current_hour = now_utc.hour
    
    # Weekend: Sabtu dan Minggu
    if current_weekday == 5:  # Saturday
        next_open = (now_utc + timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        return {
            "is_open": False,
            "status": "Market Closed",
            "reason": "Weekend - Saturday",
            "current_time": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "next_open": next_open.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "next_close": "N/A (Weekend)",
            "day": "Saturday",
            "is_weekend": True,
        }
    
    if current_weekday == 6:  # Sunday
        if current_hour < 22:
            next_open = now_utc.replace(hour=22, minute=0, second=0, microsecond=0)
            return {
                "is_open": False,
                "status": "Market Closed",
                "reason": "Weekend - Sunday (pre-open)",
                "current_time": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "next_open": next_open.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "next_close": "N/A (Weekend)",
                "day": "Sunday",
                "is_weekend": True,
            }
        else:
            return {
                "is_open": True,
                "status": "Market Open",
                "reason": "Sunday Session Open",
                "current_time": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "next_open": None,
                "next_close": None,
                "day": "Sunday",
                "is_weekend": False,
            }
    
    # Jumat malam (after 22:00 UTC)
    if current_weekday == 4 and current_hour >= 22:
        next_open = (now_utc + timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
        return {
            "is_open": False,
            "status": "Market Closed",
            "reason": "Friday Close",
            "current_time": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "next_open": next_open.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "next_close": "N/A (Weekend)",
            "day": "Friday",
            "is_weekend": True,
        }
    
    # Cek tick terakhir dari MT5
    if _mt5_connected and SYMBOLS:
        try:
            first_symbol = SYMBOLS[0]
            tick = mt5.symbol_info_tick(first_symbol)
            if tick:
                tick_time = datetime.fromtimestamp(tick.time)
                time_diff = (now_utc - tick_time).total_seconds()
                
                if time_diff > 60:
                    return {
                        "is_open": False,
                        "status": "Market Closed",
                        "reason": f"No Recent Ticks ({int(time_diff)}s ago)",
                        "current_time": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "last_tick": tick_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "next_open": "N/A",
                        "next_close": "N/A",
                        "day": now_utc.strftime("%A"),
                        "is_weekend": False,
                    }
                else:
                    return {
                        "is_open": True,
                        "status": "Market Open",
                        "reason": f"Active Trading ({int(time_diff)}s since last tick)",
                        "current_time": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "last_tick": tick_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "next_open": None,
                        "next_close": None,
                        "day": now_utc.strftime("%A"),
                        "is_weekend": False,
                    }
        except Exception as e:
            logger.warning(f"Gagal cek tick MT5: {e}")
    
    # Default: Weekday (Monday-Friday)
    if 0 <= current_weekday <= 4:
        return {
            "is_open": True,
            "status": "Market Open",
            "reason": "Regular Trading Session",
            "current_time": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "next_open": None,
            "next_close": None,
            "day": now_utc.strftime("%A"),
            "is_weekend": False,
        }
    
    # Default fallback
    return {
        "is_open": False,
        "status": "Market Closed",
        "reason": "Unknown",
        "current_time": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "next_open": "N/A",
        "next_close": "N/A",
        "day": now_utc.strftime("%A"),
        "is_weekend": True,
    }


def get_market_status() -> dict:
    """Dapatkan status pasar lengkap dari MT5"""
    return check_market_status_from_mt5()


# ----------------------------------------------------------------------------
# 3. KONEKSI MT5
# ----------------------------------------------------------------------------
def _connect_mt5_sync() -> bool:
    global _mt5_connected
    if MT5_LOGIN and MT5_PASSWORD and MT5_SERVER:
        ok = mt5.initialize(login=int(MT5_LOGIN), password=MT5_PASSWORD, server=MT5_SERVER)
    else:
        ok = mt5.initialize()

    _mt5_connected = bool(ok)
    if not ok:
        logger.error("Gagal konek ke MT5: %s", mt5.last_error())
    else:
        acc = mt5.account_info()
        logger.info("Terhubung ke MT5. Akun: %s | Server: %s", acc.login if acc else "?", acc.server if acc else "?")
    return _mt5_connected


def _mt5_time_str(epoch_seconds: float) -> str:
    if MT5_SERVER_UTC_OFFSET_HOURS:
        dt = datetime.utcfromtimestamp(epoch_seconds) + timedelta(hours=MT5_SERVER_UTC_OFFSET_HOURS)
    else:
        dt = datetime.fromtimestamp(epoch_seconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


_symbol_cache: dict = {}

def _resolve_symbol(base: str) -> Optional[str]:
    if base in _symbol_cache:
        return _symbol_cache[base]

    resolved = None
    if mt5.symbol_info(base) is not None:
        resolved = base
    else:
        all_symbols = mt5.symbols_get()
        if all_symbols:
            candidates = [s.name for s in all_symbols if base.upper() in s.name.upper()]
            if candidates:
                candidates.sort(key=len)
                resolved = candidates[0]

    if resolved is None:
        logger.warning("Simbol '%s' tidak ditemukan di broker", base)
    elif not mt5.symbol_select(resolved, True):
        logger.warning("Gagal menambahkan '%s' ke Market Watch", resolved)

    _symbol_cache[base] = resolved
    return resolved


def _fetch_snapshot_sync() -> dict:
    account = mt5.account_info()
    if account is None:
        raise RuntimeError(f"account_info() gagal: {mt5.last_error()}")

    watchlist = []
    for sym in SYMBOLS:
        resolved_sym = _resolve_symbol(sym)
        tick = mt5.symbol_info_tick(resolved_sym) if resolved_sym else None
        if tick:
            watchlist.append({
                "symbol": resolved_sym,
                "bid": tick.bid,
                "ask": tick.ask,
                "spread": round((tick.ask - tick.bid), 5),
                "time": tick.time,
            })
        else:
            watchlist.append({"symbol": sym, "bid": None, "ask": None, "spread": None, "time": None})

    positions = mt5.positions_get()
    active_trades = []
    total_floating = 0.0
    if positions:
        for p in positions:
            total_floating += p.profit
            active_trades.append({
                "ticket": p.ticket,
                "symbol": p.symbol,
                "type": "BUY" if p.type == mt5.POSITION_TYPE_BUY else "SELL",
                "volume": p.volume,
                "price_open": p.price_open,
                "price_current": p.price_current,
                "sl": p.sl,
                "tp": p.tp,
                "pnl": p.profit,
                "open_time": _mt5_time_str(p.time),
            })

    days_to_fetch = max(HISTORY_DAYS, 30)
    date_from = datetime.now() - timedelta(days=days_to_fetch)
    date_to = datetime.now() + timedelta(days=1)
    
    deals = mt5.history_deals_get(date_from, date_to)
    if deals is None or len(deals) == 0:
        deals = mt5.history_deals_get()
    
    # MT5 menampilkan "Type" di tab History berdasarkan arah deal ENTRY (pembukaan
    # posisi), bukan deal EXIT (penutupan). Saat posisi BUY ditutup, deal exit-nya
    # justru bertipe SELL (dan sebaliknya) -- jadi kita harus mapping position_id ke
    # tipe deal entry-nya supaya label BUY/SELL konsisten dengan history asli MT5.
    entry_type_by_position = {}
    if deals and len(deals) > 0:
        for d in deals:
            if d.entry == mt5.DEAL_ENTRY_IN and d.type in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL):
                entry_type_by_position[d.position_id] = d.type

    def _resolve_trade_type(d):
        # Prioritas 1: tipe deal entry (pembukaan) dari posisi yang sama -> sesuai MT5
        entry_type = entry_type_by_position.get(d.position_id)
        if entry_type is not None:
            return "BUY" if entry_type == mt5.DEAL_TYPE_BUY else "SELL"
        # Fallback: jika deal entry tidak ditemukan (mis. di luar range tanggal),
        # arah exit deal adalah kebalikan dari arah posisi aslinya
        if d.type == mt5.DEAL_TYPE_BUY:
            return "SELL"
        elif d.type == mt5.DEAL_TYPE_SELL:
            return "BUY"
        return f"TYPE_{d.type}"

    history_trades = []
    if deals and len(deals) > 0:
        for d in deals:
            if d.entry in (mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_OUT_BY, mt5.DEAL_ENTRY_INOUT):
                if d.type == mt5.DEAL_TYPE_BALANCE:
                    continue
                trade_type = _resolve_trade_type(d)
                history_trades.append({
                    "ticket": d.ticket,
                    "symbol": d.symbol,
                    "type": trade_type,
                    "volume": d.volume,
                    "pnl": d.profit,
                    "commission": d.commission,
                    "swap": d.swap,
                    "comment": d.comment if d.comment else "",
                    "time": _mt5_time_str(d.time),
                    "time_epoch": d.time,
                })
        
        if len(history_trades) == 0:
            for d in deals:
                if d.type in (mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL):
                    history_trades.append({
                        "ticket": d.ticket,
                        "symbol": d.symbol,
                        "type": _resolve_trade_type(d) if d.entry != mt5.DEAL_ENTRY_IN else ("BUY" if d.type == mt5.DEAL_TYPE_BUY else "SELL"),
                        "volume": d.volume,
                        "pnl": d.profit,
                        "commission": d.commission,
                        "swap": d.swap,
                        "comment": d.comment if d.comment else "",
                        "time": _mt5_time_str(d.time),
                        "time_epoch": d.time,
                    })
    
    if history_trades and len(history_trades) > 0:
        cutoff_epoch = (datetime.now() - timedelta(days=HISTORY_DAYS)).timestamp()
        history_trades = [t for t in history_trades if t.get("time_epoch", 0) >= cutoff_epoch]

    history_trades.sort(key=lambda t: t.get("time", ""), reverse=True)

    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    today_trades = [t for t in history_trades if t.get("time_epoch", 0) >= today_start]
    
    wins = [t["pnl"] for t in today_trades if t["pnl"] > 0]
    losses = [t["pnl"] for t in today_trades if t["pnl"] < 0]
    total_closed = len(today_trades)
    win_rate = round((len(wins) / total_closed) * 100, 1) if total_closed else 0.0
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0.0)
    best_trade = max(today_trades, key=lambda t: t["pnl"], default=None)
    worst_trade = min(today_trades, key=lambda t: t["pnl"], default=None)
    realized_pnl_today = sum(t["pnl"] + t.get("commission", 0) + t.get("swap", 0) for t in today_trades)

    market_status = get_market_status()

    return {
        "type": "snapshot",
        "connected": True,
        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "account": {
            "login": account.login,
            "server": account.server,
            "currency": account.currency,
            "balance": account.balance,
            "equity": account.equity,
            "margin": account.margin,
            "margin_free": account.margin_free,
            "margin_level": account.margin_level,
            "floating_pnl": total_floating,
        },
        "watchlist": watchlist,
        "active_trades": active_trades,
        "history_trades": history_trades,
        "stats": {
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "total_closed_today": total_closed,
            "realized_pnl_today": round(realized_pnl_today, 2),
            "best_trade": best_trade,
            "worst_trade": worst_trade,
        },
        "market_status": market_status,
    }


# ----------------------------------------------------------------------------
# 4. CONNECTION MANAGER
# ----------------------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        logger.info("Client baru terhubung. Total client: %d", len(self.active))

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)
            logger.info("Client terputus. Total client: %d", len(self.active))

    async def broadcast(self, message: dict):
        if not self.active:
            return
        payload = json.dumps(message, default=str)
        dead = []
        for ws in self.active:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


# ----------------------------------------------------------------------------
# 4b. ADMIN AUTH (session token disimpan di cookie, ditandatangani HMAC)
# ----------------------------------------------------------------------------
def _admin_make_token() -> str:
    expiry = int(time.time()) + int(ADMIN_SESSION_HOURS * 3600)
    payload = str(expiry)
    sig = hmac.new(ADMIN_SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{sig}"


def _admin_verify_token(token: Optional[str]) -> bool:
    if not token or "." not in token:
        return False
    payload, _, sig = token.partition(".")
    expected_sig = hmac.new(ADMIN_SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected_sig):
        return False
    try:
        return time.time() < int(payload)
    except ValueError:
        return False


def _admin_is_authed(request: Request) -> bool:
    return _admin_verify_token(request.cookies.get(ADMIN_COOKIE_NAME))


def _require_admin_page(request: Request):
    """Untuk route halaman (GET) — redirect ke /admin/login jika belum login."""
    if not _admin_is_authed(request):
        raise HTTPException(status_code=303, headers={"Location": "/admin/login"})


def _require_admin_api(request: Request):
    """Untuk route API (JSON) — balas 401 jika belum login."""
    if not _admin_is_authed(request):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ----------------------------------------------------------------------------
# 4c. ADMIN MT5 OPERATIONS — pending orders, close position(s), failed log
# ----------------------------------------------------------------------------
_PENDING_TYPE_NAMES = {
    getattr(mt5, "ORDER_TYPE_BUY_LIMIT", 2): "BUY LIMIT",
    getattr(mt5, "ORDER_TYPE_SELL_LIMIT", 3): "SELL LIMIT",
    getattr(mt5, "ORDER_TYPE_BUY_STOP", 4): "BUY STOP",
    getattr(mt5, "ORDER_TYPE_SELL_STOP", 5): "SELL STOP",
    getattr(mt5, "ORDER_TYPE_BUY_STOP_LIMIT", 6): "BUY STOP LIMIT",
    getattr(mt5, "ORDER_TYPE_SELL_STOP_LIMIT", 7): "SELL STOP LIMIT",
}


def _get_pending_orders_sync() -> list:
    orders = mt5.orders_get()
    result = []
    if orders:
        for o in orders:
            result.append({
                "ticket": o.ticket,
                "symbol": o.symbol,
                "type": _PENDING_TYPE_NAMES.get(o.type, f"TYPE_{o.type}"),
                "volume": o.volume_current,
                "price_open": o.price_open,
                "sl": o.sl,
                "tp": o.tp,
                "setup_time": _mt5_time_str(o.time_setup),
                "comment": o.comment or "",
            })
    result.sort(key=lambda x: x["setup_time"], reverse=True)
    return result


def _get_active_positions_sync() -> list:
    positions = mt5.positions_get()
    result = []
    if positions:
        for p in positions:
            result.append({
                "ticket": p.ticket,
                "symbol": p.symbol,
                "type": "BUY" if p.type == mt5.POSITION_TYPE_BUY else "SELL",
                "volume": p.volume,
                "price_open": p.price_open,
                "price_current": p.price_current,
                "sl": p.sl,
                "tp": p.tp,
                "pnl": p.profit,
                "open_time": _mt5_time_str(p.time),
            })
    return result


# Retcode broker untuk "Unsupported filling mode" (kadang ada broker yang
# mengembalikan 10030, kadang 10018 tergantung versi terminal).
TRADE_RETCODE_INVALID_FILL = 10030

# Terjemahan retcode MT5 yang paling sering muncul, supaya pesan error di
# dashboard jelas — bukan cuma angka. Referensi: enum ENUM_TRADE_RETCODE.
_RETCODE_MESSAGES = {
    10004: "Requote — harga sudah berubah, coba lagi.",
    10006: "Order ditolak broker.",
    10013: "Request tidak valid.",
    10014: "Volume tidak valid untuk symbol ini.",
    10015: "Harga tidak valid.",
    10016: "SL/TP tidak valid.",
    10018: "Market sedang TUTUP untuk symbol ini — order tidak bisa diproses.",
    10019: "Saldo/margin tidak cukup.",
    10021: "Tidak ada harga (requote), coba lagi sebentar lagi.",
    10025: "Tidak ada perubahan pada request.",
    10026: "Autotrading dimatikan di server broker.",
    10027: "Autotrading dimatikan di terminal MT5 (klik tombol Algo Trading di MT5).",
    10030: "Filling mode ditolak broker untuk symbol ini (sudah dicoba FOK, IOC, dan RETURN — ketiganya ditolak).",
    10031: "Tidak ada koneksi ke server trading.",
    10033: "Jumlah order pending sudah mencapai limit broker.",
    10034: "Jumlah/volume order sudah mencapai limit broker.",
}

# CATATAN PENTING: modul Python `MetaTrader5` TIDAK menyediakan konstanta
# SYMBOL_FILLING_FOK / SYMBOL_FILLING_IOC (itu cuma ada di enum MQL5, bukan
# di wrapper Python) — makai mt5.SYMBOL_FILLING_FOK akan langsung
# AttributeError. symbol_info().filling_mode tetap berupa bitmask integer
# biasa, jadi kita bandingkan langsung ke nilai bit-nya:
#   bit 0 (1) = FOK didukung, bit 1 (2) = IOC didukung.
_SYMBOL_FILLING_FOK_BIT = 1
_SYMBOL_FILLING_IOC_BIT = 2

# Urutan filling mode yang dicoba sebagai fallback jika mode "disukai" symbol
# ternyata ditolak broker. FOK & IOC paling umum didukung broker retail,
# RETURN biasanya khusus untuk akun/symbol tertentu (mis. bursa/exchange).
_FILLING_FALLBACK_ORDER = [mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_RETURN]


def _get_preferred_filling_type(symbol: str) -> int:
    """
    Deteksi mode filling yang didukung symbol dari MT5, bukan hardcode IOC.
    symbol_info().filling_mode adalah bitmask; broker sering hanya mendukung
    salah satu (FOK atau IOC), sehingga IOC yang di-hardcode sebelumnya
    memicu error [10030] Unsupported filling mode di banyak broker.
    """
    info = mt5.symbol_info(symbol)
    if info is None:
        return mt5.ORDER_FILLING_IOC

    filling_mode = info.filling_mode
    if filling_mode & _SYMBOL_FILLING_FOK_BIT:
        return mt5.ORDER_FILLING_FOK
    if filling_mode & _SYMBOL_FILLING_IOC_BIT:
        return mt5.ORDER_FILLING_IOC
    return mt5.ORDER_FILLING_RETURN


def _send_close_order_with_filling_retry(base_request: dict, symbol: str):
    """
    Kirim order close dengan filling mode yang paling cocok untuk symbol.
    Jika broker tetap menolak dengan retcode invalid-fill, coba mode
    filling lain satu per satu sebelum menyerah. Setiap percobaan dicatat
    ke log supaya kalau ketiga mode tetap gagal, penyebab aslinya (misalnya
    market memang tutup) kelihatan di log server, bukan cuma "10030".
    """
    preferred = _get_preferred_filling_type(symbol)
    order_to_try = [preferred] + [f for f in _FILLING_FALLBACK_ORDER if f != preferred]
    filling_names = {mt5.ORDER_FILLING_FOK: "FOK", mt5.ORDER_FILLING_IOC: "IOC", mt5.ORDER_FILLING_RETURN: "RETURN"}

    last_result = None
    for filling in order_to_try:
        request = dict(base_request)
        request["type_filling"] = filling
        result = mt5.order_send(request)
        last_result = result

        logger.info(
            "Close %s ticket=%s filling=%s -> retcode=%s comment=%s",
            symbol, base_request.get("position"), filling_names.get(filling, filling),
            getattr(result, "retcode", None), getattr(result, "comment", None),
        )

        if result is not None and result.retcode == mt5.TRADE_RETCODE_DONE:
            return result
        if result is not None and result.retcode != TRADE_RETCODE_INVALID_FILL:
            # Gagal karena alasan lain (harga, volume, market tutup, dll) —
            # bukan soal filling mode, jadi tidak ada gunanya coba mode lain.
            return result
        # retcode invalid-fill -> lanjut coba filling mode berikutnya
    return last_result


def _close_position_by_ticket_sync(ticket: int) -> dict:
    positions = mt5.positions_get(ticket=ticket)
    if not positions:
        return {"ticket": ticket, "success": False, "message": "Posisi tidak ditemukan (mungkin sudah tertutup)."}

    p = positions[0]
    tick = mt5.symbol_info_tick(p.symbol)
    if tick is None:
        return {"ticket": ticket, "success": False, "message": f"Gagal ambil harga untuk {p.symbol}."}

    # Cek "basi"-nya tick simbol ini secara spesifik (bukan cuma symbol pertama
    # di config seperti check_market_status_from_mt5). Kalau tick sudah lama
    # tidak update, market untuk symbol ini kemungkinan besar sedang tutup —
    # order_send tetap akan dikirim ke broker, tapi kita kasih tahu dulu di
    # pesan supaya user tidak bingung kalau nanti hasilnya juga gagal.
    tick_age_seconds = int(time.time() - tick.time)
    market_likely_closed = tick_age_seconds > 90

    if p.type == mt5.POSITION_TYPE_BUY:
        order_type = mt5.ORDER_TYPE_SELL
        price = tick.bid
    else:
        order_type = mt5.ORDER_TYPE_BUY
        price = tick.ask

    base_request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "position": p.ticket,
        "symbol": p.symbol,
        "volume": p.volume,
        "type": order_type,
        "price": price,
        "deviation": 20,
        "magic": 234000,
        "comment": "Admin manual close",
        "type_time": mt5.ORDER_TIME_GTC,
    }
    send_result = _send_close_order_with_filling_retry(base_request, p.symbol)
    if send_result is None:
        return {"ticket": ticket, "success": False, "message": f"order_send gagal: {mt5.last_error()}"}
    if send_result.retcode != mt5.TRADE_RETCODE_DONE:
        friendly = _RETCODE_MESSAGES.get(send_result.retcode, send_result.comment)
        message = f"[{send_result.retcode}] {friendly}"
        if market_likely_closed:
            message += f" (tick {p.symbol} terakhir {tick_age_seconds}s lalu — market kemungkinan tutup)"
        return {"ticket": ticket, "success": False, "message": message}
    return {"ticket": ticket, "success": True, "message": "Posisi berhasil ditutup."}


def _close_positions_bulk_sync(mode: str) -> list:
    """mode: 'all' | 'profit' | 'loss'"""
    positions = mt5.positions_get()
    if not positions:
        return []

    targets = []
    for p in positions:
        if mode == "profit" and p.profit < 0:
            continue
        if mode == "loss" and p.profit >= 0:
            continue
        targets.append(p.ticket)

    return [_close_position_by_ticket_sync(t) for t in targets]


def _read_failed_orders_sync() -> list:
    """Baca log kegagalan order yang ditulis EA (satu baris = satu objek JSON)."""
    if not os.path.exists(FAILED_ORDERS_LOG_PATH):
        return []
    rows = []
    try:
        with open(FAILED_ORDERS_LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        logger.warning("Gagal membaca file log kegagalan order: %s", e)
        return []

    rows.reverse()  # terbaru dulu
    return rows[:FAILED_ORDERS_MAX_ROWS]


# ----------------------------------------------------------------------------
# 5. BACKGROUND BROADCASTER TASK
# ----------------------------------------------------------------------------
async def market_data_broadcaster():
    global _reconnect_attempts, _mt5_connected
    equity_curve: list[dict] = []

    while True:
        if not _mt5_connected:
            backoff = min(2 ** _reconnect_attempts, 30)
            await manager.broadcast({"type": "status", "connected": False})
            await asyncio.sleep(backoff)
            ok = await asyncio.to_thread(_connect_mt5_sync)
            _reconnect_attempts = 0 if ok else _reconnect_attempts + 1
            continue

        try:
            snapshot = await asyncio.to_thread(_fetch_snapshot_sync)

            equity_curve.append({
                "t": datetime.now().strftime("%H:%M:%S"),
                "equity": snapshot["account"]["equity"],
                "balance": snapshot["account"]["balance"],
            })
            if len(equity_curve) > EQUITY_CURVE_MAX_POINTS:
                equity_curve.pop(0)
            snapshot["equity_curve"] = equity_curve

            await manager.broadcast(snapshot)
            _reconnect_attempts = 0
        except Exception as e:
            logger.exception("Gagal mengambil snapshot MT5: %s", e)
            _mt5_connected = False

        await asyncio.sleep(REFRESH_RATE)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(_connect_mt5_sync)
    task = asyncio.create_task(market_data_broadcaster())
    logger.info("Dashboard siap. Memantau simbol: %s", ", ".join(SYMBOLS))
    yield
    task.cancel()
    mt5.shutdown()
    logger.info("MT5 shutdown, server berhenti.")


app = FastAPI(title="VPS Algorithmic Trading Control Center", lifespan=lifespan)


# ----------------------------------------------------------------------------
# 6. HALAMAN DASHBOARD (HTML + CSS + JS) - PROFESSIONAL VERSION
# ----------------------------------------------------------------------------
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Trading System Center · VPS Control</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<style>
    :root {
        --bg: #0b0e14; --bg-card: #12161f; --bg-card-hover: #161b26;
        --border: #232838; --text: #e6e9f0; --text-muted: #838ba3;
        --accent: #4f7cff; --profit: #16c784; --loss: #ff4d5e; --warn: #ffb020;
        --market-closed-bg: rgba(11, 14, 20, 0.88);
        --market-closed-text: #ff6b6b;
        --market-closed-glow: rgba(255, 107, 107, 0.15);
    }
    [data-theme="light"] {
        --bg: #f4f6fb; --bg-card: #ffffff; --bg-card-hover: #f0f2f8;
        --border: #e3e7f1; --text: #1b1f2b; --text-muted: #6b7285;
        --accent: #3660ff; --profit: #0f9d58; --loss: #e53950; --warn: #b9740a;
        --market-closed-bg: rgba(255, 255, 255, 0.92);
        --market-closed-text: #dc3545;
        --market-closed-glow: rgba(220, 53, 69, 0.08);
    }
    * { box-sizing: border-box; }
    body {
        background: var(--bg); color: var(--text);
        font-family: 'Inter', sans-serif;
        transition: background .3s, color .3s;
        min-height: 100vh;
        overflow-x: hidden;
        position: relative;
    }

    /* =========================================================
       MARKET CLOSED OVERLAY - PROFESSIONAL VERSION
       ========================================================= */
    #market-overlay {
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: var(--market-closed-bg);
        backdrop-filter: blur(12px) saturate(0.8);
        -webkit-backdrop-filter: blur(12px) saturate(0.8);
        z-index: 9999;
        display: none;
        justify-content: center;
        align-items: center;
        pointer-events: none;
        transition: opacity 0.6s cubic-bezier(0.25, 0.46, 0.45, 0.94);
    }
    #market-overlay.active {
        display: flex;
        pointer-events: auto;
        animation: overlayFadeIn 0.6s ease;
    }
    @keyframes overlayFadeIn {
        0% { opacity: 0; }
        100% { opacity: 1; }
    }

    #market-overlay .overlay-container {
        max-width: 680px;
        width: 95%;
        padding: 2.5rem 2.8rem;
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 24px;
        box-shadow: 0 25px 80px rgba(0,0,0,0.6);
        position: relative;
        overflow: hidden;
        animation: contentSlideUp 0.6s cubic-bezier(0.34, 1.56, 0.64, 1);
    }
    @keyframes contentSlideUp {
        0% { opacity: 0; transform: translateY(40px) scale(0.96); }
        100% { opacity: 1; transform: translateY(0) scale(1); }
    }

    /* Close Button */
    #market-overlay .close-btn {
        position: absolute;
        top: 12px;
        right: 16px;
        background: none;
        border: none;
        color: var(--text-muted);
        font-size: 1.5rem;
        cursor: pointer;
        padding: 4px 8px;
        border-radius: 8px;
        transition: all 0.2s;
        line-height: 1;
        z-index: 10;
    }
    #market-overlay .close-btn:hover {
        color: var(--text);
        background: var(--bg);
    }

    .status-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.6rem;
        padding: 0.4rem 1.2rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        background: var(--market-closed-glow);
        color: var(--market-closed-text);
        border: 1px solid rgba(255, 107, 107, 0.2);
        margin-bottom: 1rem;
    }
    .status-badge .pulse-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: var(--market-closed-text);
        animation: pulseDot 1.8s infinite;
    }
    @keyframes pulseDot {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.3; transform: scale(0.8); }
    }

    #market-overlay .overlay-title {
        font-size: 2rem;
        font-weight: 800;
        letter-spacing: -0.02em;
        color: var(--text);
        margin-bottom: 0.35rem;
    }
    #market-overlay .overlay-title .highlight {
        color: var(--market-closed-text);
    }
    #market-overlay .overlay-subtitle {
        color: var(--text-muted);
        font-size: 1rem;
        margin-bottom: 1.5rem;
        line-height: 1.6;
    }

    #market-overlay .info-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 0.8rem;
        margin-bottom: 1.2rem;
    }
    #market-overlay .info-item {
        background: var(--bg);
        border-radius: 12px;
        padding: 0.85rem 1rem;
        border: 1px solid var(--border);
    }
    #market-overlay .info-item .label {
        font-size: 0.65rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--text-muted);
        font-weight: 600;
    }
    #market-overlay .info-item .value {
        font-family: 'JetBrains Mono', monospace;
        font-weight: 600;
        font-size: 0.95rem;
        margin-top: 0.2rem;
        color: var(--text);
    }
    #market-overlay .info-item .value.warning {
        color: var(--warn);
    }
    #market-overlay .info-item .value.danger {
        color: var(--market-closed-text);
    }
    #market-overlay .info-item .value.success {
        color: var(--profit);
    }

    #market-overlay .reason-bar {
        background: var(--bg);
        border-radius: 10px;
        padding: 0.6rem 1rem;
        border: 1px solid var(--border);
        display: flex;
        align-items: center;
        gap: 0.6rem;
        font-size: 0.85rem;
        color: var(--text-muted);
    }
    #market-overlay .reason-bar .reason-icon {
        font-size: 1.1rem;
        color: var(--market-closed-text);
    }
    #market-overlay .reason-bar .reason-text {
        color: var(--text);
        font-weight: 500;
    }

    #market-overlay .overlay-footer {
        margin-top: 1.2rem;
        padding-top: 1rem;
        border-top: 1px solid var(--border);
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 0.75rem;
        color: var(--text-muted);
    }
    #market-overlay .overlay-footer .next-session {
        display: flex;
        align-items: center;
        gap: 1rem;
    }
    #market-overlay .overlay-footer .next-session .label {
        text-transform: uppercase;
        font-weight: 600;
        font-size: 0.65rem;
        letter-spacing: 0.04em;
    }
    #market-overlay .overlay-footer .next-session .time {
        font-family: 'JetBrains Mono', monospace;
        font-weight: 600;
        color: var(--text);
    }

    /* Responsive Market Overlay */
    @media (max-width: 768px) {
        #market-overlay .overlay-container {
            padding: 1.8rem 1.5rem;
            border-radius: 18px;
        }
        #market-overlay .overlay-title { font-size: 1.5rem; }
        #market-overlay .info-grid { grid-template-columns: 1fr; gap: 0.6rem; }
        #market-overlay .overlay-footer {
            flex-direction: column;
            gap: 0.6rem;
            align-items: stretch;
        }
        #market-overlay .overlay-footer .next-session {
            justify-content: space-between;
        }
        #market-overlay .reason-bar { font-size: 0.75rem; }
    }
    @media (max-width: 480px) {
        #market-overlay .overlay-container {
            padding: 1.2rem 1rem;
            border-radius: 14px;
        }
        #market-overlay .overlay-title { font-size: 1.2rem; }
        #market-overlay .overlay-subtitle { font-size: 0.85rem; }
        #market-overlay .info-item .value { font-size: 0.8rem; }
    }

    /* =========================================================
       MAIN CONTENT
       ========================================================= */
    .navbar-custom {
        background: var(--bg-card); border-bottom: 1px solid var(--border);
        padding: .9rem 1.5rem;
        row-gap: .6rem;
    }
    .brand { font-weight: 800; font-size: 1.15rem; letter-spacing: -.02em; }
    .brand .bi { color: var(--accent); }
    .brand .brand-full { display: inline; }
    .brand .brand-short { display: none; }
    .status-pill {
        display: inline-flex; align-items: center; gap: .4rem;
        padding: .35rem .75rem; border-radius: 999px; font-size: .8rem; font-weight: 600;
        background: rgba(22,199,132,.12); color: var(--profit);
    }
    .status-pill.offline { background: rgba(255,77,94,.12); color: var(--loss); }
    .status-pill.market-closed { background: rgba(255,107,107,.15); color: var(--market-closed-text); }
    .status-dot {
        width: 8px; height: 8px; border-radius: 50%; background: currentColor;
        box-shadow: 0 0 0 0 currentColor; animation: pulse 1.6s infinite;
    }
    .status-pill.market-closed .status-dot { animation: none; background: var(--market-closed-text); }
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(22,199,132,.5); }
        70% { box-shadow: 0 0 0 6px rgba(22,199,132,0); }
        100% { box-shadow: 0 0 0 0 rgba(22,199,132,0); }
    }
    .icon-btn {
        background: var(--bg-card); border: 1px solid var(--border); color: var(--text-muted);
        width: 38px; height: 38px; border-radius: 10px; display: inline-flex;
        align-items: center; justify-content: center; cursor: pointer; transition: .15s;
    }
    .icon-btn:hover { color: var(--text); border-color: var(--accent); }
    .icon-btn.active { color: var(--accent); border-color: var(--accent); }
    .container-fluid.main { padding: 1.5rem; max-width: 1500px; margin: 0 auto; }
    .container-fluid.main.market-closed { opacity: 0.3; pointer-events: none; transition: opacity .5s; }
    .card-custom {
        background: var(--bg-card); border: 1px solid var(--border); border-radius: 14px;
        padding: 1.1rem 1.3rem;
    }
    .section-title {
        font-size: .82rem; font-weight: 700; text-transform: uppercase; letter-spacing: .06em;
        color: var(--text-muted); margin-bottom: .9rem; display: flex; align-items: center; gap: .5rem;
    }
    .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: .9rem; }
    .kpi { background: var(--bg-card); border: 1px solid var(--border); border-radius: 14px; padding: 1rem 1.1rem; }
    .kpi-label { font-size: .74rem; color: var(--text-muted); font-weight: 600; text-transform: uppercase; letter-spacing: .04em; }
    .kpi-value { font-size: 1.35rem; font-weight: 700; margin-top: .2rem; }
    .profit { color: var(--profit) !important; }
    .loss { color: var(--loss) !important; }
    .neutral { color: var(--warn) !important; }
    table { color: var(--text); }
    .table-modern {
        --bs-table-color: var(--text);
        --bs-table-bg: transparent;
        --bs-table-border-color: var(--border);
        --bs-table-striped-color: var(--text);
        --bs-table-striped-bg: var(--bg-card-hover);
        --bs-table-hover-color: var(--text);
        --bs-table-hover-bg: var(--bg-card-hover);
        color: var(--text);
        background-color: transparent;
    }
    .table-modern thead th {
        color: var(--text-muted); font-size: .72rem; text-transform: uppercase; letter-spacing: .05em;
        border-bottom: 1px solid var(--border); font-weight: 700; padding-bottom: .6rem;
    }
    .table-modern td { border-bottom: 1px solid var(--border); padding: .65rem .5rem; vertical-align: middle; font-size: .88rem; }
    .table-modern tbody tr:hover { background: var(--bg-card-hover); }
    .badge-type-buy { background: rgba(22,199,132,.14); color: var(--profit); font-weight: 700; }
    .badge-type-sell { background: rgba(255,77,94,.14); color: var(--loss); font-weight: 700; }
    .badge-symbol { background: rgba(79,124,255,.14); color: var(--accent); font-weight: 700; }
    .empty-row { text-align: center; color: var(--text-muted); padding: 1.4rem; }
    .form-select-sm, .form-control-sm { background: var(--bg); border-color: var(--border); color: var(--text); }
    .btn-outline-accent { border: 1px solid var(--border); color: var(--text-muted); }
    .btn-outline-accent:hover { border-color: var(--accent); color: var(--accent); }
    .chart-wrapper { position: relative; width: 100%; height: 280px; }
    #watchlist-container { max-height: 320px; overflow-y: auto; }
    .watchlist-row { display: flex; justify-content: space-between; align-items: center; padding: .55rem .1rem; border-bottom: 1px solid var(--border); font-size: .88rem; border-radius: 6px; }
    .watchlist-row:last-child { border-bottom: none; }
    .flash-up { animation: flashUp .5s ease; }
    .flash-down { animation: flashDown .5s ease; }
    @keyframes flashUp { 0% { background: rgba(22,199,132,.25); } 100% { background: transparent; } }
    @keyframes flashDown { 0% { background: rgba(255,77,94,.25); } 100% { background: transparent; } }
    ::-webkit-scrollbar { height: 8px; width: 8px; }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 8px; }

    @media (max-width: 991.98px) {
        .container-fluid.main { padding: 1.1rem; }
        .kpi-grid { grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: .7rem; }
        .chart-wrapper { height: 240px; }
    }
    @media (max-width: 767.98px) {
        .navbar-custom { padding: .7rem 1rem; flex-direction: column; align-items: stretch; }
        .navbar-custom > div { width: 100%; }
        .navbar-custom .brand { font-size: 1rem; justify-content: center; display: flex; align-items: center; gap: .4rem; }
        .brand .brand-full { display: none; }
        .brand .brand-short { display: inline; }
        .navbar-custom .d-flex.align-items-center.gap-2 { justify-content: space-between; }
        #status-pill { font-size: .74rem; padding: .3rem .6rem; flex: 0 0 auto; }
        #clock { font-size: .74rem; }
        .container-fluid.main { padding: .85rem; }
        .section-title { font-size: .74rem; margin-bottom: .6rem; }
        .kpi-grid { grid-template-columns: repeat(2, 1fr); gap: .6rem; }
        .kpi { padding: .75rem .8rem; border-radius: 10px; }
        .kpi-label { font-size: .68rem; }
        .kpi-value { font-size: 1.05rem; word-break: break-word; }
        .card-custom { padding: .85rem .9rem; border-radius: 10px; }
        .row.g-3.mb-4 { row-gap: .9rem !important; }
        .chart-wrapper { height: 220px; }
        #watchlist-container { max-height: 240px; }
        .watchlist-row { font-size: .8rem; padding: .45rem .1rem; }
        .table-responsive { -webkit-overflow-scrolling: touch; }
        .table-modern thead th { font-size: .66rem; padding: .5rem .4rem; white-space: nowrap; }
        .table-modern td { font-size: .78rem; padding: .5rem .4rem; white-space: nowrap; }
        #history-section-header { flex-direction: column; align-items: stretch !important; }
        #history-controls { flex-direction: column; }
        #filter-symbol, #export-csv { width: 100% !important; }
        .icon-btn { width: 34px; height: 34px; }
    }
    @media (max-width: 420px) {
        .kpi-grid { grid-template-columns: repeat(2, 1fr); gap: .5rem; }
        .kpi-value { font-size: .95rem; }
        .kpi-label { font-size: .64rem; }
        .brand .brand-short { font-size: .92rem; }
        .chart-wrapper { height: 190px; }
    }
</style>
</head>
<body data-theme="dark">

<!-- =========================================================
     MARKET CLOSED OVERLAY - PROFESSIONAL VERSION
     ========================================================= -->
<div id="market-overlay">
    <div class="overlay-container">
        <!-- Close Button X -->
        <button class="close-btn" id="close-overlay-btn" title="Close popup">✕</button>

        <div class="status-badge">
            <span class="pulse-dot"></span>
            Market Status: <strong id="overlay-status-text">Closed</strong>
        </div>

        <div class="overlay-title">
            <span class="highlight">⛔</span> Market <span class="highlight">Closed</span>
        </div>
        <p class="overlay-subtitle" id="overlay-subtitle">
            The market is currently closed. No trading activity until the next session opens.
        </p>

        <div class="info-grid">
            <div class="info-item">
                <div class="label">Status</div>
                <div class="value danger" id="overlay-status">CLOSED</div>
            </div>
            <div class="info-item">
                <div class="label">Server Time</div>
                <div class="value" id="overlay-server-time">--</div>
            </div>
            <div class="info-item">
                <div class="label">Day</div>
                <div class="value" id="overlay-day">--</div>
            </div>
            <div class="info-item">
                <div class="label">Last Tick</div>
                <div class="value" id="overlay-last-tick">--</div>
            </div>
        </div>

        <div class="reason-bar">
            <span class="reason-icon">i</span>
            <span>Reason: <span class="reason-text" id="overlay-reason">--</span></span>
        </div>

        <div class="overlay-footer">
            <div class="next-session">
                <span class="label">Next Open</span>
                <span class="time" id="overlay-next-open">--</span>
            </div>
            <div class="next-session">
                <span class="label">Next Close</span>
                <span class="time" id="overlay-next-close">--</span>
            </div>
        </div>
    </div>
</div>

<!-- =========================================================
     NAVBAR & MAIN CONTENT
     ========================================================= -->
<nav class="navbar-custom d-flex align-items-center justify-content-between flex-wrap gap-2">
    <div class="brand"><i class="bi bi-cpu-fill"></i> <span class="brand-full">Algorithmic Trading Control Center</span><span class="brand-short">Trading Control Center</span></div>
    <div class="d-flex align-items-center gap-2">
        <span id="status-pill" class="status-pill offline"><span class="status-dot"></span><span id="status-text">Connecting...</span></span>
        <span id="clock" class="mono small"></span>
        <button id="sound-toggle" class="icon-btn" title="Trade closed sound notification"><i class="bi bi-bell"></i></button>
        <button id="theme-toggle" class="icon-btn" title="Toggle theme"><i class="bi bi-moon-stars"></i></button>
    </div>
</nav>

<div class="container-fluid main">

    <!-- KPI: RINGKASAN AKUN -->
    <div class="section-title"><i class="bi bi-wallet2"></i> Account Summary</div>
    <div class="kpi-grid mb-4">
        <div class="kpi"><div class="kpi-label">Balance</div><div class="kpi-value mono" id="kpi-balance">--</div></div>
        <div class="kpi"><div class="kpi-label">Equity</div><div class="kpi-value mono" id="kpi-equity">--</div></div>
        <div class="kpi"><div class="kpi-label">Floating P/L</div><div class="kpi-value mono" id="kpi-floating">--</div></div>
        <div class="kpi"><div class="kpi-label">Margin</div><div class="kpi-value mono" id="kpi-margin">--</div></div>
        <div class="kpi"><div class="kpi-label">Free Margin</div><div class="kpi-value mono" id="kpi-margin-free">--</div></div>
        <div class="kpi"><div class="kpi-label">Margin Level</div><div class="kpi-value mono" id="kpi-margin-level">--</div></div>
    </div>

    <div class="row g-3 mb-4">
        <div class="col-lg-8">
            <div class="card-custom h-100">
                <div class="section-title mb-2"><i class="bi bi-graph-up-arrow"></i> Equity vs Balance Curve (Live)</div>
                <div class="chart-wrapper">
                    <canvas id="equityChart"></canvas>
                </div>
            </div>
        </div>
        <div class="col-lg-4">
            <div class="card-custom h-100">
                <div class="section-title mb-2"><i class="bi bi-list-ul"></i> Price Watchlist</div>
                <div id="watchlist-container"><div class="empty-row">Loading prices...</div></div>
            </div>
        </div>
    </div>

    <div class="section-title"><i class="bi bi-bar-chart-line"></i> Today's Performance Statistics</div>
    <div class="kpi-grid mb-4">
        <div class="kpi"><div class="kpi-label">Win Rate</div><div class="kpi-value mono" id="kpi-winrate">--</div></div>
        <div class="kpi"><div class="kpi-label">Profit Factor</div><div class="kpi-value mono" id="kpi-pf">--</div></div>
        <div class="kpi"><div class="kpi-label">Total Closed</div><div class="kpi-value mono" id="kpi-total-closed">--</div></div>
        <div class="kpi"><div class="kpi-label">Realized P/L</div><div class="kpi-value mono" id="kpi-realized">--</div></div>
        <div class="kpi"><div class="kpi-label">Best Trade</div><div class="kpi-value mono" id="kpi-best">--</div></div>
        <div class="kpi"><div class="kpi-label">Worst Trade</div><div class="kpi-value mono" id="kpi-worst">--</div></div>
    </div>

    <div class="card-custom mb-4">
        <div class="d-flex justify-content-between align-items-center mb-2">
            <div class="section-title mb-0"><i class="bi bi-lightning-charge-fill"></i> Active Trades</div>
            <span class="badge bg-secondary" id="active-count">0</span>
        </div>
        <div class="table-responsive">
            <table class="table table-modern mb-0">
                <thead><tr>
                    <th>Ticket</th><th>Symbol</th><th>Type</th><th>Lot</th>
                    <th>Entry</th><th>Current</th><th>SL / TP</th><th>Open Time</th><th class="text-end">P/L</th>
                </tr></thead>
                <tbody id="active-trades-table">
                    <tr><td colspan="9" class="empty-row">Waiting for data...</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <div class="card-custom mb-4">
        <div id="history-section-header" class="d-flex justify-content-between align-items-center flex-wrap gap-2 mb-2">
            <div class="section-title mb-0"><i class="bi bi-clock-history"></i> Trade History</div>
            <div id="history-controls" class="d-flex gap-2">
                <select id="filter-symbol" class="form-select form-select-sm" style="width:auto;">
                    <option value="ALL">All Symbols</option>
                </select>
                <button id="export-csv" class="btn btn-sm btn-outline-accent"><i class="bi bi-download"></i> Export CSV</button>
            </div>
        </div>
        <div class="table-responsive">
            <table class="table table-modern mb-0">
                <thead><tr>
                    <th>Time</th><th>Symbol</th><th>Type</th><th>Lot</th>
                    <th class="text-end">P/L</th>
                </tr></thead>
                <tbody id="history-trades-table">
                    <tr><td colspan="7" class="empty-row">Waiting for data...</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <div class="text-center text-muted small mb-3">
        Server time: <span id="server-time" class="mono">--</span> &middot; Refresh every <span id="refresh-rate" class="mono"></span>s
    </div>
</div>

<script>
let currentCurrency = "USD";
const fmtMoney = (v) => (v === null || v === undefined) ? "--" : (v < 0 ? "-" : "") + Math.abs(v).toLocaleString("en-US", {minimumFractionDigits:2, maximumFractionDigits:2}) + " " + currentCurrency;
const pnlClass = (v) => v > 0 ? "profit" : (v < 0 ? "loss" : "");
let historyCache = [];
let knownHistoryTickets = new Set();
let soundEnabled = localStorage.getItem("td_sound") === "1";
let equityChart, ws, reconnectDelay = 1000;
let isMarketOpen = true;
let overlayDismissed = false;

// ---------- THEME ----------
function applyTheme(theme) {
    document.body.setAttribute("data-theme", theme);
    document.getElementById("theme-toggle").innerHTML = theme === "dark"
        ? '<i class="bi bi-moon-stars"></i>' : '<i class="bi bi-sun-fill"></i>';
    localStorage.setItem("td_theme", theme);
}
applyTheme(localStorage.getItem("td_theme") || "dark");
document.getElementById("theme-toggle").onclick = () => {
    applyTheme(document.body.getAttribute("data-theme") === "dark" ? "light" : "dark");
};

// ---------- SOUND ----------
function setSoundBtn() {
    const btn = document.getElementById("sound-toggle");
    btn.classList.toggle("active", soundEnabled);
    btn.innerHTML = soundEnabled ? '<i class="bi bi-bell-fill"></i>' : '<i class="bi bi-bell-slash"></i>';
}
setSoundBtn();
document.getElementById("sound-toggle").onclick = () => {
    soundEnabled = !soundEnabled;
    localStorage.setItem("td_sound", soundEnabled ? "1" : "0");
    setSoundBtn();
};
function playBeep(freq) {
    if (!soundEnabled) return;
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.frequency.value = freq;
        osc.type = "sine";
        gain.gain.setValueAtTime(0.15, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.35);
        osc.connect(gain).connect(ctx.destination);
        osc.start(); osc.stop(ctx.currentTime + 0.35);
    } catch (e) {}
}

// ---------- CLOCK ----------
setInterval(() => {
    document.getElementById("clock").innerText = new Date().toLocaleTimeString("id-ID");
}, 1000);

// ---------- MARKET OVERLAY ----------
function updateMarketOverlay(data) {
    const overlay = document.getElementById('market-overlay');
    const status = data.market_status || {};
    isMarketOpen = status.is_open !== undefined ? status.is_open : true;
    
    // If overlay already dismissed by user, don't show again
    if (overlayDismissed) {
        return;
    }
    
    if (!isMarketOpen) {
        overlay.classList.add('active');
        document.getElementById('overlay-status-text').innerText = 'Closed';
        document.getElementById('overlay-status').innerText = 'CLOSED';
        document.getElementById('overlay-server-time').innerText = status.current_time || '--';
        document.getElementById('overlay-day').innerText = status.day || '--';
        document.getElementById('overlay-last-tick').innerText = status.last_tick || '--';
        document.getElementById('overlay-reason').innerText = status.reason || 'Unknown';
        document.getElementById('overlay-next-open').innerText = status.next_open || '--';
        document.getElementById('overlay-next-close').innerText = status.next_close || '--';
        document.getElementById('overlay-subtitle').innerText = status.is_weekend 
            ? 'Weekend market closure. Trading will resume in the next session.' 
            : 'The market is currently closed. No trading activity until the next session opens.';
        
        // Update status pill
        const pill = document.getElementById('status-pill');
        pill.className = 'status-pill market-closed';
        document.getElementById('status-text').innerText = 'Market Closed';
        
        // Lock dashboard
        document.querySelector('.container-fluid.main').classList.add('market-closed');
    } else {
        overlay.classList.remove('active');
        document.querySelector('.container-fluid.main').classList.remove('market-closed');
    }
}

// Close button - langsung dismiss overlay tanpa password
document.getElementById('close-overlay-btn').onclick = () => {
    overlayDismissed = true;
    document.getElementById('market-overlay').classList.remove('active');
    document.querySelector('.container-fluid.main').classList.remove('market-closed');
    // Update status pill
    const pill = document.getElementById('status-pill');
    pill.className = 'status-pill';
    document.getElementById('status-text').innerText = 'Dismissed';
};

// ---------- EQUITY CHART ----------
function initChart() {
    const ctx = document.getElementById("equityChart").getContext("2d");
    equityChart = new Chart(ctx, {
        type: "line",
        data: { labels: [], datasets: [
            { label: "Equity", data: [], borderColor: "#4f7cff", backgroundColor: "rgba(79,124,255,.08)", fill: true, tension: .3, pointRadius: 0, borderWidth: 2 },
            { label: "Balance", data: [], borderColor: "#838ba3", borderDash: [4,4], fill: false, tension: .3, pointRadius: 0, borderWidth: 1.5 }
        ]},
        options: {
            responsive: true, maintainAspectRatio: false, animation: false,
            plugins: { legend: { labels: { color: "#838ba3", boxWidth: 12 } } },
            scales: {
                x: { ticks: { color: "#838ba3", maxTicksLimit: 8 }, grid: { color: "rgba(131,139,163,.08)" } },
                y: { ticks: { color: "#838ba3" }, grid: { color: "rgba(131,139,163,.08)" } }
            }
        }
    });
}
initChart();

// ---------- WEBSOCKET ----------
function connectWS() {
    const proto = window.location.protocol === "https:" ? "wss://" : "ws://";
    ws = new WebSocket(proto + window.location.host + "/ws/data");

    ws.onopen = () => { reconnectDelay = 1000; };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.type === "status" && data.connected === false) {
            setStatus(false, "MT5 Disconnected - retrying...");
            return;
        }
        if (data.type !== "snapshot") return;
        setStatus(true, "Live");
        renderSnapshot(data);
    };

    ws.onclose = () => {
        setStatus(false, "Disconnected - reconnecting...");
        setTimeout(connectWS, reconnectDelay);
        reconnectDelay = Math.min(reconnectDelay * 1.6, 15000);
    };
    ws.onerror = () => ws.close();
}
function setStatus(online, text) {
    const pill = document.getElementById("status-pill");
    if (pill.classList.contains('market-closed')) return;
    pill.classList.toggle("offline", !online);
    document.getElementById("status-text").innerText = text;
}
connectWS();

// ---------- RENDER ----------
function renderSnapshot(data) {
    updateMarketOverlay(data);
    
    try {
        document.getElementById("server-time").innerText = data.server_time;
        document.getElementById("refresh-rate").innerText = "{REFRESH_RATE}";
    } catch (e) { console.error("[render] server-time:", e); }

    try {
        const a = data.account || {};
        currentCurrency = a.currency || "USD";
        document.getElementById("kpi-balance").innerText = fmtMoney(a.balance);
        document.getElementById("kpi-equity").innerText = fmtMoney(a.equity);
        const floatEl = document.getElementById("kpi-floating");
        floatEl.innerText = fmtMoney(a.floating_pnl);
        floatEl.className = "kpi-value mono " + pnlClass(a.floating_pnl);
        document.getElementById("kpi-margin").innerText = fmtMoney(a.margin);
        document.getElementById("kpi-margin-free").innerText = fmtMoney(a.margin_free);
        const hasMarginLevel = a.margin_level !== null && a.margin_level !== undefined;
        document.getElementById("kpi-margin-level").innerText = hasMarginLevel ? Number(a.margin_level).toFixed(1) + "%" : "--";
    } catch (e) { console.error("[render] account/margin KPI:", e); }

    try {
        if (!window.__prevPrices) window.__prevPrices = {};
        let wlHtml = "";
        (data.watchlist || []).forEach(w => {
            const prev = window.__prevPrices[w.symbol];
            let flashClass = "";
            if (prev !== undefined && w.bid !== null && prev !== w.bid) {
                flashClass = w.bid > prev ? "flash-up" : "flash-down";
            }
            if (w.bid !== null) window.__prevPrices[w.symbol] = w.bid;

            wlHtml += `<div class="watchlist-row ${flashClass}">
                <span class="badge-symbol badge">${w.symbol}</span>
                <span class="text-end">
                    <span class="mono fw-bold">${w.bid !== null ? w.bid : "--"}</span>
                    <span class="mono small text-muted d-block" style="font-size:.72rem;">
                        A:${w.ask !== null ? w.ask : "--"} &middot; Sp:${w.spread !== null ? w.spread : "--"}
                    </span>
                </span>
            </div>`;
        });
        document.getElementById("watchlist-container").innerHTML = wlHtml ||
            `<div class="empty-row">Symbol not found in broker.</div>`;
    } catch (e) { console.error("[render] watchlist:", e); }

    try {
        const s = data.stats || {};
        document.getElementById("kpi-winrate").innerText = (s.win_rate ?? 0) + "%";
        document.getElementById("kpi-pf").innerText = s.profit_factor ?? "--";
        document.getElementById("kpi-total-closed").innerText = s.total_closed_today ?? 0;
        const realizedEl = document.getElementById("kpi-realized");
        realizedEl.innerText = fmtMoney(s.realized_pnl_today ?? 0);
        realizedEl.className = "kpi-value mono " + pnlClass(s.realized_pnl_today ?? 0);
        document.getElementById("kpi-best").innerText = s.best_trade ? fmtMoney(s.best_trade.pnl) : "--";
        document.getElementById("kpi-worst").innerText = s.worst_trade ? fmtMoney(s.worst_trade.pnl) : "--";
    } catch (e) { console.error("[render] stats:", e); }

    try {
        const activeTrades = data.active_trades || [];
        document.getElementById("active-count").innerText = activeTrades.length;
        let activeHtml = "";
        activeTrades.forEach(t => {
            activeHtml += `<tr>
                <td class="mono">${t.ticket}</td>
                <td><span class="badge badge-symbol">${t.symbol}</span></td>
                <td><span class="badge ${t.type === 'BUY' ? 'badge-type-buy' : 'badge-type-sell'}">${t.type}</span></td>
                <td class="mono">${t.volume}</td>
                <td class="mono">${t.price_open}</td>
                <td class="mono">${t.price_current}</td>
                <td class="mono small">${t.sl || "-"} / ${t.tp || "-"}</td>
                <td class="small">${t.open_time}</td>
                <td class="text-end mono ${pnlClass(t.pnl)}">${fmtMoney(t.pnl)}</td>
            </tr>`;
        });
        document.getElementById("active-trades-table").innerHTML = activeHtml ||
            `<tr><td colspan="9" class="empty-row">No active trades.</td></tr>`;
    } catch (e) { console.error("[render] active trades:", e); }

    try {
        historyCache = data.history_trades || [];
        const symbolFilter = document.getElementById("filter-symbol");
        const currentVal = symbolFilter.value;
        const uniqueSymbols = [...new Set(historyCache.map(h => h.symbol))];
        symbolFilter.innerHTML = '<option value="ALL">All Symbols</option>' +
            uniqueSymbols.map(s => `<option value="${s}">${s}</option>`).join("");
        symbolFilter.value = uniqueSymbols.includes(currentVal) ? currentVal : "ALL";

        historyCache.forEach(h => {
            if (!knownHistoryTickets.has(h.ticket)) {
                knownHistoryTickets.add(h.ticket);
                if (knownHistoryTickets.size > historyCache.length) return;
                playBeep(h.pnl >= 0 ? 880 : 330);
            }
        });

        renderHistoryTable();
    } catch (e) { console.error("[render] history:", e); }
}

function renderHistoryTable() {
    const filter = document.getElementById("filter-symbol").value;
    const rows = filter === "ALL" ? historyCache : historyCache.filter(h => h.symbol === filter);
    let html = "";
    rows.forEach(h => {
        html += `<tr>
            <td class="small mono">${h.time}</td>
            <td><span class="badge badge-symbol">${h.symbol}</span></td>
            <td><span class="badge ${h.type === 'BUY' ? 'badge-type-buy' : 'badge-type-sell'}">${h.type}</span></td>
            <td class="mono">${h.volume}</td>
            <td class="text-end mono ${pnlClass(h.pnl)}">${fmtMoney(h.pnl)}</td>
        </tr>`;
    });
    document.getElementById("history-trades-table").innerHTML = html ||
        `<tr><td colspan="7" class="empty-row">No trade history yet.</td></tr>`;

    if (window.__lastCurve) {
        equityChart.data.labels = window.__lastCurve.map(p => p.t);
        equityChart.data.datasets[0].data = window.__lastCurve.map(p => p.equity);
        equityChart.data.datasets[1].data = window.__lastCurve.map(p => p.balance);
        equityChart.update("none");
    }
}
document.getElementById("filter-symbol").addEventListener("change", renderHistoryTable);

const _origRender = renderSnapshot;
renderSnapshot = function(data) {
    window.__lastCurve = data.equity_curve || [];
    _origRender(data);
};

// ---------- CSV EXPORT ----------
document.getElementById("export-csv").onclick = () => {
    const filter = document.getElementById("filter-symbol").value;
    const rows = filter === "ALL" ? historyCache : historyCache.filter(h => h.symbol === filter);
    const header = "time,ticket,symbol,type,volume,pnl,commission,swap,comment";
    const lines = rows.map(h => [h.time, h.ticket, h.symbol, h.type, h.volume, h.pnl, h.commission, h.swap, `"${(h.comment||"").replace(/"/g,'""')}"`].join(","));
    const csv = [header, ...lines].join("\\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `trade_history_${new Date().toISOString().slice(0,10)}.csv`;
    a.click();
};
</script>
</body>
</html>
"""


ADMIN_LOGIN_HTML = """
<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Admin Login · Trading System Center</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
    :root {
        --bg: #0b0e14; --bg-card: #12161f; --border: #232838;
        --text: #e6e9f0; --text-muted: #838ba3; --accent: #4f7cff; --loss: #ff4d5e;
    }
    * { box-sizing: border-box; }
    body {
        margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center;
        background: var(--bg); color: var(--text); font-family: 'Inter', sans-serif;
    }
    .card {
        width: 100%; max-width: 360px; background: var(--bg-card); border: 1px solid var(--border);
        border-radius: 14px; padding: 32px 28px; box-shadow: 0 20px 50px rgba(0,0,0,.35);
    }
    h1 { font-size: 1.2rem; font-weight: 700; margin: 0 0 4px; }
    p.sub { color: var(--text-muted); font-size: .85rem; margin: 0 0 22px; }
    label { font-size: .8rem; color: var(--text-muted); margin-bottom: 6px; display: block; }
    input[type=password] {
        width: 100%; padding: 11px 12px; border-radius: 9px; border: 1px solid var(--border);
        background: #0e121a; color: var(--text); font-size: .95rem; outline: none;
    }
    input[type=password]:focus { border-color: var(--accent); }
    button {
        width: 100%; margin-top: 18px; padding: 11px; border-radius: 9px; border: none;
        background: var(--accent); color: #fff; font-weight: 600; font-size: .9rem; cursor: pointer;
    }
    button:hover { filter: brightness(1.08); }
    .error {
        margin-top: 14px; padding: 10px 12px; border-radius: 8px; background: rgba(255,77,94,.12);
        border: 1px solid rgba(255,77,94,.35); color: var(--loss); font-size: .82rem;
    }
</style>
</head>
<body>
    <form class="card" method="post" action="/admin/login">
        <h1>Admin Panel</h1>
        <p class="sub">Trading System Control Center</p>
        <label for="password">Password</label>
        <input type="password" id="password" name="password" autofocus required>
        __ERROR_BLOCK__
        <button type="submit">Masuk</button>
    </form>
</body>
</html>
"""

ADMIN_HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Admin · Trading System Center</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
<style>
    :root {
        --bg: #0b0e14; --bg-card: #12161f; --bg-card-hover: #161b26;
        --border: #232838; --text: #e6e9f0; --text-muted: #838ba3;
        --accent: #4f7cff; --profit: #16c784; --loss: #ff4d5e; --warn: #ffb020;
    }
    * { box-sizing: border-box; }
    html { -webkit-text-size-adjust: 100%; }
    body {
        margin: 0; background: var(--bg); color: var(--text); font-family: 'Inter', sans-serif;
        padding: 20px 24px 60px;
    }
    .mono { font-family: 'JetBrains Mono', monospace; }
    header {
        display: flex; align-items: center; justify-content: space-between; margin-bottom: 22px;
        flex-wrap: wrap; gap: 12px;
    }
    header h1 { font-size: 1.25rem; margin: 0; font-weight: 800; }
    header .sub { color: var(--text-muted); font-size: .82rem; }
    a.back-link { color: var(--text-muted); text-decoration: none; font-size: .82rem; margin-right: 14px; }
    a.back-link:hover { color: var(--text); }
    button.logout {
        background: transparent; border: 1px solid var(--border); color: var(--text-muted);
        padding: 7px 14px; border-radius: 8px; font-size: .8rem; cursor: pointer;
    }
    button.logout:hover { border-color: var(--loss); color: var(--loss); }
    .card {
        background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px;
        padding: 16px 18px; margin-bottom: 20px;
    }
    .card-head {
        display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px;
        flex-wrap: wrap; gap: 10px;
    }
    .card-head h2 { font-size: .95rem; margin: 0; font-weight: 700; }
    .count-badge {
        background: rgba(79,124,255,.15); color: var(--accent); font-size: .72rem; font-weight: 700;
        padding: 2px 9px; border-radius: 999px; margin-left: 8px;
    }
    .bulk-actions { display: flex; gap: 8px; flex-wrap: wrap; }
    .btn {
        border: 1px solid var(--border); background: #171c28; color: var(--text);
        padding: 7px 13px; border-radius: 8px; font-size: .78rem; cursor: pointer; font-weight: 600;
    }
    .btn:hover { background: var(--bg-card-hover); }
    .btn-danger { border-color: rgba(255,77,94,.4); color: var(--loss); }
    .btn-danger:hover { background: rgba(255,77,94,.1); }
    .btn-profit { border-color: rgba(22,199,132,.4); color: var(--profit); }
    .btn-profit:hover { background: rgba(22,199,132,.1); }
    .btn-loss { border-color: rgba(255,176,32,.4); color: var(--warn); }
    .btn-loss:hover { background: rgba(255,176,32,.1); }
    .table-scroll { width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch; }
    table { width: 100%; border-collapse: collapse; font-size: .82rem; }
    thead th {
        text-align: left; color: var(--text-muted); font-weight: 600; font-size: .72rem;
        text-transform: uppercase; letter-spacing: .03em; padding: 8px 10px; border-bottom: 1px solid var(--border);
        white-space: nowrap;
    }
    tbody td { padding: 9px 10px; border-bottom: 1px solid var(--border); vertical-align: middle; }
    tbody tr:hover { background: var(--bg-card-hover); }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 6px; font-size: .72rem; font-weight: 700; }
    .badge-buy { background: rgba(22,199,132,.15); color: var(--profit); }
    .badge-sell { background: rgba(255,77,94,.15); color: var(--loss); }
    .badge-pending { background: rgba(255,176,32,.15); color: var(--warn); }
    .pnl-pos { color: var(--profit); }
    .pnl-neg { color: var(--loss); }
    .x-btn {
        background: rgba(255,77,94,.12); border: 1px solid rgba(255,77,94,.35); color: var(--loss);
        width: 26px; height: 26px; border-radius: 7px; cursor: pointer; font-weight: 700; line-height: 1;
    }
    .x-btn:hover { background: rgba(255,77,94,.25); }
    .empty-row { text-align: center; color: var(--text-muted); padding: 20px !important; }
    .reason-text { color: var(--loss); }
    /* Modal */
    .modal-backdrop {
        position: fixed; inset: 0; background: rgba(0,0,0,.55); display: none;
        align-items: center; justify-content: center; z-index: 999; padding: 16px;
    }
    .modal-backdrop.show { display: flex; }
    .modal-box {
        background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px;
        padding: 22px; max-width: 380px; width: 100%;
    }
    .modal-box h3 { margin: 0 0 10px; font-size: 1rem; }
    .modal-box p { color: var(--text-muted); font-size: .85rem; margin: 0 0 18px; }
    .modal-actions { display: flex; gap: 10px; justify-content: flex-end; }
    .modal-actions .btn { flex: 1; padding: 10px 13px; }
    .toast-wrap {
        position: fixed; top: 16px; right: 16px; left: 16px; z-index: 1000;
        display: flex; flex-direction: column; align-items: flex-end; gap: 8px;
    }
    .toast {
        padding: 10px 14px; border-radius: 8px; font-size: .82rem; font-weight: 600;
        min-width: 220px; max-width: 100%; box-shadow: 0 8px 24px rgba(0,0,0,.35);
    }
    .toast-ok { background: rgba(22,199,132,.15); border: 1px solid rgba(22,199,132,.4); color: var(--profit); }
    .toast-err { background: rgba(255,77,94,.15); border: 1px solid rgba(255,77,94,.4); color: var(--loss); }

    /* ---------------- MOBILE LAYOUT ---------------- */
    @media (max-width: 720px) {
        body { padding: 14px 12px 50px; }
        header { flex-direction: column; align-items: stretch; gap: 8px; margin-bottom: 16px; }
        header > div { text-align: left; }
        header h1 { font-size: 1.08rem; display: block !important; margin-top: 2px; }
        a.back-link { font-size: .78rem; }
        header .sub { font-size: .76rem; }
        button.logout { align-self: flex-start; padding: 8px 16px; }

        .card { padding: 12px 12px; margin-bottom: 14px; border-radius: 10px; }
        .card-head { margin-bottom: 10px; }
        .card-head h2 { font-size: .88rem; }

        .bulk-actions { width: 100%; gap: 6px; }
        .bulk-actions .btn { flex: 1 1 calc(33.333% - 6px); padding: 9px 6px; font-size: .72rem; }

        /* Tabel tetap berbentuk list biasa, tapi bisa digeser horizontal (sama seperti dashboard utama) */
        .table-scroll { -webkit-overflow-scrolling: touch; }
        thead th { font-size: .68rem; padding: 8px 8px; white-space: nowrap; }
        tbody td { font-size: .78rem; padding: 9px 8px; white-space: nowrap; }
        .x-btn { width: 30px; height: 30px; font-size: 1rem; }

        .modal-box { padding: 18px; }
        .toast-wrap { left: 10px; right: 10px; top: 10px; align-items: stretch; }
        .toast { min-width: 0; text-align: center; }
    }
</style>
</head>
<body>
<header>
    <div>
        <a class="back-link" href="/">&larr; Dashboard</a>
        <h1 style="display:inline;">Admin Control Panel</h1>
        <div class="sub">Kelola pending order, posisi aktif, dan log kegagalan robot EA</div>
    </div>
    <button class="logout" id="logout-btn">Logout</button>
</header>

<div class="card">
    <div class="card-head">
        <h2>Pending Orders <span class="count-badge" id="pending-count">0</span></h2>
    </div>
    <div class="table-scroll">
    <table>
        <thead><tr>
            <th>Ticket</th><th>Symbol</th><th>Type</th><th>Volume</th>
            <th>Price</th><th>SL / TP</th><th>Setup Time</th><th>Comment</th>
        </tr></thead>
        <tbody id="pending-table"><tr><td colspan="8" class="empty-row">Memuat...</td></tr></tbody>
    </table>
    </div>
</div>

<div class="card">
    <div class="card-head">
        <h2>Active Trades <span class="count-badge" id="active-count">0</span></h2>
        <div class="bulk-actions">
            <button class="btn btn-profit" id="close-profit-btn">Close All Profit</button>
            <button class="btn btn-loss" id="close-loss-btn">Close All Loss</button>
            <button class="btn btn-danger" id="close-all-btn">Close All Position</button>
        </div>
    </div>
    <div class="table-scroll">
    <table>
        <thead><tr>
            <th>Ticket</th><th>Symbol</th><th>Type</th><th>Volume</th><th>Open</th>
            <th>Current</th><th>SL / TP</th><th>Open Time</th><th>PnL</th><th></th>
        </tr></thead>
        <tbody id="active-table"><tr><td colspan="10" class="empty-row">Memuat...</td></tr></tbody>
    </table>
    </div>
</div>

<div class="card">
    <div class="card-head">
        <h2>Failed Trades (Robot EA) <span class="count-badge" id="failed-count">0</span></h2>
    </div>
    <div class="table-scroll">
    <table>
        <thead><tr>
            <th>Time</th><th>Symbol</th><th>Action</th><th>Volume</th><th>SL / TP</th><th>Retcode</th><th>Alasan Gagal</th>
        </tr></thead>
        <tbody id="failed-table"><tr><td colspan="7" class="empty-row">Memuat...</td></tr></tbody>
    </table>
    </div>
</div>

<div class="modal-backdrop" id="modal-backdrop">
    <div class="modal-box">
        <h3 id="modal-title">Konfirmasi</h3>
        <p id="modal-desc">Apakah kamu yakin?</p>
        <div class="modal-actions">
            <button class="btn" id="modal-cancel">Batal</button>
            <button class="btn btn-danger" id="modal-confirm">Ya, Lanjutkan</button>
        </div>
    </div>
</div>

<div class="toast-wrap" id="toast-wrap"></div>

<script>
const REFRESH_MS = __REFRESH_MS__;
let pendingAction = null;

function fmtMoney(v) {
    if (v === null || v === undefined) return "--";
    return Number(v).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
}
function pnlClass(v) { return v >= 0 ? "pnl-pos" : "pnl-neg"; }

function showToast(message, ok) {
    const wrap = document.getElementById("toast-wrap");
    const el = document.createElement("div");
    el.className = "toast " + (ok ? "toast-ok" : "toast-err");
    el.innerText = message;
    wrap.appendChild(el);
    setTimeout(() => el.remove(), 4000);
}

function openModal(title, desc, onConfirm) {
    document.getElementById("modal-title").innerText = title;
    document.getElementById("modal-desc").innerText = desc;
    document.getElementById("modal-backdrop").classList.add("show");
    pendingAction = onConfirm;
}
function closeModal() {
    document.getElementById("modal-backdrop").classList.remove("show");
    pendingAction = null;
}
document.getElementById("modal-cancel").onclick = closeModal;
document.getElementById("modal-confirm").onclick = async () => {
    const action = pendingAction;
    closeModal();
    if (action) await action();
};

async function apiPost(url) {
    const res = await fetch(url, { method: "POST" });
    if (res.status === 401) { window.location.href = "/admin/login"; return null; }
    return res.json();
}

async function loadData() {
    try {
        const res = await fetch("/admin/api/data");
        if (res.status === 401) { window.location.href = "/admin/login"; return; }
        const data = await res.json();
        renderPending(data.pending_orders || []);
        renderActive(data.active_trades || []);
        renderFailed(data.failed_orders || []);
    } catch (e) { console.error("[admin] load data failed:", e); }
}

function renderPending(rows) {
    document.getElementById("pending-count").innerText = rows.length;
    let html = "";
    rows.forEach(o => {
        html += `<tr>
            <td class="mono">${o.ticket}</td>
            <td><span class="badge badge-pending">${o.symbol}</span></td>
            <td>${o.type}</td>
            <td class="mono">${o.volume}</td>
            <td class="mono">${o.price_open}</td>
            <td class="mono small">${o.sl || "-"} / ${o.tp || "-"}</td>
            <td>${o.setup_time}</td>
            <td>${o.comment || "-"}</td>
        </tr>`;
    });
    document.getElementById("pending-table").innerHTML = html || `<tr><td colspan="8" class="empty-row">Tidak ada pending order.</td></tr>`;
}

function renderActive(rows) {
    document.getElementById("active-count").innerText = rows.length;
    let html = "";
    rows.forEach(t => {
        html += `<tr>
            <td class="mono">${t.ticket}</td>
            <td><span class="badge ${t.type === 'BUY' ? 'badge-buy' : 'badge-sell'}">${t.symbol}</span></td>
            <td><span class="badge ${t.type === 'BUY' ? 'badge-buy' : 'badge-sell'}">${t.type}</span></td>
            <td class="mono">${t.volume}</td>
            <td class="mono">${t.price_open}</td>
            <td class="mono">${t.price_current}</td>
            <td class="mono small">${t.sl || "-"} / ${t.tp || "-"}</td>
            <td>${t.open_time}</td>
            <td class="mono ${pnlClass(t.pnl)}">${fmtMoney(t.pnl)}</td>
            <td><button class="x-btn" data-ticket="${t.ticket}" title="Close posisi ini">&times;</button></td>
        </tr>`;
    });
    document.getElementById("active-table").innerHTML = html || `<tr><td colspan="10" class="empty-row">Tidak ada posisi aktif.</td></tr>`;

    document.querySelectorAll(".x-btn").forEach(btn => {
        btn.onclick = () => {
            const ticket = btn.getAttribute("data-ticket");
            openModal(
                "Tutup Posisi #" + ticket,
                "Posisi ini akan ditutup di harga market saat ini. Lanjutkan?",
                async () => {
                    const r = await apiPost("/admin/api/close/" + ticket);
                    if (r) {
                        showToast(r.success ? "Posisi #" + ticket + " ditutup." : "Gagal: " + r.message, r.success);
                        loadData();
                    }
                }
            );
        };
    });
}

function renderFailed(rows) {
    document.getElementById("failed-count").innerText = rows.length;
    let html = "";
    rows.forEach(f => {
        html += `<tr>
            <td class="small">${f.time || "-"}</td>
            <td><span class="badge badge-pending">${f.symbol || "-"}</span></td>
            <td>${f.action || "-"}</td>
            <td class="mono">${f.volume ?? "-"}</td>
            <td class="mono small">${f.sl ?? "-"} / ${f.tp ?? "-"}</td>
            <td class="mono">${f.retcode ?? "-"}</td>
            <td class="reason-text">${f.reason || "-"}</td>
        </tr>`;
    });
    document.getElementById("failed-table").innerHTML = html || `<tr><td colspan="7" class="empty-row">Belum ada kegagalan tercatat.</td></tr>`;
}

document.getElementById("close-all-btn").onclick = () => {
    openModal("Close All Position", "Semua posisi aktif akan ditutup sekarang. Tindakan ini tidak bisa dibatalkan. Lanjutkan?", async () => {
        const r = await apiPost("/admin/api/close-all");
        if (r) { showToast(`Selesai: ${r.filter(x=>x.success).length}/${r.length} posisi ditutup.`, true); loadData(); }
    });
};
document.getElementById("close-profit-btn").onclick = () => {
    openModal("Close All Profit", "Semua posisi yang sedang profit akan ditutup sekarang. Lanjutkan?", async () => {
        const r = await apiPost("/admin/api/close-all-profit");
        if (r) { showToast(`Selesai: ${r.filter(x=>x.success).length}/${r.length} posisi profit ditutup.`, true); loadData(); }
    });
};
document.getElementById("close-loss-btn").onclick = () => {
    openModal("Close All Loss", "Semua posisi yang sedang loss akan ditutup sekarang. Lanjutkan?", async () => {
        const r = await apiPost("/admin/api/close-all-loss");
        if (r) { showToast(`Selesai: ${r.filter(x=>x.success).length}/${r.length} posisi loss ditutup.`, true); loadData(); }
    });
};
document.getElementById("logout-btn").onclick = async () => {
    await fetch("/admin/logout", { method: "POST" });
    window.location.href = "/admin/login";
};

loadData();
setInterval(loadData, REFRESH_MS);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return HTMLResponse(
        content=HTML_TEMPLATE.replace("{REFRESH_RATE}", str(REFRESH_RATE)),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/health")
async def health():
    market_status = get_market_status()
    return {
        "mt5_connected": _mt5_connected,
        "clients": len(manager.active),
        "symbols": SYMBOLS,
        "market_open": market_status.get("is_open", False),
        "market_reason": market_status.get("reason", "Unknown"),
    }


@app.websocket("/ws/data")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ----------------------------------------------------------------------------
# 7. ADMIN ROUTES ("/admin") — dilindungi password dari .env
# ----------------------------------------------------------------------------
@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_page(error: Optional[str] = None):
    error_block = ""
    if error:
        error_block = '<div class="error">Password salah. Silakan coba lagi.</div>'
    return HTMLResponse(content=ADMIN_LOGIN_HTML.replace("__ERROR_BLOCK__", error_block))


@app.post("/admin/login")
async def admin_login_submit(password: str = Form(...)):
    if not ADMIN_PASSWORD or not secrets.compare_digest(password, ADMIN_PASSWORD):
        return RedirectResponse(url="/admin/login?error=1", status_code=303)

    token = _admin_make_token()
    resp = RedirectResponse(url="/admin", status_code=303)
    resp.set_cookie(
        key=ADMIN_COOKIE_NAME,
        value=token,
        max_age=int(ADMIN_SESSION_HOURS * 3600),
        httponly=True,
        samesite="lax",
    )
    return resp


@app.post("/admin/logout")
async def admin_logout():
    resp = JSONResponse(content={"ok": True})
    resp.delete_cookie(ADMIN_COOKIE_NAME)
    return resp


@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    if not _admin_is_authed(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    return HTMLResponse(content=ADMIN_HTML_TEMPLATE.replace("__REFRESH_MS__", str(int(REFRESH_RATE * 1000))))


@app.get("/admin/api/data")
async def admin_api_data(request: Request):
    _require_admin_api(request)
    if not _mt5_connected:
        return JSONResponse(content={"pending_orders": [], "active_trades": [], "failed_orders": [], "connected": False})

    pending_orders, active_trades, failed_orders = await asyncio.gather(
        asyncio.to_thread(_get_pending_orders_sync),
        asyncio.to_thread(_get_active_positions_sync),
        asyncio.to_thread(_read_failed_orders_sync),
    )
    return JSONResponse(content={
        "connected": True,
        "pending_orders": pending_orders,
        "active_trades": active_trades,
        "failed_orders": failed_orders,
    })


@app.post("/admin/api/close/{ticket}")
async def admin_api_close_one(ticket: int, request: Request):
    _require_admin_api(request)
    result = await asyncio.to_thread(_close_position_by_ticket_sync, ticket)
    return JSONResponse(content=result)


@app.post("/admin/api/close-all")
async def admin_api_close_all(request: Request):
    _require_admin_api(request)
    results = await asyncio.to_thread(_close_positions_bulk_sync, "all")
    return JSONResponse(content=results)


@app.post("/admin/api/close-all-profit")
async def admin_api_close_all_profit(request: Request):
    _require_admin_api(request)
    results = await asyncio.to_thread(_close_positions_bulk_sync, "profit")
    return JSONResponse(content=results)


@app.post("/admin/api/close-all-loss")
async def admin_api_close_all_loss(request: Request):
    _require_admin_api(request)
    results = await asyncio.to_thread(_close_positions_bulk_sync, "loss")
    return JSONResponse(content=results)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)