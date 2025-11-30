from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

import httpx
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from usdt_converter import UsdtConverter

# =========================
# Paths
# =========================
BASE_DIR = Path(__file__).parent.resolve()
FRONTEND_DIR = BASE_DIR / "frontend"
STATIC_DIR = FRONTEND_DIR / "static"
DATASET_DIR = BASE_DIR / "datasets"

ROUTES_CSV_PATH = BASE_DIR / "mexc_usdtm_with_dex.csv"
DATASET_RAW_DIR = DATASET_DIR / "dex_raw"

# =========================
# Logging (quiet but useful)
# =========================
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("Lets.Trade")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)


# =========================
# Helpers
# =========================
def now_ts() -> float:
    return time.time()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s or s.lower() in {"nan", "none"}:
            return None
        return float(s)
    except Exception:
        return None


def mask_secret(s: Optional[str], keep: int = 4) -> str:
    if not s:
        return "None"
    if len(s) <= keep * 2:
        return "***"
    return s[:keep] + "…" + s[-keep:]


def clamp(n: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, n))


# =========================
# Config (env)
# =========================
@dataclass
class LetsTradeConfig:
    spread_action_pct: float = 1.0
    spread_setup_pct: float = 0.5

    cex_notional_usdt: float = 100.0  # kept for future depth; dashboard uses mid now

    dex_enabled: bool = True

    # DexScreener scheduler
    dex_rps: float = 12.0                   # requests per second
    dex_refresh_target_sec: float = 90.0    # “soft target” (average age for full scan)
    dex_pick_per_symbol: int = 1            # how many best pairs per symbol to track (1 is fastest)

    # USD->USDT refresh
    usd_to_usdt_refresh_sec: float = 60.0

    # Dataset
    dataset_flush_sec: float = 1.0
    dataset_mode: str = "routed"  # routed | signals | all

    # Paths
    routes_csv: str = str(ROUTES_CSV_PATH)

    # Optional: store raw DexScreener responses
    save_dex_raw: bool = True


def load_config() -> LetsTradeConfig:
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    cfg = LetsTradeConfig()

    def _f(name: str, default: float) -> float:
        try:
            return float(os.getenv(name, str(default)))
        except Exception:
            return default

    def _i(name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)))
        except Exception:
            return default

    def _b(name: str, default: bool) -> bool:
        v = os.getenv(name)
        if v is None:
            return default
        v = v.strip().lower()
        if v in {"1", "true", "yes", "y", "on"}:
            return True
        if v in {"0", "false", "no", "n", "off"}:
            return False
        return default

    cfg.spread_action_pct = _f("SPREAD_ACTION_PCT", cfg.spread_action_pct)
    cfg.spread_setup_pct = _f("SPREAD_SETUP_PCT", cfg.spread_setup_pct)
    cfg.cex_notional_usdt = _f("CEX_NOTIONAL_USDT", cfg.cex_notional_usdt)

    cfg.dex_enabled = _b("DEX_ENABLED", cfg.dex_enabled)
    cfg.dex_rps = _f("DEX_RPS", cfg.dex_rps)
    cfg.dex_refresh_target_sec = _f("DEX_REFRESH_TARGET_SEC", cfg.dex_refresh_target_sec)
    cfg.dex_pick_per_symbol = _i("DEX_PICK_PER_SYMBOL", cfg.dex_pick_per_symbol)
    cfg.dex_pick_per_symbol = max(1, min(3, cfg.dex_pick_per_symbol))

    cfg.usd_to_usdt_refresh_sec = _f("USD_TO_USDT_REFRESH_SEC", cfg.usd_to_usdt_refresh_sec)

    cfg.dataset_flush_sec = _f("DATASET_FLUSH_SEC", cfg.dataset_flush_sec)
    cfg.dataset_mode = (os.getenv("DATASET_MODE", cfg.dataset_mode) or "routed").strip().lower()
    if cfg.dataset_mode not in {"routed", "signals", "all"}:
        cfg.dataset_mode = "routed"

    cfg.routes_csv = os.getenv("ROUTES_CSV", cfg.routes_csv)
    cfg.save_dex_raw = _b("SAVE_DEX_RAW", cfg.save_dex_raw)

    log.info(
        "Config loaded: spread(action/setup)=%.3f/%.3f cex_notional=%.2f dex=%s "
        "dex(rps=%.1f target=%.0fs pick=%d) usd→usdt_refresh=%.0fs dataset(mode=%s flush=%.1fs) routes=%s raw=%s",
        cfg.spread_action_pct,
        cfg.spread_setup_pct,
        cfg.cex_notional_usdt,
        "ON" if cfg.dex_enabled else "OFF",
        cfg.dex_rps,
        cfg.dex_refresh_target_sec,
        cfg.dex_pick_per_symbol,
        cfg.usd_to_usdt_refresh_sec,
        cfg.dataset_mode,
        cfg.dataset_flush_sec,
        cfg.routes_csv,
        "ON" if cfg.save_dex_raw else "OFF",
    )
    return cfg


CFG = load_config()

# =========================
# Events (for /log page)
# =========================
GLOBAL_EVENTS: Deque[Dict[str, Any]] = deque(maxlen=8000)
SYMBOL_EVENTS: Dict[str, Deque[Dict[str, Any]]] = {}


def push_event(level: str, msg: str, symbol: Optional[str] = None, extra: Optional[Dict[str, Any]] = None) -> None:
    ev = {"ts": now_ts(), "level": level, "symbol": symbol, "msg": msg, "extra": extra or {}}
    GLOBAL_EVENTS.append(ev)
    if symbol:
        if symbol not in SYMBOL_EVENTS:
            SYMBOL_EVENTS[symbol] = deque(maxlen=3000)
        SYMBOL_EVENTS[symbol].append(ev)


# =========================
# Runtime state
# =========================
@dataclass
class DexPairRoute:
    chain: str
    chain_id: Optional[int]
    token_address: Optional[str]
    pair_address: str
    pair_url: Optional[str]
    quote_symbol: Optional[str]
    init_liq_usd: Optional[float] = None
    init_vol24: Optional[float] = None


@dataclass
class SymbolRuntime:
    symbol: str
    base: str
    quote: str = "USDT"

    # MEXC
    mexc_ts: float = 0.0
    last: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume24: Optional[float] = None
    mid: Optional[float] = None

    # DEX (DexScreener best route at runtime)
    dex_ts: float = 0.0
    dex_price_usd: Optional[float] = None
    dex_price_usdt: Optional[float] = None
    dex_status: str = "NO_ROUTE"  # OK/NO_ROUTE/NO_POOL/NOT_WORKING
    dex_chain: Optional[str] = None
    dex_chain_id: Optional[int] = None
    dex_pair: Optional[str] = None
    dex_pair_url: Optional[str] = None
    dex_quote_symbol: Optional[str] = None
    dex_liq_usd: Optional[float] = None
    dex_vol24: Optional[float] = None
    dex_mode: str = "INIT"  # USDT_RATE_MEDIAN / FALLBACK_1.0 / INIT

    # Analysis
    edge_pct: Optional[float] = None
    signal: str = "NONE"  # NONE/SETUP/ACTION
    direction: Optional[str] = None

    # Routes
    routes: List[DexPairRoute] = field(default_factory=list)


SYMBOLS: Dict[str, SymbolRuntime] = {}

STATUS: Dict[str, Any] = {
    "mexc_ws": "INIT",
    "dex": "INIT",
    "dataset": "INIT",
    "routes": "INIT",
    "usd_to_usdt": "INIT",
    "usdt_rate": None,
    "usdt_rate_src": None,
    "usdt_rate_ok": False,
    "usdt_rate_age": None,
}

# =========================
# HTTP
# =========================
HTTP: Optional[httpx.AsyncClient] = None


async def get_http() -> httpx.AsyncClient:
    global HTTP
    if HTTP is None:
        HTTP = httpx.AsyncClient(timeout=httpx.Timeout(12.0), headers={"user-agent": "Lets.Trade/1.0"})
    return HTTP


# =========================
# Load routes from mexc_usdtm_with_dex.csv
# =========================
def load_routes_from_csv(path: str) -> None:
    p = Path(path)
    if not p.exists():
        STATUS["routes"] = "NOT_WORKING"
        push_event("ERROR", "Routes CSV NOT FOUND", extra={"path": str(p)})
        return

    # Group rows by mexc_symbol
    by_sym: Dict[str, List[Dict[str, Any]]] = {}
    with open(p, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            sym = (row.get("mexc_symbol") or "").strip()
            if not sym:
                continue
            by_sym.setdefault(sym, []).append(row)

    created = 0
    routed = 0

    for sym, rows in by_sym.items():
        base = (rows[0].get("mexc_base") or sym.split("_")[0]).strip() or sym.split("_")[0]
        s = SYMBOLS.get(sym)
        if not s:
            s = SymbolRuntime(symbol=sym, base=base, quote="USDT")
            SYMBOLS[sym] = s
            created += 1

        # Build route list
        routes: List[DexPairRoute] = []
        for row in rows:
            status = (row.get("status") or "").strip().upper()
            # We only accept rows that actually resolved a DEX pair
            if status not in {"OK", "APPROX", "OK_APPROX", "OK_USD"}:
                # Some collectors mark OK only; keep strict
                if status != "OK":
                    continue

            chain = (row.get("dex_chain") or "").strip()
            pair = (row.get("dex_pairAddress") or "").strip()
            if not chain or not pair:
                continue

            routes.append(
                DexPairRoute(
                    chain=chain,
                    chain_id=int(row["chain_id"]) if (row.get("chain_id") or "").strip().isdigit() else None,
                    token_address=(row.get("token_address") or "").strip() or None,
                    pair_address=pair,
                    pair_url=(row.get("dex_pairUrl") or "").strip() or None,
                    quote_symbol=(row.get("dex_quoteSymbol") or "").strip() or None,
                    init_liq_usd=safe_float(row.get("dex_liquidityUsd")),
                    init_vol24=safe_float(row.get("dex_volume24h")),
                )
            )

        # pick best routes for monitoring (liquidity first, then vol)
        if routes:
            routes.sort(
                key=lambda x: (
                    (x.init_liq_usd or 0.0),
                    (x.init_vol24 or 0.0),
                ),
                reverse=True,
            )
            s.routes = routes[: CFG.dex_pick_per_symbol]
            s.dex_status = "INIT"
            routed += 1
        else:
            s.routes = []
            s.dex_status = "NO_ROUTE"

    STATUS["routes"] = "OK"
    push_event(
        "INFO",
        "Routes loaded from CSV",
        extra={"symbols_total": len(by_sym), "symbols_created": created, "symbols_routed": routed, "picked_per_symbol": CFG.dex_pick_per_symbol},
    )


# =========================
# MEXC WS
# =========================
MEXC_WS_URL = "wss://contract.mexc.com/edge"


def handle_tickers_msg(items: List[Dict[str, Any]]) -> None:
    ts = now_ts()
    for it in items:
        symbol = it.get("symbol")
        if not symbol or not symbol.endswith("_USDT"):
            continue
        s = SYMBOLS.get(symbol)
        if not s:
            base = symbol.split("_")[0]
            s = SymbolRuntime(symbol=symbol, base=base, quote="USDT")
            SYMBOLS[symbol] = s

        s.last = safe_float(it.get("lastPrice"))
        s.bid = safe_float(it.get("maxBidPrice"))
        s.ask = safe_float(it.get("minAskPrice"))
        s.volume24 = safe_float(it.get("volume24"))
        s.mexc_ts = ts


async def mexc_ws_loop(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            log.info("Connecting to MEXC WebSocket...")
            async with websockets.connect(MEXC_WS_URL, ping_interval=None) as ws:
                STATUS["mexc_ws"] = "OK"
                push_event("INFO", "MEXC WS connected")

                await ws.send(json.dumps({"method": "sub.tickers", "param": {}}))

                last_ping = now_ts()
                async for raw in ws:
                    if stop.is_set():
                        return
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    channel = msg.get("channel")
                    if channel == "push.tickers":
                        data = msg.get("data") or []
                        if isinstance(data, list):
                            handle_tickers_msg(data)
                    elif channel == "rs.error":
                        push_event("WARN", "MEXC rs.error", extra={"msg": msg})

                    if now_ts() - last_ping > 25.0:
                        try:
                            await ws.send(json.dumps({"method": "ping"}))
                        except Exception:
                            break
                        last_ping = now_ts()

        except Exception as e:
            STATUS["mexc_ws"] = "NOT_WORKING"
            push_event("ERROR", "MEXC WS error", extra={"err": str(e)})
            await asyncio.sleep(3.0)


# =========================
# Dataset writer (per-symbol CSV)
# =========================
DATASET_Q: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=80_000)


def dataset_should_write(row: Dict[str, Any]) -> bool:
    mode = CFG.dataset_mode
    if mode == "all":
        return True
    if mode == "signals":
        return row.get("signal") == "ACTION"
    # routed
    return row.get("dexStatus") in ("OK", "NO_POOL", "NO_ROUTE", "NOT_WORKING", "INIT")


async def dataset_writer_loop(stop: asyncio.Event) -> None:
    ensure_dir(DATASET_DIR)
    ensure_dir(DATASET_RAW_DIR)
    STATUS["dataset"] = "OK"

    open_files: Dict[Tuple[str, str], Any] = {}
    writers: Dict[Tuple[str, str], csv.DictWriter] = {}
    last_flush = now_ts()

    try:
        while not stop.is_set():
            try:
                row = await asyncio.wait_for(DATASET_Q.get(), timeout=0.5)
            except asyncio.TimeoutError:
                row = None

            if row:
                day = time.strftime("%Y-%m-%d", time.localtime(row["ts"]))
                sym = row["symbol"]
                folder = DATASET_DIR / day
                ensure_dir(folder)

                fp = folder / f"{sym}.csv"
                key = (day, sym)

                if key not in open_files:
                    f = open(fp, "a", newline="", encoding="utf-8")
                    open_files[key] = f
                    fieldnames = list(row.keys())
                    w = csv.DictWriter(f, fieldnames=fieldnames)
                    writers[key] = w
                    if fp.stat().st_size == 0:
                        w.writeheader()

                writers[key].writerow(row)

            if now_ts() - last_flush >= CFG.dataset_flush_sec:
                for f in open_files.values():
                    try:
                        f.flush()
                    except Exception:
                        pass
                last_flush = now_ts()
    finally:
        for f in open_files.values():
            try:
                f.flush()
                f.close()
            except Exception:
                pass


# =========================
# DexScreener updater (pair endpoint)
# =========================
DEXSCREENER_PAIR_URL = "https://api.dexscreener.com/latest/dex/pairs/{chain}/{pair}"


async def fetch_pair(http: httpx.AsyncClient, chain: str, pair: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        url = DEXSCREENER_PAIR_URL.format(chain=chain, pair=pair)
        r = await http.get(url)
        if r.status_code != 200:
            return None, f"HTTP_{r.status_code}"
        js = r.json()
        return js, None
    except Exception:
        return None, "FETCH_FAILED"


def pick_best_pair_from_response(js: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    pairs = js.get("pairs")
    if not isinstance(pairs, list) or not pairs:
        return None
    # best by liquidity.usd then volume.h24
    def key(p: Dict[str, Any]) -> Tuple[float, float]:
        liq = safe_float((p.get("liquidity") or {}).get("usd")) or 0.0
        vol = safe_float((p.get("volume") or {}).get("h24")) or 0.0
        return (liq, vol)
    pairs.sort(key=key, reverse=True)
    return pairs[0]


async def dex_loop(
    stop: asyncio.Event,
    converter: UsdtConverter,
    status_dict: Dict[str, Any],
) -> None:
    if not CFG.dex_enabled:
        STATUS["dex"] = "DISABLED"
        push_event("INFO", "DEX disabled by config")
        return

    # build worklist once (symbols that have routes)
    worklist = [s for s in SYMBOLS.keys() if SYMBOLS[s].routes]
    if not worklist:
        STATUS["dex"] = "NOT_WORKING"
        push_event("WARN", "No DEX routes loaded (worklist empty)")
        return

    STATUS["dex"] = "OK"
    push_event("INFO", "DEX loop starting", extra={"symbols": len(worklist), "rps": CFG.dex_rps})

    http = await get_http()
    min_delay = 1.0 / max(0.5, CFG.dex_rps)

    # round-robin iterator
    i = 0
    last_full_scan_t0 = now_ts()

    while not stop.is_set():
        sym = worklist[i % len(worklist)]
        i += 1

        s = SYMBOLS.get(sym)
        if not s or not s.routes:
            await asyncio.sleep(min_delay)
            continue

        # query 1..N picked routes and select best runtime
        best: Optional[Dict[str, Any]] = None
        best_route: Optional[DexPairRoute] = None
        best_err: Optional[str] = None

        for rt in s.routes:
            js, err = await fetch_pair(http, rt.chain, rt.pair_address)
            if err:
                best_err = err
                continue

            # raw saving (optional)
            if CFG.save_dex_raw:
                try:
                    day = time.strftime("%Y-%m-%d", time.localtime(now_ts()))
                    folder = DATASET_RAW_DIR / day
                    ensure_dir(folder)
                    fp = folder / f"{sym}.jsonl"
                    with open(fp, "a", encoding="utf-8") as f:
                        f.write(json.dumps({"ts": now_ts(), "chain": rt.chain, "pair": rt.pair_address, "resp": js}, ensure_ascii=False) + "\n")
                except Exception:
                    # silent
                    pass

            p = pick_best_pair_from_response(js)
            if not p:
                best_err = "NO_PAIR"
                continue

            liq = safe_float((p.get("liquidity") or {}).get("usd")) or 0.0
            vol = safe_float((p.get("volume") or {}).get("h24")) or 0.0
            if best is None:
                best = p
                best_route = rt
            else:
                cur_liq = safe_float((best.get("liquidity") or {}).get("usd")) or 0.0
                cur_vol = safe_float((best.get("volume") or {}).get("h24")) or 0.0
                if (liq, vol) > (cur_liq, cur_vol):
                    best = p
                    best_route = rt

        # apply runtime
        ts = now_ts()
        if not best or not best_route:
            s.dex_status = "NOT_WORKING" if best_err else "NO_POOL"
            s.dex_ts = ts
            s.dex_price_usd = None
            s.dex_price_usdt = None
            s.dex_chain = None
            s.dex_chain_id = None
            s.dex_pair = None
            s.dex_pair_url = None
            s.dex_quote_symbol = None
            s.dex_liq_usd = None
            s.dex_vol24 = None
            s.dex_mode = "INIT"
            STATUS["dex"] = "NOT_WORKING"
            push_event("WARN", "DEX NOT WORKING", symbol=sym, extra={"err": best_err or "UNKNOWN"})
        else:
            price_usd = safe_float(best.get("priceUsd"))
            liq_usd = safe_float((best.get("liquidity") or {}).get("usd"))
            vol24 = safe_float((best.get("volume") or {}).get("h24"))
            quote = (best.get("quoteToken") or {}).get("symbol")

            s.dex_status = "OK"
            s.dex_ts = ts
            s.dex_price_usd = price_usd
            s.dex_price_usdt = converter.usd_to_usdt(price_usd)
            s.dex_chain = best_route.chain
            s.dex_chain_id = best_route.chain_id
            s.dex_pair = best_route.pair_address
            s.dex_pair_url = best_route.pair_url
            s.dex_quote_symbol = quote or best_route.quote_symbol
            s.dex_liq_usd = liq_usd
            s.dex_vol24 = vol24
            s.dex_mode = "USDT_RATE_MEDIAN" if status_dict.get("usdt_rate_ok") else "FALLBACK_1.0"

            STATUS["dex"] = "OK"

            # Write dataset row on each successful DEX fetch (this is the “each request” record)
            row = build_dataset_row(ts=ts, s=s, converter_snapshot=status_dict)
            if row and dataset_should_write(row):
                try:
                    DATASET_Q.put_nowait(row)
                except asyncio.QueueFull:
                    STATUS["dataset"] = "NOT_WORKING"
                    push_event("WARN", "DATASET queue full (dropping)")

        # pacing
        await asyncio.sleep(min_delay)

        # optional “heartbeat”
        if now_ts() - last_full_scan_t0 > max(10.0, CFG.dex_refresh_target_sec):
            last_full_scan_t0 = now_ts()
            push_event("INFO", "DEX scan heartbeat", extra={"symbols": len(worklist), "next": sym})


# =========================
# Analysis loop (snapshot + signals)
# =========================

LATEST_SNAPSHOT: Dict[str, Any] = {
    "ts": now_ts(),
    "brand": "Lets.Trade",
    "status": STATUS,
    "config": {},
    "symbols": [],
    "signals": [],
}


def build_dataset_row(ts: float, s: SymbolRuntime, converter_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    # MEXC mid
    mid = None
    if s.bid and s.ask and s.bid > 0 and s.ask > 0:
        mid = (s.bid + s.ask) / 2.0
    elif s.last and s.last > 0:
        mid = s.last

    # Edge calc (only if dex_price_usdt exists)
    edge = None
    sig = "NONE"
    direction = None
    dex_usdt = s.dex_price_usdt if s.dex_status == "OK" else None

    if mid and dex_usdt and mid > 0:
        edge = (dex_usdt - mid) / mid * 100.0
        abs_edge = abs(edge)
        if abs_edge >= CFG.spread_action_pct:
            sig = "ACTION"
        elif abs_edge >= CFG.spread_setup_pct:
            sig = "SETUP"
        direction = "LONG_CEX_SHORT_DEX" if edge > 0 else "SHORT_CEX_LONG_DEX"

    return {
        "ts": ts,
        "symbol": s.symbol,
        "signal": sig,
        "edgePct": edge,
        "direction": direction,
        "mexcMid": mid,
        "mexcBid": s.bid,
        "mexcAsk": s.ask,
        "mexcLast": s.last,
        "mexcVol24": s.volume24,
        "mexcAgeSec": (ts - s.mexc_ts) if s.mexc_ts else None,
        "dexStatus": s.dex_status,
        "dexChain": s.dex_chain,
        "dexChainId": s.dex_chain_id,
        "dexPair": s.dex_pair,
        "dexPairUrl": s.dex_pair_url,
        "dexQuoteSymbol": s.dex_quote_symbol,
        "dexPriceUsd": s.dex_price_usd,
        "dexPriceUsdt": s.dex_price_usdt,
        "dexLiqUsd": s.dex_liq_usd,
        "dexVol24": s.dex_vol24,
        "dexAgeSec": (ts - s.dex_ts) if s.dex_ts else None,
        "usdToUsdtRate": converter_snapshot.get("usdt_rate"),
        "usdToUsdtSource": converter_snapshot.get("usdt_rate_src"),
        "usdToUsdtOk": bool(converter_snapshot.get("usdt_rate_ok")),
        "dexPriceMode": s.dex_mode,
    }


async def analysis_loop(stop: asyncio.Event, converter_snapshot: Dict[str, Any]) -> None:
    global LATEST_SNAPSHOT
    push_event("INFO", "Analysis loop starting")

    while not stop.is_set():
        ts = now_ts()

        symbols_rows: List[Dict[str, Any]] = []
        signals: List[Dict[str, Any]] = []

        # build snapshot for every known symbol (mexc+routes union)
        for sym, s in list(SYMBOLS.items()):
            # MEXC mid
            mid = None
            if s.bid and s.ask and s.bid > 0 and s.ask > 0:
                mid = (s.bid + s.ask) / 2.0
            elif s.last and s.last > 0:
                mid = s.last
            s.mid = mid

            # calc signal
            s.edge_pct = None
            s.signal = "NONE"
            s.direction = None

            dex_usdt = s.dex_price_usdt if s.dex_status == "OK" else None
            if mid and dex_usdt and mid > 0:
                edge = (dex_usdt - mid) / mid * 100.0
                s.edge_pct = edge
                abs_edge = abs(edge)

                if abs_edge >= CFG.spread_action_pct:
                    s.signal = "ACTION"
                elif abs_edge >= CFG.spread_setup_pct:
                    s.signal = "SETUP"

                s.direction = "LONG_CEX_SHORT_DEX" if edge > 0 else "SHORT_CEX_LONG_DEX"

            symbols_rows.append(
                {
                    "symbol": sym,
                    "base": s.base,
                    "quote": s.quote,
                    "mexc": {
                        "bid": s.bid,
                        "ask": s.ask,
                        "mid": s.mid,
                        "last": s.last,
                        "volume24": s.volume24,
                        "ageSec": (ts - s.mexc_ts) if s.mexc_ts else None,
                    },
                    "dex": {
                        "status": s.dex_status,
                        "chain": s.dex_chain,
                        "chainId": s.dex_chain_id,
                        "pair": s.dex_pair,
                        "pairUrl": s.dex_pair_url,
                        "quoteSymbol": s.dex_quote_symbol,
                        "priceUsd": s.dex_price_usd if s.dex_status == "OK" else None,
                        "priceUsdt": s.dex_price_usdt if s.dex_status == "OK" else None,
                        "liqUsd": s.dex_liq_usd,
                        "vol24": s.dex_vol24,
                        "ageSec": (ts - s.dex_ts) if s.dex_ts else None,
                        "mode": s.dex_mode,
                    },
                    "edgePct": s.edge_pct,
                    "signal": s.signal,
                    "direction": s.direction,
                    "routesCount": len(s.routes),
                }
            )

            if s.signal == "ACTION":
                signals.append(symbols_rows[-1])

        # rank signals
        signals.sort(key=lambda r: abs(r.get("edgePct") or 0.0), reverse=True)

        LATEST_SNAPSHOT = {
            "ts": ts,
            "brand": "Lets.Trade",
            "status": STATUS,
            "config": {
                "spreadActionPct": CFG.spread_action_pct,
                "spreadSetupPct": CFG.spread_setup_pct,
                "cexNotionalUsdt": CFG.cex_notional_usdt,
                "dexEnabled": CFG.dex_enabled,
                "dexRps": CFG.dex_rps,
                "dexPickPerSymbol": CFG.dex_pick_per_symbol,
                "datasetMode": CFG.dataset_mode,
                "routesCsv": CFG.routes_csv,
            },
            "symbols": symbols_rows,
            "signals": signals[:2000],
        }

        await asyncio.sleep(1.0)


# =========================
# FastAPI app + routes
# =========================
STOP_EVENT = asyncio.Event()
TASKS: List[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_dir(DATASET_DIR)
    ensure_dir(DATASET_RAW_DIR)

    push_event("INFO", "Lets.Trade startup")

    # load CSV routes once
    load_routes_from_csv(CFG.routes_csv)

    # shared converter status snapshot
    converter_status: Dict[str, Any] = {}

    # background tasks
    converter = UsdtConverter(refresh_sec=CFG.usd_to_usdt_refresh_sec)
    http = await get_http()

    TASKS.append(asyncio.create_task(converter.loop(STOP_EVENT, http, push_event=push_event, status=converter_status)))

    TASKS.append(asyncio.create_task(mexc_ws_loop(STOP_EVENT)))
    TASKS.append(asyncio.create_task(dataset_writer_loop(STOP_EVENT)))
    TASKS.append(asyncio.create_task(dex_loop(STOP_EVENT, converter=converter, status_dict=converter_status)))
    TASKS.append(asyncio.create_task(analysis_loop(STOP_EVENT, converter_snapshot=converter_status)))

    try:
        yield
    finally:
        STOP_EVENT.set()
        for t in TASKS:
            try:
                t.cancel()
            except Exception:
                pass
        if HTTP is not None:
            try:
                await HTTP.aclose()
            except Exception:
                pass


app = FastAPI(title="Lets.Trade — Arbitrage Radar", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(FRONTEND_DIR / "index.html"))


@app.get("/log")
async def log_page():
    # your current UI expects /log
    return FileResponse(str(FRONTEND_DIR / "log.html"))


@app.get("/api/state")
async def api_state():
    return JSONResponse(LATEST_SNAPSHOT)


@app.get("/api/symbols")
async def api_symbols():
    return JSONResponse({"symbols": sorted(SYMBOLS.keys())})


@app.get("/api/events")
async def api_events(symbol: Optional[str] = None, limit: int = 300):
    limit = max(1, min(3000, int(limit)))
    if symbol:
        buf = SYMBOL_EVENTS.get(symbol, deque())
        arr = list(buf)[-limit:]
    else:
        arr = list(GLOBAL_EVENTS)[-limit:]
    return JSONResponse({"events": arr})


@app.get("/favicon.ico")
async def favicon():
    # serve icon.png if present; otherwise avoid noisy 404
    icon = STATIC_DIR / "icon.png"
    if icon.exists():
        return FileResponse(str(icon))
    return Response(status_code=204)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await ws.send_json({"type": "snapshot", "payload": LATEST_SNAPSHOT})
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        return
    except Exception:
        return


if __name__ == "__main__":
    import uvicorn

    # IMPORTANT: pass app instance to avoid double-import duplicate logs
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
