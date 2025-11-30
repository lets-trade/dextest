# usdt_converter.py
# Lets.Trade — lightweight USD->USDT estimator using MEXC Spot stablecoin quotes
# Used to convert DexScreener priceUsd into "priceUsdt" (approx but consistent).
# Python 3.10+

from __future__ import annotations

import asyncio
import random
import statistics
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx

MEXC_SPOT_BASE = "https://api.mexc.com"
MEXC_SPOT_TICKER_PRICE = f"{MEXC_SPOT_BASE}/api/v3/ticker/price"

STABLE_SPOT_SYMBOLS = [
    "USDCUSDT",
    "DAIUSDT",
    "TUSDUSDT",
    "FDUSDUSDT",
    "USDEUSDT",
    "USDDUSDT",
    "BUSDUSDT",
    "USDPUSDT",
]


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            if v != v:  # NaN
                return None
            return v
        s = str(x).strip()
        if not s or s.lower() in {"nan", "none"}:
            return None
        return float(s)
    except Exception:
        return None


def _jitter_sleep(base_s: float) -> float:
    return base_s * (0.85 + 0.3 * random.random())


@dataclass
class UsdtRate:
    rate: float = 1.0
    source: str = "INIT"
    used: List[str] = None
    ts: float = 0.0
    ok: bool = False


class UsdtConverter:
    """
    Periodically refreshes USD->USDT using MEXC Spot stablecoin quotes.
    Thread-safe enough for asyncio single-thread usage.
    """

    def __init__(self, *, refresh_sec: float = 60.0, timeout_s: float = 8.0):
        self.refresh_sec = float(refresh_sec)
        self.timeout_s = float(timeout_s)
        self.state = UsdtRate(rate=1.0, source="INIT", used=[], ts=0.0, ok=False)

    async def _fetch_one(self, http: httpx.AsyncClient, symbol: str) -> Optional[float]:
        try:
            r = await http.get(MEXC_SPOT_TICKER_PRICE, params={"symbol": symbol}, timeout=self.timeout_s)
            if r.status_code != 200:
                return None
            js = r.json()
            return _safe_float(js.get("price"))
        except Exception:
            return None

    async def refresh_now(self, http: httpx.AsyncClient) -> UsdtRate:
        quotes: List[Tuple[str, float]] = []

        # Do sequential requests to keep payload small and avoid “Content-Length exceeds Body” edge cases.
        for sym in STABLE_SPOT_SYMBOLS:
            p = await self._fetch_one(http, sym)
            if p is None:
                continue
            # sanity window; if outside, ignore
            if 0.95 <= p <= 1.05:
                quotes.append((sym, p))

        if not quotes:
            self.state = UsdtRate(rate=1.0, source="FALLBACK_1.0", used=[], ts=time.time(), ok=False)
            return self.state

        vals = [p for _, p in quotes]
        rate = float(statistics.median(vals))
        used = [s for s, _ in quotes]
        src = "MEDIAN(" + ",".join(used) + ")"

        self.state = UsdtRate(rate=rate, source=src, used=used, ts=time.time(), ok=True)
        return self.state

    async def loop(self, stop_event: asyncio.Event, http: httpx.AsyncClient, push_event=None, status: Optional[Dict[str, Any]] = None) -> None:
        """
        Background loop. Updates status dict if provided:
          status["usdt_rate"] = rate
          status["usdt_rate_src"] = source
          status["usdt_rate_ok"] = bool
        """
        # first refresh fast
        try:
            await self.refresh_now(http)
            if push_event:
                push_event("PIPELINE", f"USD→USDT rate ready: {self.state.rate:.6f} ({self.state.source})", ok=self.state.ok)
        except Exception:
            pass

        while not stop_event.is_set():
            try:
                if status is not None:
                    status["usdt_rate"] = self.state.rate
                    status["usdt_rate_src"] = self.state.source
                    status["usdt_rate_ok"] = self.state.ok
                    status["usdt_rate_age"] = round(time.time() - self.state.ts, 3) if self.state.ts else None

                # refresh if stale
                if (time.time() - self.state.ts) >= self.refresh_sec or self.state.source in {"INIT"}:
                    await self.refresh_now(http)
                    if push_event:
                        push_event("PIPELINE", f"USD→USDT refreshed: {self.state.rate:.6f} ({self.state.source})", ok=self.state.ok)
            except Exception:
                # keep running
                if push_event:
                    push_event("PIPELINE", "USD→USDT refresh: NOT_WORKING", ok=False)
            await asyncio.sleep(_jitter_sleep(1.0))

    def usd_to_usdt(self, price_usd: Optional[float]) -> Optional[float]:
        if price_usd is None:
            return None
        return float(price_usd) * float(self.state.rate)

    def snapshot(self) -> Dict[str, Any]:
        return {
            "rate": self.state.rate,
            "source": self.state.source,
            "ok": self.state.ok,
            "ageSec": (time.time() - self.state.ts) if self.state.ts else None,
            "used": list(self.state.used or []),
        }
