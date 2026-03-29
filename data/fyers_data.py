"""
data/fyers_data.py  —  Fyers Data Layer
═══════════════════════════════════════════════════════════════════════════════
  Provides 1-minute OHLCV data from Fyers for BOTH modes:

  MODE 1 (Backtest) → Historical data via REST API
    • Splits date range into CHUNK_DAYS chunks   (avoids API limits)
    • Waits API_DELAY_SEC between requests       (avoids rate limits)
    • Caches results to Parquet                  (fast reruns)

  MODE 2 (Live)     → Real-time data via WebSocket
    • Streams live ticks
    • Aggregates into 1-min OHLCV bars
    • Pushes completed bars to the strategy

  ⚡ Split → Loop → Fetch → Wait → Combine → Clean
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import time
import threading
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Callable, Optional

import pandas as pd
import numpy as np

import config
from utils.logger import get_logger

logger = get_logger(__name__)

IST = ZoneInfo(config.TIMEZONE)
UTC = ZoneInfo("UTC")


# ═══════════════════════════════════════════════════════════════════════════
#  HISTORICAL DATA  (Backtest mode)
# ═══════════════════════════════════════════════════════════════════════════

class HistoricalData:
    """
    Fetch and cache 1-minute OHLCV data from Fyers.

    Strategy
    ────────
    1. Break full date range into config.CHUNK_DAYS slices
    2. Fetch each slice via fyers.history()
    3. Sleep config.API_DELAY_SEC between requests
    4. Combine all chunks, deduplicate, sort
    5. Cache to Parquet — subsequent runs load from disk instantly
    """

    def __init__(self, client=None):
        self.client = client
        config.CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # ── Public ──────────────────────────────────────────────────────────────

    def get(
        self,
        symbol      : str = config.SYMBOL,
        start       : str = config.BACKTEST_START,
        end         : str = config.BACKTEST_END,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Returns clean 1-min OHLCV DataFrame indexed by IST DatetimeIndex.

        Columns: open, high, low, close, volume, date, ny_hhmm (ist_hhmm)
        """
        cache_file = config.CACHE_DIR / f"{symbol.replace(':','_')}_{start}_{end}_1min.parquet"

        if config.USE_CACHE and cache_file.exists() and not force_refresh:
            logger.info("📂  Loading from cache: %s", cache_file.name)
            df = pd.read_parquet(cache_file)
            return self._add_helpers(df)

        logger.info("🌐  Fetching %s  |  %s → %s", symbol, start, end)
        df = self._fetch_chunked(symbol, start, end)

        if df.empty:
            raise ValueError(f"No data returned for {symbol} ({start} → {end})")

        df = self._clean(df)

        if config.USE_CACHE:
            df.to_parquet(cache_file)
            logger.info("💾  Cached %d rows → %s", len(df), cache_file.name)

        return self._add_helpers(df)

    # ── Chunked fetcher ──────────────────────────────────────────────────────

    def _fetch_chunked(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """
        ⚡ Split → Loop → Fetch → Wait → Combine

        Fyers allows ~100 days of 1-min data per request.
        We break the range into config.CHUNK_DAYS slices.
        """
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt   = datetime.strptime(end,   "%Y-%m-%d")

        chunks   = []
        cursor   = start_dt
        chunk_n  = 0

        while cursor < end_dt:
            chunk_end = min(cursor + timedelta(days=config.CHUNK_DAYS), end_dt)
            chunk_n  += 1

            logger.info(
                "  Chunk %d: %s → %s",
                chunk_n,
                cursor.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d"),
            )

            chunk = self._fetch_single(
                symbol,
                cursor.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d"),
            )

            if chunk is not None and not chunk.empty:
                chunks.append(chunk)
                logger.debug("    → %d rows", len(chunk))
            else:
                logger.warning("    → empty chunk (holiday period or API issue)")

            cursor = chunk_end

            # ── Wait between requests (rate-limit safety) ─────────────────
            if cursor < end_dt:
                logger.debug("    Waiting %.1f s …", config.API_DELAY_SEC)
                time.sleep(config.API_DELAY_SEC)

        if not chunks:
            return pd.DataFrame()

        # ── Combine all chunks ─────────────────────────────────────────────
        combined = pd.concat(chunks, ignore_index=False)
        logger.info("✅  Total rows before cleaning: %d", len(combined))
        return combined

    def _fetch_single(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        """
        Single Fyers history API call.

        Fyers date format: "YYYY-MM-DD HH:MM:SS"
        Resolution: 1 = 1-minute
        """
        if self.client is None:
            raise RuntimeError(
                "No Fyers client provided to HistoricalData.\n"
                "Pass client=get_client() when constructing HistoricalData."
            )

        payload = {
            "symbol"     : symbol,
            "resolution" : "1",               # 1-minute candles
            "date_format": "1",               # epoch timestamps
            "range_from" : start,
            "range_to"   : end,
            "cont_flag"  : "1",
        }

        try:
            response = self.client.history(data=payload)
        except Exception as e:
            logger.error("API call failed: %s", e)
            return None

        if response.get("s") != "ok":
            logger.warning("Fyers history error: %s", response.get("message", response))
            return None

        candles = response.get("candles", [])
        if not candles:
            return None

        df = pd.DataFrame(
            candles,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )

        # Convert epoch → IST DatetimeIndex
        df.index = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert(IST)
        df = df.drop(columns=["timestamp"])
        return df

    # ── Cleaning ──────────────────────────────────────────────────────────────

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        ⚡ Remove duplicates → sort by time → drop bad candles
        """
        df = df.copy()

        # Sort chronologically
        df = df.sort_index()

        # Remove exact duplicate timestamps (overlap between chunks)
        df = df[~df.index.duplicated(keep="first")]

        # Drop bad candles
        df = df.dropna(subset=["open", "high", "low", "close"])
        df = df[df["high"] >= df["low"]]
        df = df[df["close"] > 0]
        df = df[df["volume"] >= 0]

        logger.info("✅  After cleaning: %d rows", len(df))
        return df

    def _add_helpers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add IST time helper columns used by the strategy."""
        df = df.copy()
        ist_index         = df.index if df.index.tzinfo else df.index.tz_localize(IST)
        df["ist_hour"]    = ist_index.hour
        df["ist_minute"]  = ist_index.minute
        df["ist_hhmm"]    = df["ist_hour"] * 100 + df["ist_minute"]
        df["date"]        = ist_index.date
        return df


# ═══════════════════════════════════════════════════════════════════════════
#  LIVE DATA  (Live trading mode)
# ═══════════════════════════════════════════════════════════════════════════

class LiveDataFeed:
    """
    Real-time 1-minute bar builder using Fyers WebSocket.

    Flow
    ────
    WebSocket tick → _on_tick()
        → accumulate into current 1-min bar
        → on minute boundary: emit completed bar → call on_bar_close()

    Usage
    ─────
        def my_callback(bar: pd.Series):
            # bar has: open, high, low, close, volume, ist_hhmm, ...
            strategy.on_bar(bar)

        feed = LiveDataFeed(client, on_bar_close=my_callback)
        feed.start()          # non-blocking (runs in background thread)
        ...
        feed.stop()
    """

    def __init__(
        self,
        client,
        symbol      : str = config.SYMBOL,
        on_bar_close: Optional[Callable[[pd.Series], None]] = None,
    ):
        self.client       = client
        self.symbol       = symbol
        self.on_bar_close = on_bar_close

        # Current incomplete bar being built
        self._bar_open    : Optional[float] = None
        self._bar_high    : Optional[float] = None
        self._bar_low     : Optional[float] = None
        self._bar_close   : Optional[float] = None
        self._bar_volume  : float = 0.0
        self._bar_minute  : Optional[int]   = None   # HHMM of current bar

        self._running     = False
        self._thread      : Optional[threading.Thread] = None
        self._ws          = None

        # Historical bars accumulated this session (for strategy lookback)
        self._bars        : list[pd.Series] = []

    # ── Public ────────────────────────────────────────────────────────────

    def start(self):
        """Start streaming in a background thread."""
        self._running = True
        self._thread  = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("📡  LiveDataFeed started for %s", self.symbol)

    def stop(self):
        """Stop streaming."""
        self._running = False
        if self._ws is not None:
            try:
                self._ws.close_connection()
            except Exception:
                pass
        logger.info("📡  LiveDataFeed stopped")

    def get_bars_df(self) -> pd.DataFrame:
        """Return all completed bars this session as a DataFrame."""
        if not self._bars:
            return pd.DataFrame()
        return pd.DataFrame(self._bars)

    # ── WebSocket internals ────────────────────────────────────────────────

    def _run(self):
        """Background thread — connects WebSocket and blocks on recv."""
        try:
            from fyers_apiv3.FyersWebsocket import data_ws
        except ImportError:
            logger.error("fyers-apiv3 not installed — cannot start live feed")
            return

        def _on_message(msg):
            if not self._running:
                return
            try:
                self._process_tick(msg)
            except Exception as e:
                logger.debug("Tick processing error: %s", e)

        def _on_error(err):
            logger.error("WebSocket error: %s", err)

        def _on_close():
            logger.warning("WebSocket closed")
            if self._running:
                logger.info("Reconnecting in 5 s …")
                time.sleep(5)
                self._run()   # reconnect

        def _on_open():
            logger.info("WebSocket connected")
            self._ws.subscribe(
                symbols    = [self.symbol],
                data_type  = "SymbolUpdate",
            )

        self._ws = data_ws.FyersDataSocket(
            access_token  = self.client.token,
            log_path      = str(config.LOG_DIR),
            litemode      = False,
            write_to_file = False,
            on_connect    = _on_open,
            on_message    = _on_message,
            on_error      = _on_error,
            on_close      = _on_close,
        )

        self._ws.connect()

    def _process_tick(self, msg: dict):
        """
        Process a single tick message and update the current 1-min bar.
        When the minute changes, emit the completed bar.
        """
        if not isinstance(msg, dict):
            return

        ltp = msg.get("ltp") or msg.get("last_traded_price")
        vol = msg.get("vol_traded_today") or msg.get("volume", 0)
        ts  = msg.get("timestamp") or msg.get("tt")

        if ltp is None or ts is None:
            return

        # Convert timestamp → IST datetime
        try:
            dt       = datetime.fromtimestamp(ts, tz=IST)
            hhmm     = dt.hour * 100 + dt.minute
        except Exception:
            return

        # Ignore ticks outside trading session
        if hhmm < config.SESSION_START_HHMM or hhmm >= config.SESSION_END_HHMM:
            return

        # ── New minute → emit previous bar ────────────────────────────────
        if self._bar_minute is not None and hhmm != self._bar_minute:
            self._emit_bar()

        # ── Update current bar ─────────────────────────────────────────────
        if self._bar_minute != hhmm:
            # Fresh bar
            self._bar_minute = hhmm
            self._bar_open   = ltp
            self._bar_high   = ltp
            self._bar_low    = ltp
            self._bar_volume = 0.0

        self._bar_high   = max(self._bar_high,  ltp)
        self._bar_low    = min(self._bar_low,   ltp)
        self._bar_close  = ltp
        self._bar_volume = float(vol)

    def _emit_bar(self):
        """Package completed bar as pd.Series and push to callback."""
        if self._bar_open is None:
            return

        now = datetime.now(IST)
        bar = pd.Series({
            "open"      : self._bar_open,
            "high"      : self._bar_high,
            "low"       : self._bar_low,
            "close"     : self._bar_close,
            "volume"    : self._bar_volume,
            "ist_hhmm"  : self._bar_minute,
            "ist_hour"  : self._bar_minute // 100,
            "ist_minute": self._bar_minute % 100,
            "date"      : now.date(),
        })

        self._bars.append(bar)
        logger.debug(
            "Bar closed | %04d | O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f",
            self._bar_minute,
            bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"],
        )

        if self.on_bar_close is not None:
            try:
                self.on_bar_close(bar)
            except Exception as e:
                logger.error("on_bar_close callback error: %s", e)

        # Reset bar state
        self._bar_open   = None
        self._bar_high   = None
        self._bar_low    = None
        self._bar_close  = None
        self._bar_volume = 0.0
        self._bar_minute = None
