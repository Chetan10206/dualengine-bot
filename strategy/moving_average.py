"""
strategy/moving_average.py  —  NY Open FVG Breakout Strategy
═══════════════════════════════════════════════════════════════════════════════
  Pure strategy logic. Zero API calls. Zero broker imports.
  Works identically for BOTH backtest and live modes.

  Adapted for Indian markets:
    Opening Range  → 9:15 – 9:19 AM IST  (first 5-min candle)
    Trading begins → 9:20 AM IST onward
    EOD close      → 3:25 PM IST

  Rules (same as Pine Script)
  ───────────────────────────
  1. Opening Range  : high/low of first 5-min candle
  2. Breakout       : body-close outside OR (wick alone ≠ breakout)
  3a. Primary Entry : FVG confirmed on 3rd candle after breakout
  3b. Retest Entry  : no FVG → retest OR level → new FVG
  4. Stop Loss      : breakout candle's low (long) or high (short)
  5. Take Profit    : 2 × risk  (config.RR_RATIO)
  One trade per session.
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd

import config
from utils.logger import get_logger, log_trade_entry, log_trade_exit

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
#  TRADE RECORD
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Trade:
    date         : date
    direction    : str          # "long" | "short"
    entry_type   : str          # "primary_fvg" | "retest_fvg"
    entry_time   : object       # pd.Timestamp
    entry_price  : float
    stop_loss    : float
    take_profit  : float
    exit_time    : Optional[object] = None
    exit_price   : Optional[float]  = None
    exit_reason  : str = ""         # "tp" | "sl" | "eod"
    pnl_pts      : float = 0.0
    pnl_pct      : float = 0.0
    risk_pts     : float = 0.0
    r_multiple   : float = 0.0

    def to_dict(self) -> dict:
        return {k: round(v, 4) if isinstance(v, float) else v
                for k, v in self.__dict__.items()}


# ═══════════════════════════════════════════════════════════════════════════
#  DAILY STATE MACHINE
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class _DayState:
    or_high    : float = np.nan
    or_low     : float = np.nan
    or_set     : bool  = False
    or_hi_run  : float = np.nan
    or_lo_run  : float = np.nan

    bull_break : bool  = False
    bear_break : bool  = False
    bk_high    : float = np.nan
    bk_low     : float = np.nan
    bk_bar_idx : int   = -1

    fvg_wait   : bool  = False
    rt_wait    : bool  = False
    rt_ready   : bool  = False
    traded     : bool  = False

    def reset(self):
        for f in self.__dataclass_fields__:
            setattr(self, f, self.__dataclass_fields__[f].default)


# ═══════════════════════════════════════════════════════════════════════════
#  STRATEGY ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class NYOpenFVGStrategy:
    """
    Dual-mode strategy engine.

    Backtest mode  → call run(df)         : processes full DataFrame at once
    Live mode      → call on_bar(bar, i)  : called on each new completed bar
    """

    def __init__(
        self,
        rr_ratio      : float = config.RR_RATIO,
        rt_tolerance  : float = config.RETEST_TOLERANCE,
        eod_exit_hhmm : int   = config.EOD_EXIT_HHMM,
        range_start   : int   = config.RANGE_START_HHMM,
        range_end     : int   = config.RANGE_END_HHMM,
        trading_start : int   = config.TRADING_START_HHMM,
    ):
        self.rr           = rr_ratio
        self.rt_tol_pct   = rt_tolerance / 100.0
        self.eod_hhmm     = eod_exit_hhmm
        self.range_start  = range_start
        self.range_end    = range_end
        self.trading_start= trading_start

        # Live-mode state
        self._state       = _DayState()
        self._prev_date   : Optional[date] = None
        self._active_trade: Optional[Trade] = None
        self._all_bars    : list[dict]      = []   # growing bar history for FVG lookback
        self._bar_counter : int = 0

        # Signals to emit (live mode)
        self.pending_entry  : Optional[Trade] = None
        self.pending_exit   : Optional[Trade] = None

    # ══════════════════════════════════════════════════════════════════════
    #  BACKTEST MODE  (batch)
    # ══════════════════════════════════════════════════════════════════════

    def run(self, df: pd.DataFrame) -> list[Trade]:
        """
        Iterate bar-by-bar over historical DataFrame.
        Returns list of all completed Trade objects.
        """
        trades : list[Trade] = []
        state   = _DayState()
        bars    = df.reset_index(drop=False)
        active  : Optional[Trade] = None
        prev_date: Optional[date] = None

        for i, row in bars.iterrows():
            d    = row["date"]
            hhmm = row["ist_hhmm"]

            # ── Day reset ─────────────────────────────────────────────────
            if d != prev_date:
                if active is not None:
                    active = self._close_trade(active, bars.iloc[i - 1], "eod")
                    log_trade_exit(logger, active)
                    trades.append(active)
                    active = None
                state.reset()
                prev_date = d
                logger.debug("─── New day: %s ───", d)

            # ── Manage open position ──────────────────────────────────────
            if active is not None:
                result = self._check_exit(active, row, hhmm)
                if result:
                    log_trade_exit(logger, result)
                    trades.append(result)
                    active = None
                continue

            # ── Process bar ───────────────────────────────────────────────
            entry = self._process_bar(row, i, bars, state)
            if entry:
                active = entry
                log_trade_entry(logger, active)

        # Close lingering trade
        if active is not None:
            active = self._close_trade(active, bars.iloc[-1], "eod")
            log_trade_exit(logger, active)
            trades.append(active)

        return trades

    # ══════════════════════════════════════════════════════════════════════
    #  LIVE MODE  (bar-by-bar callback)
    # ══════════════════════════════════════════════════════════════════════

    def on_bar(self, bar: pd.Series) -> dict:
        """
        Called by LiveDataFeed every time a 1-min bar closes.

        Returns a signal dict:
          {
            "action"  : "entry" | "exit" | None,
            "trade"   : Trade object,
            "direction": "long" | "short",
            ...
          }
        """
        d    = bar["date"]
        hhmm = bar["ist_hhmm"]

        # Day reset
        if d != self._prev_date:
            if self._active_trade is not None:
                self._active_trade = self._close_trade(
                    self._active_trade, bar, "eod"
                )
                log_trade_exit(logger, self._active_trade)
                signal = {"action": "exit", "trade": self._active_trade,
                          "reason": "eod"}
                self._active_trade = None
                self._state.reset()
                self._all_bars.clear()
                self._bar_counter = 0
                self._prev_date = d
                return signal

            self._state.reset()
            self._all_bars.clear()
            self._bar_counter = 0
            self._prev_date = d

        # Store bar for lookback
        self._all_bars.append(bar.to_dict())
        i = self._bar_counter
        self._bar_counter += 1

        # Check exit on active trade
        if self._active_trade is not None:
            # Convert bar to row-like for _check_exit
            result = self._check_exit_bar(self._active_trade, bar, hhmm)
            if result:
                log_trade_exit(logger, result)
                self._active_trade = None
                return {"action": "exit", "trade": result, "reason": result.exit_reason}
            return {"action": None}

        # Build a minimal bars accessor for FVG lookback
        bars_acc = _BarsAccessor(self._all_bars)

        # Try to generate entry
        entry = self._process_bar(bar, i, bars_acc, self._state)
        if entry:
            self._active_trade = entry
            log_trade_entry(logger, entry)
            return {
                "action"    : "entry",
                "trade"     : entry,
                "direction" : entry.direction,
                "entry_price": entry.entry_price,
                "stop_loss" : entry.stop_loss,
                "take_profit": entry.take_profit,
            }

        return {"action": None}

    # ══════════════════════════════════════════════════════════════════════
    #  CORE BAR PROCESSING  (shared by both modes)
    # ══════════════════════════════════════════════════════════════════════

    def _process_bar(self, row, i: int, bars, state: _DayState) -> Optional[Trade]:
        """
        Run one bar through all strategy phases.
        Returns a Trade if an entry is triggered, else None.
        """
        hhmm = row["ist_hhmm"]
        d    = row["date"]

        # ── PHASE 0: Build Opening Range ─────────────────────────────────
        if self.range_start <= hhmm < self.range_end:
            if np.isnan(state.or_hi_run):
                state.or_hi_run = row["high"]
                state.or_lo_run = row["low"]
            else:
                state.or_hi_run = max(state.or_hi_run, row["high"])
                state.or_lo_run = min(state.or_lo_run, row["low"])
            return None

        # Lock range when trading starts
        if not state.or_set:
            if np.isnan(state.or_hi_run):
                return None
            state.or_high = state.or_hi_run
            state.or_low  = state.or_lo_run
            state.or_set  = True
            logger.debug("%s  OR locked  H=%.2f  L=%.2f", d, state.or_high, state.or_low)

        # Outside trading hours
        if hhmm < self.trading_start or hhmm >= self.eod_hhmm:
            return None

        # ── PHASE 1: Detect breakout candle ───────────────────────────────
        if state.or_set and not state.bull_break and not state.bear_break:
            if row["close"] > state.or_high:
                state.bull_break = True
                state.bk_high    = row["high"]
                state.bk_low     = row["low"]
                state.bk_bar_idx = i
                state.fvg_wait   = True
                logger.debug("%s  BULL breakout  close=%.2f", d, row["close"])

            elif row["close"] < state.or_low:
                state.bear_break = True
                state.bk_high    = row["high"]
                state.bk_low     = row["low"]
                state.bk_bar_idx = i
                state.fvg_wait   = True
                logger.debug("%s  BEAR breakout  close=%.2f", d, row["close"])

        # ── PHASE 2: Check FVG on 3rd candle (bkBar + 2) ─────────────────
        if state.fvg_wait and i == state.bk_bar_idx + 2:
            state.fvg_wait = False

            c1 = bars.iloc(state.bk_bar_idx)
            c3_high = row["high"]
            c3_low  = row["low"]

            if state.bull_break and c3_low > c1["high"]:
                trade = self._open_trade(row, d, "long", "primary_fvg", state)
                state.traded = True
                return trade

            elif state.bear_break and c3_high < c1["low"]:
                trade = self._open_trade(row, d, "short", "primary_fvg", state)
                state.traded = True
                return trade

            else:
                state.rt_wait = True    # no FVG → watch for retest

        # ── PHASE 3: Retest detection ─────────────────────────────────────
        if state.rt_wait and not state.rt_ready and not state.traded:
            tol = (state.or_high - state.or_low) * self.rt_tol_pct
            if state.bull_break and row["low"] <= state.or_high + tol:
                state.rt_ready = True
                logger.debug("%s  Retest of OR HIGH detected", d)
            elif state.bear_break and row["high"] >= state.or_low - tol:
                state.rt_ready = True
                logger.debug("%s  Retest of OR LOW detected", d)

        # ── PHASE 4: Retest FVG entry ─────────────────────────────────────
        if state.rt_wait and state.rt_ready and not state.traded and i >= 2:
            c1_rt = bars.iloc(i - 2)
            c3_high_rt = row["high"]
            c3_low_rt  = row["low"]

            if state.bull_break:
                near_or = c1_rt["high"] >= state.or_high * 0.9985
                if c3_low_rt > c1_rt["high"] and near_or:
                    trade = self._open_trade(row, d, "long", "retest_fvg", state)
                    state.traded = True
                    return trade

            elif state.bear_break:
                near_or = c1_rt["low"] <= state.or_low * 1.0015
                if c3_high_rt < c1_rt["low"] and near_or:
                    trade = self._open_trade(row, d, "short", "retest_fvg", state)
                    state.traded = True
                    return trade

        return None

    # ══════════════════════════════════════════════════════════════════════
    #  TRADE HELPERS
    # ══════════════════════════════════════════════════════════════════════

    def _open_trade(self, row, d, direction, entry_type, state) -> Trade:
        ep = row["close"]
        if direction == "long":
            sl = state.bk_low
            tp = ep + self.rr * (ep - sl)
        else:
            sl = state.bk_high
            tp = ep - self.rr * (sl - ep)

        return Trade(
            date        = d,
            direction   = direction,
            entry_type  = entry_type,
            entry_time  = row.get("index", row.name) if hasattr(row, "name") else None,
            entry_price = ep,
            stop_loss   = sl,
            take_profit = tp,
            risk_pts    = abs(ep - sl),
        )

    def _check_exit(self, trade: Trade, row, hhmm: int) -> Optional[Trade]:
        """For backtest mode — row is a DataFrame Series."""
        bar_high = row["high"]
        bar_low  = row["low"]

        if trade.direction == "long":
            sl_hit = bar_low  <= trade.stop_loss
            tp_hit = bar_high >= trade.take_profit
        else:
            sl_hit = bar_high >= trade.stop_loss
            tp_hit = bar_low  <= trade.take_profit

        if sl_hit and tp_hit:
            sl_hit, tp_hit = True, False   # conservative: SL before TP

        if sl_hit:
            return self._close_trade(trade, row, "sl")
        if tp_hit:
            return self._close_trade(trade, row, "tp")
        if hhmm >= self.eod_hhmm:
            return self._close_trade(trade, row, "eod")
        return None

    def _check_exit_bar(self, trade: Trade, bar: pd.Series, hhmm: int) -> Optional[Trade]:
        """For live mode — bar is a pd.Series from LiveDataFeed."""
        return self._check_exit(trade, bar, hhmm)

    @staticmethod
    def _close_trade(trade: Trade, row, reason: str) -> Trade:
        exit_px = (
            trade.take_profit if reason == "tp" else
            trade.stop_loss   if reason == "sl" else
            row["close"]
        )
        trade.exit_time   = row.get("index", getattr(row, "name", None))
        trade.exit_price  = exit_px
        trade.exit_reason = reason

        if trade.direction == "long":
            trade.pnl_pts = exit_px - trade.entry_price
        else:
            trade.pnl_pts = trade.entry_price - exit_px

        trade.pnl_pct    = trade.pnl_pts / trade.entry_price * 100
        trade.r_multiple = trade.pnl_pts / trade.risk_pts if trade.risk_pts else 0.0
        return trade


# ─── Accessor shim so live mode bars list works like backtest DataFrame ──────

class _BarsAccessor:
    """Minimal iloc-compatible wrapper around a list of bar dicts."""
    def __init__(self, bars: list[dict]):
        self._bars = bars

    def iloc(self, idx: int) -> dict:
        if idx < 0 or idx >= len(self._bars):
            return {"high": np.nan, "low": np.nan, "close": np.nan}
        return self._bars[idx]


# ── Helper to convert trade list → DataFrame ────────────────────────────────

def trades_to_df(trades: list[Trade]) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame()
    return pd.DataFrame([t.to_dict() for t in trades])
