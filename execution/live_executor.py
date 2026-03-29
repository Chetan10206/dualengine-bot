"""
execution/live_executor.py  —  Live Order Executor
═══════════════════════════════════════════════════════════════════════════════
  Bridges the strategy's signals to real Fyers orders.

  The strategy emits:
    {"action": "entry", "direction": "long", "stop_loss": X, ...}
    {"action": "exit",  "reason": "tp" / "sl" / "eod"}

  The executor translates these into Fyers BUY / SELL orders.

  Key features
  ────────────
  • DRY_RUN mode  → logs orders, never touches real money
  • Auto EOD exit → closes position at config.EOD_EXIT_HHMM
  • Order tracking → remembers open order IDs for cancellation
  • Heartbeat log → every minute prints position status
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import config
from broker.fyers import place_order, cancel_all_orders, get_ltp
from strategy.moving_average import Trade
from utils.logger import get_logger, log_trade_entry, log_trade_exit, log_separator

logger = get_logger(__name__)
IST    = ZoneInfo(config.TIMEZONE)


# ═══════════════════════════════════════════════════════════════════════════
#  LIVE EXECUTOR
# ═══════════════════════════════════════════════════════════════════════════

class LiveExecutor:
    """
    Receives signals from NYOpenFVGStrategy.on_bar() and places Fyers orders.

    Usage
    ─────
        executor = LiveExecutor(client)
        signal   = strategy.on_bar(bar)     # called by LiveDataFeed callback
        executor.handle(signal)
    """

    def __init__(self, client, dry_run: bool = config.DRY_RUN):
        self.client      = client
        self.dry_run     = dry_run
        self.symbol      = config.SYMBOL
        self.qty         = config.LIVE_QUANTITY

        # Track current position
        self._position   : Optional[str]  = None   # "long" | "short" | None
        self._open_trade : Optional[Trade] = None
        self._entry_order_id : Optional[str] = None

        # Session P&L tracker
        self._session_pnl   : float = 0.0
        self._session_trades: int   = 0

        if dry_run:
            logger.warning("⚠️  DRY RUN MODE — orders will NOT be sent to Fyers")
        else:
            logger.warning("🔴  LIVE MODE — REAL ORDERS will be placed!")

    # ── Public ─────────────────────────────────────────────────────────────

    def handle(self, signal: dict) -> None:
        """
        Main dispatcher.  Call this with every signal dict from strategy.on_bar().

        Signal shapes:
          {"action": None}
          {"action": "entry", "trade": Trade, "direction": "long", ...}
          {"action": "exit",  "trade": Trade, "reason": "tp"|"sl"|"eod"}
        """
        action = signal.get("action")

        if action == "entry":
            self._handle_entry(signal)
        elif action == "exit":
            self._handle_exit(signal)

    def end_of_day(self) -> None:
        """
        Force-close all open positions and cancel pending orders.
        Call this at config.EOD_EXIT_HHMM.
        """
        log_separator(logger, "EOD CLEANUP")

        if self._position is not None:
            logger.info("Closing open %s position …", self._position)
            side = "SELL" if self._position == "long" else "BUY"
            self._send_order(side, tag="eod_close")
            self._position   = None
            self._open_trade = None

        cancel_all_orders(self.client, dry_run=self.dry_run)
        self._print_session_summary()

    def heartbeat(self) -> None:
        """Log current position status (call every minute from main loop)."""
        now  = datetime.now(IST)
        hhmm = now.hour * 100 + now.minute

        if self._position is None:
            logger.info("💤  No position | %04d IST", hhmm)
            return

        ltp = get_ltp(self.client, self.symbol)
        if ltp and self._open_trade:
            t   = self._open_trade
            upnl = (ltp - t.entry_price) if t.direction == "long" else \
                   (t.entry_price - ltp)
            logger.info(
                "📊  %s | LTP=%.2f | Entry=%.2f | SL=%.2f | TP=%.2f | uPnL=%.2f",
                self._position.upper(), ltp,
                t.entry_price, t.stop_loss, t.take_profit, upnl,
            )

    # ── Entry ───────────────────────────────────────────────────────────────

    def _handle_entry(self, signal: dict) -> None:
        if self._position is not None:
            logger.warning("Already in a %s position — ignoring entry signal", self._position)
            return

        trade     = signal["trade"]
        direction = signal["direction"]

        logger.info("━" * 50)
        logger.info("🔔  ENTRY SIGNAL  →  %s", direction.upper())
        logger.info("    Entry: %.2f  |  SL: %.2f  |  TP: %.2f",
                    trade.entry_price, trade.stop_loss, trade.take_profit)

        # Send market order
        side = "BUY" if direction == "long" else "SELL"
        resp = self._send_order(side, tag=f"fvg_{trade.entry_type}")

        if resp.get("s") == "ok":
            self._position       = direction
            self._open_trade     = trade
            self._entry_order_id = resp.get("id")
            self._session_trades += 1

            # In live mode, place bracket / SL order immediately
            if not self.dry_run:
                self._place_exit_orders(trade)
        else:
            logger.error("❌  Entry order failed: %s", resp)

    # ── Exit ────────────────────────────────────────────────────────────────

    def _handle_exit(self, signal: dict) -> None:
        if self._position is None:
            return

        trade  = signal["trade"]
        reason = signal.get("reason", "unknown")

        logger.info("━" * 50)
        logger.info("🔔  EXIT SIGNAL  →  %s  |  Reason: %s", self._position.upper(), reason)

        # Close position
        side = "SELL" if self._position == "long" else "BUY"
        resp = self._send_order(side, tag=f"exit_{reason}")

        if resp.get("s") == "ok":
            # Cancel any remaining SL/TP bracket orders
            cancel_all_orders(self.client, dry_run=self.dry_run)

            pnl = trade.pnl_pts if trade else 0.0
            self._session_pnl += pnl
            log_trade_exit(logger, trade)

            self._position       = None
            self._open_trade     = None
            self._entry_order_id = None
        else:
            logger.error("❌  Exit order failed: %s", resp)

    def _place_exit_orders(self, trade: Trade) -> None:
        """
        For live trading: place SL and TP limit orders immediately after entry.
        These act as a bracket so the exit fires automatically.
        """
        # Stop-loss order
        sl_side = "SELL" if trade.direction == "long" else "BUY"
        self._send_order(
            sl_side,
            order_type  = "STOP",
            stop_price  = round(trade.stop_loss, 2),
            tag         = "sl_bracket",
        )

        # Take-profit limit order
        tp_side = "SELL" if trade.direction == "long" else "BUY"
        self._send_order(
            tp_side,
            order_type  = "LIMIT",
            limit_price = round(trade.take_profit, 2),
            tag         = "tp_bracket",
        )

    # ── Order sender ────────────────────────────────────────────────────────

    def _send_order(
        self,
        side        : str,
        order_type  : str   = config.ORDER_TYPE,
        limit_price : float = 0,
        stop_price  : float = 0,
        tag         : str   = "fvg_bot",
    ) -> dict:
        return place_order(
            client       = self.client,
            symbol       = self.symbol,
            qty          = self.qty,
            side         = side,
            order_type   = order_type,
            limit_price  = limit_price,
            stop_price   = stop_price,
            product_type = config.PRODUCT_TYPE,
            tag          = tag,
            dry_run      = self.dry_run,
        )

    # ── Summary ─────────────────────────────────────────────────────────────

    def _print_session_summary(self) -> None:
        log_separator(logger, "SESSION SUMMARY")
        logger.info("  Trades taken  : %d", self._session_trades)
        logger.info("  Session P&L   : %.2f pts", self._session_pnl)
        if self.dry_run:
            logger.info("  (DRY RUN — no real orders were placed)")
