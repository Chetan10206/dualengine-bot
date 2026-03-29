"""
utils/logger.py  —  Centralized Logger
═══════════════════════════════════════════════════════════════════════════════
  Every module imports get_logger() from here.
  Logs go to:
    • Console  (coloured, always)
    • logs/bot_YYYYMMDD.log  (plain text, if LOG_TO_FILE=True in config)
═══════════════════════════════════════════════════════════════════════════════
"""

import logging
import sys
from datetime import datetime
from pathlib import Path


# ── ANSI colour codes for console output ────────────────────────────────────
_COLOURS = {
    "DEBUG"   : "\033[36m",    # cyan
    "INFO"    : "\033[32m",    # green
    "WARNING" : "\033[33m",    # yellow
    "ERROR"   : "\033[31m",    # red
    "CRITICAL": "\033[35m",    # magenta
    "RESET"   : "\033[0m",
    "BOLD"    : "\033[1m",
    "DIM"     : "\033[2m",
}


class _ColourFormatter(logging.Formatter):
    """Console formatter with level-based colours."""

    FMT = "%(asctime)s  {bold}%(levelname)-8s{reset}  {dim}%(name)-25s{reset}  %(message)s"
    TIME_FMT = "%H:%M:%S"

    def format(self, record: logging.LogRecord) -> str:
        col   = _COLOURS.get(record.levelname, "")
        reset = _COLOURS["RESET"]
        bold  = _COLOURS["BOLD"]
        dim   = _COLOURS["DIM"]

        fmt = (
            f"%(asctime)s  {col}{bold}%(levelname)-8s{reset}  "
            f"{dim}%(name)-25s{reset}  {col}%(message)s{reset}"
        )
        formatter = logging.Formatter(fmt, datefmt=self.TIME_FMT)
        return formatter.format(record)


class _PlainFormatter(logging.Formatter):
    """Plain formatter for file output (no ANSI codes)."""
    FMT      = "%(asctime)s  %(levelname)-8s  %(name)-25s  %(message)s"
    TIME_FMT = "%Y-%m-%d %H:%M:%S"

    def __init__(self):
        super().__init__(self.FMT, datefmt=self.TIME_FMT)


# ── Module-level state ──────────────────────────────────────────────────────
_configured = False


def setup(log_level: str = "INFO", log_to_file: bool = True, log_dir: Path = None):
    """
    Call ONCE from main.py before anything else.
    Subsequent get_logger() calls just inherit this config.
    """
    global _configured
    if _configured:
        return
    _configured = True

    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # ── Console handler ──────────────────────────────────────────────────
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(_ColourFormatter())
    root.addHandler(ch)

    # ── File handler ─────────────────────────────────────────────────────
    if log_to_file and log_dir is not None:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        today    = datetime.now().strftime("%Y%m%d")
        log_file = log_dir / f"bot_{today}.log"
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(_PlainFormatter())
        root.addHandler(fh)
        logging.getLogger(__name__).info("Log file → %s", log_file)

    # ── Silence noisy third-party loggers ────────────────────────────────
    for noisy in ("urllib3", "requests", "websocket", "fyers_apiv3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Returns a named logger.  Call at the top of every module:

      from utils.logger import get_logger
      logger = get_logger(__name__)
    """
    return logging.getLogger(name)


# ── Trade-specific helpers ───────────────────────────────────────────────────

def log_trade_entry(logger, trade):
    logger.info(
        "▶  ENTRY  | %-6s | %-12s | entry=%-10.2f | SL=%-10.2f | TP=%-10.2f",
        trade.direction.upper(),
        trade.entry_type,
        trade.entry_price,
        trade.stop_loss,
        trade.take_profit,
    )


def log_trade_exit(logger, trade):
    icon = "✅" if trade.pnl_pts >= 0 else "❌"
    logger.info(
        "%s EXIT   | %-6s | %-6s | exit=%-10.2f | PnL=%-+8.2f pts | R=%-+5.2f",
        icon,
        trade.direction.upper(),
        trade.exit_reason.upper(),
        trade.exit_price,
        trade.pnl_pts,
        trade.r_multiple,
    )


def log_separator(logger, label: str = ""):
    logger.info("─" * 60 + (f"  {label}" if label else ""))
