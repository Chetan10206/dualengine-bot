"""
The ONLY file you need to edit before running the bot.
  Everything else reads from here.

  MODE 1 → Backtest   (historical data, simulated trades)
  MODE 2 → Live       (real Fyers orders, real money)
═══════════════════════════════════════════════════════════════════════════════
"""

from pathlib import Path
from dotenv import load_dotenv
import os

# ── Load .env ───────────────────────────────────────────────────────────────
_env = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_env, override=False)

ROOT = Path(__file__).parent


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  1.  MODE  ←  CHANGE THIS FIRST                                         ║
# ╚══════════════════════════════════════════════════════════════════════════╝

MODE = "backtest"   # "backtest"  |  "live"


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  2.  TRADING INSTRUMENT                                                  ║
# ╚══════════════════════════════════════════════════════════════════════════╝

# Fyers symbol format:  "EXCHANGE:TICKER-TYPE"
# Examples:
#   NSE equity   → "NSE:RELIANCE-EQ"
#   NIFTY 50 idx → "NSE:NIFTY50-INDEX"
#   BankNifty fut→ "NSE:BANKNIFTY25JANFUT"
SYMBOL      = "NSE:NIFTY50-INDEX"
EXCHANGE    = "NSE"
TIMEFRAME   = 1          # minutes (1 = 1-min chart)


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  3.  SESSION TIMES  (IST)                                                ║
# ╚══════════════════════════════════════════════════════════════════════════╝

SESSION_START_HHMM   = 915    # 9:15 AM IST  (NSE open)
RANGE_START_HHMM     = 915    # Opening range start
RANGE_END_HHMM       = 920    # Opening range end  (first 5-min candle = 9:15–9:19)
TRADING_START_HHMM   = 920    # Begin hunting entries after range is set
SESSION_END_HHMM     = 1525   # 3:25 PM IST  (force-close all positions)
TIMEZONE             = "Asia/Kolkata"


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  4.  STRATEGY PARAMETERS                                                 ║
# ╚══════════════════════════════════════════════════════════════════════════╝

RR_RATIO          = 2.0    # Risk : Reward  (TP = risk × 2)
RETEST_TOLERANCE  = 10.0   # % of opening range height used as retest band
EOD_EXIT_HHMM     = 1525   # Force-close time  (same as SESSION_END_HHMM)


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  5.  BACKTEST SETTINGS                                                   ║
# ╚══════════════════════════════════════════════════════════════════════════╝

BACKTEST_START   = "2024-01-01"   # YYYY-MM-DD
BACKTEST_END     = "2025-01-01"   # YYYY-MM-DD  (1 full year)
INITIAL_CAPITAL  = 100_000        # ₹ starting balance
RISK_PER_TRADE   = 1.0            # % of equity risked per trade


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  6.  LIVE TRADING SETTINGS                                               ║
# ╚══════════════════════════════════════════════════════════════════════════╝

LIVE_QUANTITY    = 1          # Number of lots / shares per trade
PRODUCT_TYPE     = "INTRADAY" # "INTRADAY" | "CNC" | "MARGIN"
ORDER_TYPE       = "MARKET"   # "MARKET"  | "LIMIT"
DRY_RUN          = True       # True  = log orders but DO NOT send to Fyers
                               # False = REAL orders  ⚠ real money!


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  7.  DATA FETCHING                                                       ║
# ╚══════════════════════════════════════════════════════════════════════════╝

CHUNK_DAYS       = 90         # Days per Fyers API request (max ~100 for 1-min)
API_DELAY_SEC    = 1.0        # Pause between chunk requests (rate-limit safety)
USE_CACHE        = True       # Cache fetched data to disk (Parquet)
CACHE_DIR        = ROOT / "results" / "cache"


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  8.  FYERS CREDENTIALS  (read from .env)                                 ║
# ╚══════════════════════════════════════════════════════════════════════════╝

FYERS_CLIENT_ID    = os.getenv("FYERS_CLIENT_ID",    "")   # APP-XXXXXXXXXX
FYERS_SECRET_KEY   = os.getenv("FYERS_SECRET_KEY",   "")
FYERS_REDIRECT_URI = os.getenv("FYERS_REDIRECT_URI", "https://trade.fyers.in/api-login/redirect-uri/index.html")
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN", "")   # refreshed daily


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  9.  LOGGING                                                             ║
# ╚══════════════════════════════════════════════════════════════════════════╝

LOG_LEVEL        = "INFO"     # "DEBUG" | "INFO" | "WARNING" | "ERROR"
LOG_DIR          = ROOT / "logs"
LOG_TO_FILE      = True


# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  10. PATHS                                                               ║
# ╚══════════════════════════════════════════════════════════════════════════╝

RESULTS_DIR      = ROOT / "results"
TOKEN_FILE       = ROOT / ".fyers_token"   # auto-saved access token


# ── Sanity check on startup ─────────────────────────────────────────────────
def validate():
    errors = []
    if MODE not in ("backtest", "live"):
        errors.append(f"MODE must be 'backtest' or 'live', got: {MODE!r}")
    if MODE == "live" and not DRY_RUN:
        if not FYERS_CLIENT_ID or not FYERS_SECRET_KEY:
            errors.append("FYERS_CLIENT_ID / FYERS_SECRET_KEY missing in .env (required for live trading)")
        if not FYERS_ACCESS_TOKEN:
            errors.append("FYERS_ACCESS_TOKEN missing — run: python main.py login")
    if CHUNK_DAYS > 100:
        errors.append("CHUNK_DAYS should be ≤ 100 for 1-min Fyers data")
    if errors:
        raise EnvironmentError(
            "\n\n  ✗  Config errors:\n" +
            "\n".join(f"    • {e}" for e in errors) +
            "\n"
        )


if __name__ == "__main__":
    validate()
    print("✅  Config OK")
    print(f"  MODE      : {MODE}")
    print(f"  SYMBOL    : {SYMBOL}")
    print(f"  DRY_RUN   : {DRY_RUN}")
    print(f"  CAPITAL   : ₹{INITIAL_CAPITAL:,}")
    print(f"  BACKTEST  : {BACKTEST_START} → {BACKTEST_END}")
