"""
main.py  —  Main Control  (The Brain)
═══════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────┐
  │  config.py  →  MODE = "backtest"  OR  MODE = "live"     │
  └─────────────────────────────────────────────────────────┘
             ↓                              ↓
  ┌─────────────────────┐       ┌─────────────────────────┐
  │   MODE 1: BACKTEST  │       │   MODE 2: LIVE TRADING  │
  │                     │       │                         │
  │  fyers_data.py      │       │  broker/fyers.py        │
  │  → HistoricalData   │       │  → get_client()         │
  │                     │       │                         │
  │  strategy/          │       │  fyers_data.py          │
  │  → run(df)          │       │  → LiveDataFeed         │
  │                     │       │                         │
  │  backtest_executor  │       │  strategy/              │
  │  → metrics, report  │       │  → on_bar(bar)          │
  └─────────────────────┘       │                         │
                                │  live_executor          │
                                │  → place Fyers orders   │
                                └─────────────────────────┘

  CLI Commands
  ────────────
  python main.py                  # run in mode set in config.py
  python main.py --mode backtest  # force backtest
  python main.py --mode live      # force live
  python main.py login            # Fyers OAuth2 login (run once per day)
  python main.py check            # validate config + test Fyers connection
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

# ── Logger must be set up before any other import ───────────────────────────
import config
from utils.logger import setup, get_logger, log_separator

setup(
    log_level  = config.LOG_LEVEL,
    log_to_file= config.LOG_TO_FILE,
    log_dir    = config.LOG_DIR,
)
logger = get_logger("main")

IST = ZoneInfo(config.TIMEZONE)


# ═══════════════════════════════════════════════════════════════════════════
#  MODE 1 — BACKTEST
# ═══════════════════════════════════════════════════════════════════════════

def run_backtest():
    log_separator(logger, "MODE 1 — BACKTEST")
    logger.info("Symbol   : %s", config.SYMBOL)
    logger.info("Period   : %s → %s", config.BACKTEST_START, config.BACKTEST_END)
    logger.info("Capital  : ₹%s", f"{config.INITIAL_CAPITAL:,}")
    logger.info("Risk/trade: %.1f%%", config.RISK_PER_TRADE)

    # 1. Connect to Fyers (needed even for historical data)
    from broker.fyers import get_client
    client = get_client()

    # 2. Fetch historical data (chunked, cached)
    from data.fyers_data import HistoricalData
    fetcher = HistoricalData(client=client)
    df      = fetcher.get(
        symbol = config.SYMBOL,
        start  = config.BACKTEST_START,
        end    = config.BACKTEST_END,
    )
    logger.info("Data loaded: %d rows  |  %s → %s",
                len(df), df.index[0].date(), df.index[-1].date())

    # 3. Run strategy (pure logic, no API)
    from strategy.moving_average import NYOpenFVGStrategy
    strategy = NYOpenFVGStrategy()
    trades   = strategy.run(df)
    logger.info("Strategy finished: %d trades generated", len(trades))

    # 4. Simulate execution + compute metrics
    from execution.backtest_executor import BacktestExecutor
    executor = BacktestExecutor()
    results  = executor.run(trades)
    executor.print_summary(results)
    executor.save(results)

    return results


# ═══════════════════════════════════════════════════════════════════════════
#  MODE 2 — LIVE TRADING
# ═══════════════════════════════════════════════════════════════════════════

def run_live():
    log_separator(logger, "MODE 2 — LIVE TRADING")
    logger.info("Symbol   : %s", config.SYMBOL)
    logger.info("Quantity : %d", config.LIVE_QUANTITY)
    logger.info("DRY RUN  : %s", config.DRY_RUN)

    if not config.DRY_RUN:
        logger.warning("⚠️  REAL ORDERS WILL BE PLACED — press Ctrl+C within 5s to abort")
        time.sleep(5)

    # 1. Connect to Fyers
    from broker.fyers import get_client
    client = get_client()

    # 2. Prepare strategy and executor
    from strategy.moving_average import NYOpenFVGStrategy
    from execution.live_executor import LiveExecutor

    strategy = NYOpenFVGStrategy()
    executor = LiveExecutor(client)

    # 3. Set up live data feed
    from data.fyers_data import LiveDataFeed

    def on_bar_close(bar):
        """Called by LiveDataFeed each time a 1-min bar closes."""
        hhmm = bar["ist_hhmm"]

        # EOD check — force-close position and stop
        if hhmm >= config.EOD_EXIT_HHMM:
            logger.info("⏰  EOD reached (%04d IST) — closing positions", hhmm)
            executor.end_of_day()
            feed.stop()
            return

        # Heartbeat every 5 minutes
        if hhmm % 100 == 0 or hhmm % 5 == 0:
            executor.heartbeat()

        # Run strategy bar
        signal = strategy.on_bar(bar)

        # Execute signal
        if signal.get("action"):
            executor.handle(signal)

    feed = LiveDataFeed(
        client       = client,
        symbol       = config.SYMBOL,
        on_bar_close = on_bar_close,
    )

    logger.info("🚀  Starting live feed …  (Ctrl+C to stop)")
    feed.start()

    # ── Main loop: keep process alive, show heartbeat, handle EOD ─────────
    try:
        while True:
            now  = datetime.now(IST)
            hhmm = now.hour * 100 + now.minute

            # Hard EOD stop
            if hhmm >= config.EOD_EXIT_HHMM + 5:
                logger.info("🏁  Session over. Shutting down.")
                feed.stop()
                break

            # Pre-market wait
            if hhmm < config.SESSION_START_HHMM:
                mins_left = (
                    (config.SESSION_START_HHMM // 100) * 60 +
                    (config.SESSION_START_HHMM %  100)
                ) - (now.hour * 60 + now.minute)
                logger.info("⏳  Market opens in %d min", mins_left)

            time.sleep(30)   # check every 30 seconds

    except KeyboardInterrupt:
        logger.info("🛑  Interrupted by user")
        executor.end_of_day()
        feed.stop()


# ═══════════════════════════════════════════════════════════════════════════
#  UTILITY COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

def cmd_login():
    """Run Fyers OAuth2 login flow and save access token."""
    from broker.fyers import login
    token = login()
    logger.info("✅  Token saved.  You can now run the bot.")
    return token


def cmd_check():
    """Validate config and test Fyers connection."""
    logger.info("Checking config …")
    config.validate()
    logger.info("✅  Config OK")

    logger.info("Testing Fyers connection …")
    from broker.fyers import get_client
    client = get_client()

    # Test market quote
    from broker.fyers import get_ltp
    ltp = get_ltp(client, config.SYMBOL)
    if ltp:
        logger.info("✅  %s  LTP = %.2f", config.SYMBOL, ltp)
    else:
        logger.warning("⚠️  Could not fetch LTP (market may be closed)")

    logger.info("✅  All checks passed")


# ═══════════════════════════════════════════════════════════════════════════
#  ARGUMENT PARSER
# ═══════════════════════════════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog        = "main.py",
        description = "Fyers Dual-Mode FVG Trading Bot",
    )
    p.add_argument(
        "command",
        nargs   = "?",
        default = "run",
        choices = ["run", "login", "check"],
        help    = "run (default) | login | check",
    )
    p.add_argument(
        "--mode",
        choices = ["backtest", "live"],
        default = None,
        help    = "Override MODE in config.py",
    )
    return p


# ═══════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser  = build_parser()
    args    = parser.parse_args()

    # Allow --mode to override config
    mode = args.mode or config.MODE

    if args.command == "login":
        cmd_login()
        sys.exit(0)

    if args.command == "check":
        cmd_check()
        sys.exit(0)

    # ── Main run ────────────────────────────────────────────────────────────
    logger.info("═" * 62)
    logger.info("  FYERS FVG BOT  |  MODE: %s", mode.upper())
    logger.info("═" * 62)

    try:
        if mode == "backtest":
            run_backtest()
        elif mode == "live":
            run_live()
        else:
            logger.error("Unknown mode: %s  (set 'backtest' or 'live' in config.py)", mode)
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        sys.exit(1)
