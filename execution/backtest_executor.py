"""
execution/backtest_executor.py  —  Backtest Simulator
═══════════════════════════════════════════════════════════════════════════════
  Takes the Trade list from the strategy and:
    1. Applies fixed-fractional position sizing (risk % of equity)
    2. Builds the equity curve
    3. Computes full performance metrics
    4. Saves trade log CSV + summary report
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

import config
from strategy.moving_average import Trade, trades_to_df
from utils.logger import get_logger, log_separator

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
#  METRICS DATACLASS
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Metrics:
    total_trades      : int   = 0
    winning_trades    : int   = 0
    losing_trades     : int   = 0
    win_rate          : float = 0.0
    profit_factor     : float = 0.0
    expectancy_pct    : float = 0.0
    avg_r             : float = 0.0
    total_r           : float = 0.0
    initial_capital   : float = 0.0
    final_equity      : float = 0.0
    total_return_pct  : float = 0.0
    cagr_pct          : float = 0.0
    max_drawdown_pct  : float = 0.0
    sharpe_ratio      : float = 0.0
    sortino_ratio     : float = 0.0
    calmar_ratio      : float = 0.0
    primary_fvg_count : int   = 0
    retest_fvg_count  : int   = 0
    tp_exits          : int   = 0
    sl_exits          : int   = 0
    eod_exits         : int   = 0
    max_consec_wins   : int   = 0
    max_consec_losses : int   = 0
    avg_win_pct       : float = 0.0
    avg_loss_pct      : float = 0.0
    largest_win_pct   : float = 0.0
    largest_loss_pct  : float = 0.0

    def to_dict(self) -> dict:
        return {k: round(v, 4) if isinstance(v, float) else v
                for k, v in self.__dict__.items()}


# ═══════════════════════════════════════════════════════════════════════════
#  POSITION SIZING
# ═══════════════════════════════════════════════════════════════════════════

def _apply_sizing(trades_df: pd.DataFrame, capital: float, risk_pct: float) -> pd.DataFrame:
    """
    Fixed-fractional sizing.

    shares = (equity × risk%) / (entry - stop)
    dollar_pnl = shares × (exit - entry)
    """
    df     = trades_df.copy().reset_index(drop=True)
    equity = capital
    rows   = []

    for _, r in df.iterrows():
        risk_pts = abs(r["entry_price"] - r["stop_loss"])
        if risk_pts == 0:
            shares = 0.0
        else:
            shares = (equity * risk_pct) / risk_pts

        # Floor to whole shares/units
        shares = math.floor(shares) if shares > 1 else shares

        if r["direction"] == "long":
            dollar_pnl = shares * (r["exit_price"] - r["entry_price"])
        else:
            dollar_pnl = shares * (r["entry_price"] - r["exit_price"])

        equity_before = equity
        equity        = max(0.01, equity + dollar_pnl)

        rows.append({
            **r.to_dict(),
            "equity_before": round(equity_before, 2),
            "shares"       : round(shares, 4),
            "dollar_pnl"   : round(dollar_pnl, 2),
            "equity_after" : round(equity, 2),
        })

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════════
#  METRIC COMPUTATION
# ═══════════════════════════════════════════════════════════════════════════

def _compute_metrics(df: pd.DataFrame, capital: float, start: str, end: str) -> Metrics:
    m  = Metrics(initial_capital=capital)
    if df.empty:
        return m

    m.total_trades  = len(df)
    m.final_equity  = df["equity_after"].iloc[-1]
    m.initial_capital = capital

    wins = df[df["pnl_pct"] > 0]
    loss = df[df["pnl_pct"] < 0]

    m.winning_trades  = len(wins)
    m.losing_trades   = len(loss)
    m.win_rate        = m.winning_trades / m.total_trades * 100

    m.avg_win_pct     = wins["pnl_pct"].mean() if len(wins) else 0.0
    m.avg_loss_pct    = loss["pnl_pct"].mean() if len(loss) else 0.0
    m.largest_win_pct = wins["pnl_pct"].max()  if len(wins) else 0.0
    m.largest_loss_pct= loss["pnl_pct"].min()  if len(loss) else 0.0
    m.expectancy_pct  = df["pnl_pct"].mean()

    gp = wins["dollar_pnl"].sum() if len(wins) else 0.0
    gl = abs(loss["dollar_pnl"].sum()) if len(loss) else 0.0
    m.profit_factor   = gp / gl if gl > 0 else np.inf

    m.avg_r    = df["r_multiple"].mean()
    m.total_r  = df["r_multiple"].sum()

    # Equity / drawdown
    eq  = np.array([capital] + list(df["equity_after"]))
    rmx = np.maximum.accumulate(eq)
    dd  = (eq - rmx) / rmx * 100
    m.max_drawdown_pct = abs(dd.min())

    m.total_return_pct = (m.final_equity - capital) / capital * 100

    try:
        n_yrs = (pd.Timestamp(end) - pd.Timestamp(start)).days / 365.25
        if n_yrs > 0:
            m.cagr_pct = ((m.final_equity / capital) ** (1 / n_yrs) - 1) * 100
    except Exception:
        pass

    daily = df.groupby("date")["pnl_pct"].sum()
    if len(daily) > 1:
        mu      = daily.mean()
        sig     = daily.std()
        neg_sig = daily[daily < 0].std()
        m.sharpe_ratio  = (mu / sig  * np.sqrt(252)) if sig  > 0 else 0.0
        m.sortino_ratio = (mu / neg_sig * np.sqrt(252)) if neg_sig > 0 else 0.0

    m.calmar_ratio = m.cagr_pct / m.max_drawdown_pct if m.max_drawdown_pct > 0 else 0.0

    m.primary_fvg_count = (df["entry_type"] == "primary_fvg").sum()
    m.retest_fvg_count  = (df["entry_type"] == "retest_fvg").sum()
    m.tp_exits          = (df["exit_reason"] == "tp").sum()
    m.sl_exits          = (df["exit_reason"] == "sl").sum()
    m.eod_exits         = (df["exit_reason"] == "eod").sum()

    outcomes = (df["pnl_pct"] > 0).tolist()
    ws = ls = cw = cl = 0
    for w in outcomes:
        if w: cw += 1; cl = 0
        else: cl += 1; cw = 0
        ws = max(ws, cw); ls = max(ls, cl)
    m.max_consec_wins   = ws
    m.max_consec_losses = ls

    return m


# ═══════════════════════════════════════════════════════════════════════════
#  BACKTEST EXECUTOR
# ═══════════════════════════════════════════════════════════════════════════

class BacktestExecutor:
    """
    Receives raw Trade list from strategy, applies sizing, computes metrics.

    Usage
    ─────
        executor = BacktestExecutor()
        results  = executor.run(trades)
        executor.print_summary(results)
        executor.save(results)
    """

    def __init__(
        self,
        initial_capital : float = config.INITIAL_CAPITAL,
        risk_per_trade  : float = config.RISK_PER_TRADE / 100.0,
        start           : str   = config.BACKTEST_START,
        end             : str   = config.BACKTEST_END,
    ):
        self.capital = initial_capital
        self.risk    = risk_per_trade
        self.start   = start
        self.end     = end

    def run(self, trades: list[Trade]) -> dict:
        raw_df = trades_to_df(trades)

        if raw_df.empty:
            logger.warning("No trades to evaluate.")
            return {
                "trades"      : pd.DataFrame(),
                "metrics"     : Metrics(initial_capital=self.capital),
                "equity_curve": pd.Series([self.capital], dtype=float),
                "drawdown"    : pd.Series([0.0], dtype=float),
            }

        sized_df = _apply_sizing(raw_df, self.capital, self.risk)
        metrics  = _compute_metrics(sized_df, self.capital, self.start, self.end)

        eq = pd.Series([self.capital] + list(sized_df["equity_after"]), name="equity")
        dd = (eq - eq.expanding().max()) / eq.expanding().max() * 100
        dd.name = "drawdown_pct"

        return {
            "trades"      : sized_df,
            "metrics"     : metrics,
            "equity_curve": eq,
            "drawdown"    : dd,
        }

    def print_summary(self, results: dict) -> None:
        m = results["metrics"]
        sep = "═" * 62

        log_separator(logger, "BACKTEST RESULTS")
        logger.info(sep)
        logger.info("  %-30s %10s", "Symbol",         config.SYMBOL)
        logger.info("  %-30s %10s", "Period",         f"{self.start} → {self.end}")
        logger.info(sep)
        logger.info("  %-30s %10d", "Total Trades",   m.total_trades)
        logger.info("  %-30s %9.1f%%", "Win Rate",    m.win_rate)
        logger.info("  %-30s %10.2f", "Profit Factor",m.profit_factor)
        logger.info("  %-30s %9.2f%%", "Expectancy",  m.expectancy_pct)
        logger.info("  %-30s %10.2f", "Avg R",        m.avg_r)
        logger.info(sep)
        logger.info("  %-30s ₹%10,.2f", "Initial Capital", m.initial_capital)
        logger.info("  %-30s ₹%10,.2f", "Final Equity",    m.final_equity)
        logger.info("  %-30s %9.1f%%", "Total Return",     m.total_return_pct)
        logger.info("  %-30s %9.1f%%", "CAGR",             m.cagr_pct)
        logger.info("  %-30s %9.1f%%", "Max Drawdown",     m.max_drawdown_pct)
        logger.info("  %-30s %10.2f", "Sharpe Ratio",      m.sharpe_ratio)
        logger.info("  %-30s %10.2f", "Sortino Ratio",     m.sortino_ratio)
        logger.info("  %-30s %10.2f", "Calmar Ratio",      m.calmar_ratio)
        logger.info(sep)
        logger.info("  %-30s %10d", "Primary FVG",    m.primary_fvg_count)
        logger.info("  %-30s %10d", "Retest FVG",     m.retest_fvg_count)
        logger.info("  %-30s %3d / %3d / %3d",
                    "TP / SL / EOD", m.tp_exits, m.sl_exits, m.eod_exits)
        logger.info("  %-30s %10d", "Max Consec Wins",  m.max_consec_wins)
        logger.info("  %-30s %10d", "Max Consec Losses",m.max_consec_losses)
        logger.info(sep)

    def save(self, results: dict) -> Path:
        config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
        sym  = config.SYMBOL.replace(":", "_")
        stem = f"{sym}_{self.start}_{self.end}_{ts}"

        # Trades CSV
        csv_path = config.RESULTS_DIR / f"{stem}_trades.csv"
        if not results["trades"].empty:
            results["trades"].to_csv(csv_path, index=False)
            logger.info("💾  Trades saved → %s", csv_path)

        # Summary txt
        txt_path = config.RESULTS_DIR / f"{stem}_summary.txt"
        m = results["metrics"]
        with open(txt_path, "w") as f:
            f.write(f"Fyers FVG Backtest | {config.SYMBOL} | {self.start} → {self.end}\n")
            f.write("=" * 60 + "\n")
            for k, v in m.to_dict().items():
                f.write(f"{k:<30}: {v}\n")
        logger.info("💾  Summary saved → %s", txt_path)

        return csv_path
