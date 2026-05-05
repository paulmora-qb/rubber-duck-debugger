"""Price-only strategy portfolio nodes.

Five strategies, each producing PortfolioHoldingsSchema-compliant holdings
partitioned by rebalance date. All strategies:
  - Use OHLCV data only (no fundamentals, no external signals)
  - Backfill over the last ``backfill_months`` months at monthly intervals
  - Rank all tickers by a numeric score; take the top ``top_pct`` fraction
  - Only hold tickers where the score is positive (bullish direction)
  - Use equal weighting across selected tickers
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd

from rdd.schemas.portfolio_holdings import PortfolioHoldingsSchema

logger = logging.getLogger(__name__)


# ── Shared helpers ────────────────────────────────────────────────────────────


def _rebalance_dates(params: dict[str, Any]) -> list[pd.Timestamp]:
    """Return the last ``backfill_months`` first-business-day-of-month dates."""
    backfill_months: int = int(params.get("backfill_months", 3))
    today = pd.Timestamp.now("UTC").normalize().tz_localize(None)
    return list(pd.date_range(end=today, periods=backfill_months, freq="BMS"))


def _load_ticker_data(
    ohlcv: dict[str, Callable[[], pd.DataFrame]],
    as_of: pd.Timestamp,
) -> dict[str, pd.DataFrame]:
    """Load all tickers filtered to bars on or before *as_of*."""
    result: dict[str, pd.DataFrame] = {}
    for ticker_key, loader in ohlcv.items():
        try:
            df = loader().sort_values("date")
            df = df.copy()
            df["date"] = pd.to_datetime(df["date"])
            filtered = df[df["date"] <= as_of].reset_index(drop=True)
            if len(filtered) >= 2:
                result[ticker_key] = filtered
        except Exception:
            logger.debug("Could not load OHLCV for %s.", ticker_key)
    return result


def _build_holdings(
    strategy: str,
    date: pd.Timestamp,
    scores: dict[str, float],
    top_pct: float,
) -> pd.DataFrame | None:
    """Select top ``top_pct`` tickers, keep only positive scores, equal-weight.

    Ranks the full universe by score, takes the top fraction, then drops any
    with a non-positive score so the portfolio is long-only and bullish-only.
    """
    if not scores:
        return None
    n_select = max(1, round(len(scores) * top_pct))
    ranked = sorted(scores, key=scores.__getitem__, reverse=True)[:n_select]
    selected = [t for t in ranked if scores[t] > 0]
    if not selected:
        return None
    n = len(selected)
    weight = 1.0 / n
    rows = [
        {"strategy": strategy, "date": date, "ticker": t.upper(), "weight": weight}
        for t in selected
    ]
    return PortfolioHoldingsSchema.validate(pd.DataFrame(rows))


# ── Strategy-specific signal helpers ─────────────────────────────────────────


def _obv(df: pd.DataFrame) -> pd.Series:
    """Compute On-Balance Volume from adj_close and volume."""
    direction = np.sign(df["adj_close"].diff()).fillna(0)
    return (direction * df["volume"].fillna(0)).cumsum()


def _adx(df: pd.DataFrame, window: int) -> tuple[float, float, float]:
    """Return (adx, di_plus, di_minus) for the last bar using Wilder smoothing.

    Returns (nan, nan, nan) when there is insufficient data.
    """
    high = df["high"]
    low = df["low"]
    close = df["adj_close"]

    if len(close) < window * 2 + 1:
        return float("nan"), float("nan"), float("nan")

    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)

    up_move = high - high.shift(1)
    down_move = low.shift(1) - low

    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    # Wilder smoothing: com = window - 1  ↔  alpha = 1 / window
    atr = tr.ewm(com=window - 1, adjust=False).mean()
    smooth_plus = plus_dm.ewm(com=window - 1, adjust=False).mean()
    smooth_minus = minus_dm.ewm(com=window - 1, adjust=False).mean()

    di_plus = 100.0 * smooth_plus / (atr + 1e-10)
    di_minus = 100.0 * smooth_minus / (atr + 1e-10)

    dx = 100.0 * (di_plus - di_minus).abs() / (di_plus + di_minus + 1e-10)
    adx_series = dx.ewm(com=window - 1, adjust=False).mean()

    return float(adx_series.iloc[-1]), float(di_plus.iloc[-1]), float(di_minus.iloc[-1])


# ── Strategy nodes ────────────────────────────────────────────────────────────


def compute_donchian_holdings(
    ohlcv: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Donchian Channel Breakout: price position within the N-day high/low channel.

    Score = (close - N_low) / (N_high - N_low) - 0.5, centred on the channel
    midpoint. Positive scores indicate the price is trading in the upper half of
    the channel; negative scores indicate the lower half.

    Args:
        ohlcv: Partitioned OHLCV dataset keyed by lowercase ticker.
        params: ``price_strategies`` parameter block.

    Returns:
        Date-keyed dict of equal-weight holdings DataFrames.
    """
    strategy_params: dict[str, Any] = params.get("donchian", {})
    window: int = int(strategy_params.get("window", 20))
    strategy_name: str = str(strategy_params.get("strategy_name", "donchian_breakout"))
    top_pct: float = float(params.get("top_pct", 0.20))

    result: dict[str, pd.DataFrame] = {}
    for rebalance_date in _rebalance_dates(params):
        ticker_data = _load_ticker_data(ohlcv, rebalance_date)
        scores: dict[str, float] = {}

        for ticker_key, df in ticker_data.items():
            prices = df["adj_close"].dropna()
            if len(prices) < window:
                continue
            high_n = float(prices.rolling(window).max().iloc[-1])
            low_n = float(prices.rolling(window).min().iloc[-1])
            band = high_n - low_n
            if band <= 0:
                continue
            scores[ticker_key] = (float(prices.iloc[-1]) - low_n) / band - 0.5

        holdings = _build_holdings(strategy_name, rebalance_date, scores, top_pct)
        if holdings is not None:
            result[rebalance_date.strftime("%Y-%m-%d")] = holdings
        else:
            logger.info(
                "No bullish tickers for %s on %s.", strategy_name, rebalance_date.date()
            )

    return result


def compute_52w_high_holdings(
    ohlcv: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """52-Week High Proximity: price closeness to its rolling 252-day high.

    Score = close / rolling_252_high - threshold. Positive scores indicate the
    price is within ``threshold`` of its annual high; negative scores are further
    away. Higher scores → closer to the 52-week high → stronger price anchoring.

    Args:
        ohlcv: Partitioned OHLCV dataset keyed by lowercase ticker.
        params: ``price_strategies`` parameter block.

    Returns:
        Date-keyed dict of equal-weight holdings DataFrames.
    """
    strategy_params: dict[str, Any] = params.get("high_52w", {})
    window: int = int(strategy_params.get("window", 252))
    threshold: float = float(strategy_params.get("proximity_threshold", 0.95))
    strategy_name: str = str(strategy_params.get("strategy_name", "high_52w"))
    top_pct: float = float(params.get("top_pct", 0.20))

    result: dict[str, pd.DataFrame] = {}
    for rebalance_date in _rebalance_dates(params):
        ticker_data = _load_ticker_data(ohlcv, rebalance_date)
        scores: dict[str, float] = {}

        for ticker_key, df in ticker_data.items():
            prices = df["adj_close"].dropna()
            if len(prices) < window:
                continue
            rolling_max = float(prices.rolling(window).max().iloc[-1])
            if rolling_max <= 0:
                continue
            scores[ticker_key] = float(prices.iloc[-1]) / rolling_max - threshold

        holdings = _build_holdings(strategy_name, rebalance_date, scores, top_pct)
        if holdings is not None:
            result[rebalance_date.strftime("%Y-%m-%d")] = holdings
        else:
            logger.info(
                "No bullish tickers for %s on %s.", strategy_name, rebalance_date.date()
            )

    return result


def compute_cross_sect_momentum_holdings(
    ohlcv: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Cross-sectional 12-1 momentum: trailing return with skip-month reversal filter.

    Score = close[t - skip] / close[t - lookback] - 1. Skipping the most recent
    month avoids short-term reversal noise (Jegadeesh & Titman). Tickers are
    ranked cross-sectionally; the top ``top_pct`` with positive scores are held.

    Args:
        ohlcv: Partitioned OHLCV dataset keyed by lowercase ticker.
        params: ``price_strategies`` parameter block.

    Returns:
        Date-keyed dict of equal-weight holdings DataFrames.
    """
    strategy_params: dict[str, Any] = params.get("cross_sect_momentum", {})
    lookback: int = int(strategy_params.get("lookback", 252))
    skip: int = int(strategy_params.get("skip", 21))
    strategy_name: str = str(
        strategy_params.get("strategy_name", "cross_sect_momentum")
    )
    top_pct: float = float(params.get("top_pct", 0.20))

    result: dict[str, pd.DataFrame] = {}
    for rebalance_date in _rebalance_dates(params):
        ticker_data = _load_ticker_data(ohlcv, rebalance_date)
        scores: dict[str, float] = {}

        for ticker_key, df in ticker_data.items():
            prices = df["adj_close"].dropna()
            if len(prices) < lookback + 1:
                continue
            p_recent = (
                float(prices.iloc[-skip])
                if len(prices) > skip
                else float(prices.iloc[-1])
            )
            p_past = float(prices.iloc[-lookback])
            if p_past <= 0:
                continue
            scores[ticker_key] = p_recent / p_past - 1.0

        holdings = _build_holdings(strategy_name, rebalance_date, scores, top_pct)
        if holdings is not None:
            result[rebalance_date.strftime("%Y-%m-%d")] = holdings
        else:
            logger.info(
                "No bullish tickers for %s on %s.", strategy_name, rebalance_date.date()
            )

    return result


def compute_obv_holdings(
    ohlcv: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """OBV Momentum: z-score of On-Balance Volume relative to its rolling mean.

    Score = (OBV[-1] - mean(OBV[-window:])) / std(OBV[-window:]). A positive
    z-score indicates the OBV is currently elevated above its recent baseline,
    signalling net buying pressure (accumulation).

    Args:
        ohlcv: Partitioned OHLCV dataset keyed by lowercase ticker.
        params: ``price_strategies`` parameter block.

    Returns:
        Date-keyed dict of equal-weight holdings DataFrames.
    """
    strategy_params: dict[str, Any] = params.get("obv", {})
    window: int = int(strategy_params.get("window", 21))
    strategy_name: str = str(strategy_params.get("strategy_name", "obv_momentum"))
    top_pct: float = float(params.get("top_pct", 0.20))

    result: dict[str, pd.DataFrame] = {}
    for rebalance_date in _rebalance_dates(params):
        ticker_data = _load_ticker_data(ohlcv, rebalance_date)
        scores: dict[str, float] = {}

        for ticker_key, df in ticker_data.items():
            if len(df) < window + 1:
                continue
            obv_series = _obv(df)
            rolling_window = obv_series.iloc[-window:]
            std = float(rolling_window.std())
            if std < 1e-10:
                continue
            scores[ticker_key] = (
                float(obv_series.iloc[-1]) - float(rolling_window.mean())
            ) / std

        holdings = _build_holdings(strategy_name, rebalance_date, scores, top_pct)
        if holdings is not None:
            result[rebalance_date.strftime("%Y-%m-%d")] = holdings
        else:
            logger.info(
                "No bullish tickers for %s on %s.", strategy_name, rebalance_date.date()
            )

    return result


def compute_adx_holdings(
    ohlcv: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """ADX Trend Strength: directional movement index with trend-strength weighting.

    Score = ADX * sign(DI+ - DI-). Positive scores indicate an upward trend
    (DI+ > DI-) with strength proportional to ADX; negative scores indicate a
    downward trend. Only stocks with ADX above ``adx_threshold`` qualify.

    Args:
        ohlcv: Partitioned OHLCV dataset keyed by lowercase ticker.
        params: ``price_strategies`` parameter block.

    Returns:
        Date-keyed dict of equal-weight holdings DataFrames.
    """
    strategy_params: dict[str, Any] = params.get("adx", {})
    window: int = int(strategy_params.get("window", 14))
    adx_threshold: float = float(strategy_params.get("adx_threshold", 25.0))
    strategy_name: str = str(strategy_params.get("strategy_name", "adx_trend"))
    top_pct: float = float(params.get("top_pct", 0.20))

    result: dict[str, pd.DataFrame] = {}
    for rebalance_date in _rebalance_dates(params):
        ticker_data = _load_ticker_data(ohlcv, rebalance_date)
        scores: dict[str, float] = {}

        for ticker_key, df in ticker_data.items():
            adx_val, di_plus, di_minus = _adx(df, window)
            if np.isnan(adx_val) or adx_val < adx_threshold:
                continue
            direction = 1.0 if di_plus > di_minus else -1.0
            scores[ticker_key] = adx_val * direction

        holdings = _build_holdings(strategy_name, rebalance_date, scores, top_pct)
        if holdings is not None:
            result[rebalance_date.strftime("%Y-%m-%d")] = holdings
        else:
            logger.info(
                "No bullish tickers for %s on %s.", strategy_name, rebalance_date.date()
            )

    return result
