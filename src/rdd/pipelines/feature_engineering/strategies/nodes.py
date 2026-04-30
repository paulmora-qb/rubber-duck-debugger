"""Strategy signal computation nodes."""

from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd
from arch import arch_model

from rdd.pipelines.feature_engineering.strategies.models import (
    StockAnalysis,
    StrategySignal,
)


def compute_momentum_signals(
    ohlcv: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, StrategySignal]:
    """Compute price-return momentum signals over configurable lookback windows.

    Direction is bullish when at least ``momentum_min_bullish`` windows show a
    positive return, bearish when fewer than ``total - momentum_min_bullish``
    windows are positive, and neutral otherwise.

    Args:
        ohlcv: Partitioned OHLCV dataset, keyed by lowercase ticker symbol.
        params: Strategy parameters (see ``params_strategies.yml``).

    Returns:
        Mapping of ticker key to momentum ``StrategySignal``.
    """
    windows: dict[str, int] = params.get(
        "momentum_windows", {"1m": 21, "3m": 63, "6m": 126, "12m": 252}
    )
    min_bullish: int = params.get("momentum_min_bullish", 3)
    signals: dict[str, StrategySignal] = {}

    for ticker_key, loader in ohlcv.items():
        df = loader().sort_values("date")
        prices = df["adj_close"].dropna()
        if len(prices) < 2:
            continue

        metrics: dict[str, Any] = {}
        positive_count = 0
        total = 0

        for label, days in windows.items():
            if len(prices) > days:
                ret = round(float(prices.iloc[-1] / prices.iloc[-days] - 1), 4)
                metrics[f"return_{label}"] = ret
                if ret > 0:
                    positive_count += 1
                total += 1

        if total == 0:
            continue

        if positive_count >= min_bullish:
            direction = "bullish"
        elif positive_count <= total - min_bullish:
            direction = "bearish"
        else:
            direction = "neutral"

        signals[ticker_key] = StrategySignal(
            strategy="momentum", direction=direction, metrics=metrics
        )

    return signals


def compute_trend_signals(
    ohlcv: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, StrategySignal]:
    """Compute moving-average trend signals (golden / death cross).

    When both MAs are available, direction follows the MA cross: bullish on a
    golden cross (short > long), bearish on a death cross. When only the short
    MA is available, direction follows price vs. short MA.

    Args:
        ohlcv: Partitioned OHLCV dataset, keyed by lowercase ticker symbol.
        params: Strategy parameters (see ``params_strategies.yml``).

    Returns:
        Mapping of ticker key to trend ``StrategySignal``.
    """
    short_window: int = params.get("trend_short_window", 50)
    long_window: int = params.get("trend_long_window", 200)
    signals: dict[str, StrategySignal] = {}

    for ticker_key, loader in ohlcv.items():
        df = loader().sort_values("date")
        prices = df["adj_close"].dropna()
        if len(prices) < short_window:
            continue

        ma_short = float(prices.rolling(short_window).mean().iloc[-1])
        price = float(prices.iloc[-1])
        price_vs_short_pct = round((price / ma_short - 1), 4)

        metrics: dict[str, Any] = {
            f"ma{short_window}": round(ma_short, 2),
            "price_vs_ma_short_pct": price_vs_short_pct,
        }

        if len(prices) >= long_window:
            ma_long = float(prices.rolling(long_window).mean().iloc[-1])
            metrics[f"ma{long_window}"] = round(ma_long, 2)
            metrics["cross"] = "golden" if ma_short > ma_long else "death"
            direction = "bullish" if ma_short > ma_long else "bearish"
        else:
            direction = "bullish" if price_vs_short_pct > 0 else "bearish"

        signals[ticker_key] = StrategySignal(
            strategy="trend", direction=direction, metrics=metrics
        )

    return signals


def compute_mean_reversion_signals(
    ohlcv: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, StrategySignal]:
    """Compute mean-reversion signals via RSI and Bollinger Bands.

    RSI below ``rsi_oversold`` signals a bullish (oversold) condition; above
    ``rsi_overbought`` signals bearish (overbought). Bollinger Band position
    is reported as a 0-1 fraction (0 = lower band, 1 = upper band).

    Args:
        ohlcv: Partitioned OHLCV dataset, keyed by lowercase ticker symbol.
        params: Strategy parameters (see ``params_strategies.yml``).

    Returns:
        Mapping of ticker key to mean-reversion ``StrategySignal``.
    """
    rsi_window: int = params.get("rsi_window", 14)
    rsi_oversold: float = params.get("rsi_oversold", 30.0)
    rsi_overbought: float = params.get("rsi_overbought", 70.0)
    bb_window: int = params.get("bb_window", 20)
    bb_std: float = params.get("bb_std", 2.0)
    signals: dict[str, StrategySignal] = {}

    for ticker_key, loader in ohlcv.items():
        df = loader().sort_values("date")
        prices = df["adj_close"].dropna()
        if len(prices) < max(rsi_window + 1, bb_window):
            continue

        # RSI via simple rolling averages
        delta = prices.diff()
        avg_gain = delta.clip(lower=0).rolling(rsi_window).mean()
        avg_loss = (-delta.clip(upper=0)).rolling(rsi_window).mean()
        # avg_loss=0 means pure gains → RS → ∞ → RSI → 100; avoid div-by-zero
        rs = avg_gain / avg_loss.replace(0, 1e-10)
        rsi = float((100 - 100 / (1 + rs)).iloc[-1])

        # Bollinger Bands
        ma = float(prices.rolling(bb_window).mean().iloc[-1])
        std = float(prices.rolling(bb_window).std().iloc[-1])
        bb_upper = ma + bb_std * std
        bb_lower = ma - bb_std * std
        price = float(prices.iloc[-1])
        band_width = bb_upper - bb_lower
        bb_position = (
            round((price - bb_lower) / band_width, 2) if band_width > 0 else 0.5
        )

        metrics: dict[str, Any] = {
            f"rsi_{rsi_window}": round(rsi, 1),
            "bb_position": bb_position,
            "bb_upper": round(bb_upper, 2),
            "bb_lower": round(bb_lower, 2),
        }

        if rsi < rsi_oversold:
            direction = "bullish"
        elif rsi > rsi_overbought:
            direction = "bearish"
        else:
            direction = "neutral"

        signals[ticker_key] = StrategySignal(
            strategy="mean_reversion", direction=direction, metrics=metrics
        )

    return signals


def compute_volatility_signals(
    ohlcv: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, StrategySignal]:
    """Fit a GARCH(1,1) model on log returns to estimate conditional volatility.

    Produces a regime descriptor — not a directional bet. Direction is:
    - ``"bearish"`` when current vol is elevated vs. long-run (vol_ratio > high threshold)
    - ``"bullish"`` when vol is compressed vs. long-run (vol_ratio < low threshold)
    - ``"neutral"`` otherwise

    Args:
        ohlcv: Partitioned OHLCV dataset, keyed by lowercase ticker symbol.
        params: Strategy parameters (see ``params_strategies.yml``).

    Returns:
        Mapping of ticker key to volatility ``StrategySignal``.
    """
    min_obs: int = params.get("garch_min_obs", 252)
    vol_high: float = params.get("garch_vol_ratio_high", 1.5)
    vol_low: float = params.get("garch_vol_ratio_low", 0.75)
    signals: dict[str, StrategySignal] = {}

    for ticker_key, loader in ohlcv.items():
        df = loader().sort_values("date")
        prices = df["adj_close"].dropna()
        if len(prices) < min_obs + 1:
            continue

        log_returns = np.log(prices / prices.shift(1)).dropna() * 100

        try:
            res = arch_model(log_returns, vol="Garch", p=1, q=1, rescale=False).fit(
                disp="off"
            )
        except Exception:
            continue

        # Conditional vol for today (annualised %)
        current_vol = float(np.sqrt(res.conditional_volatility.iloc[-1] ** 2 * 252))
        # Long-run unconditional vol: omega / (1 - alpha - beta), annualised
        omega = float(res.params["omega"])
        alpha = float(res.params["alpha[1]"])
        beta = float(res.params["beta[1]"])
        persistence = round(alpha + beta, 4)
        denom = 1 - alpha - beta
        if denom <= 0:
            continue
        long_run_vol = float(np.sqrt(omega / denom * 252))
        vol_ratio = round(current_vol / long_run_vol, 4) if long_run_vol > 0 else 1.0

        if vol_ratio > vol_high:
            direction = "bearish"
        elif vol_ratio < vol_low:
            direction = "bullish"
        else:
            direction = "neutral"

        signals[ticker_key] = StrategySignal(
            strategy="volatility",
            direction=direction,
            metrics={
                "current_vol_ann": round(current_vol, 4),
                "long_run_vol_ann": round(long_run_vol, 4),
                "vol_ratio": vol_ratio,
                "persistence": persistence,
            },
        )

    return signals


def assemble_stock_analyses(
    momentum: dict[str, StrategySignal],
    trend: dict[str, StrategySignal],
    mean_reversion: dict[str, StrategySignal],
    volatility: dict[str, StrategySignal],
) -> dict[str, dict[str, Any]]:
    """Combine per-strategy signals into one ``StockAnalysis`` per ticker.

    Args:
        momentum: Momentum signals keyed by lowercase ticker.
        trend: Trend signals keyed by lowercase ticker.
        mean_reversion: Mean-reversion signals keyed by lowercase ticker.
        volatility: GARCH volatility signals keyed by lowercase ticker.

    Returns:
        Mapping of lowercase ticker to serialised ``StockAnalysis`` dict,
        ready for JSON persistence.
    """
    all_tickers = set(momentum) | set(trend) | set(mean_reversion) | set(volatility)
    result: dict[str, dict[str, Any]] = {}

    for ticker_key in sorted(all_tickers):
        signals = [
            sig
            for sig_map in (momentum, trend, mean_reversion, volatility)
            if (sig := sig_map.get(ticker_key)) is not None
        ]
        analysis = StockAnalysis(ticker=ticker_key.upper(), signals=signals)
        result[ticker_key] = analysis.to_dict()

    return result
