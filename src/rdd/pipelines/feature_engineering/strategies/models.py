"""Strategy output data models."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal


@dataclass
class StrategySignal:
    """A single strategy verdict for one ticker."""

    strategy: str
    direction: Literal["bullish", "bearish", "neutral"]
    metrics: dict[str, Any]


@dataclass
class StockAnalysis:
    """Aggregated strategy signals for one ticker."""

    ticker: str
    signals: list[StrategySignal]
    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dict."""
        return {
            "ticker": self.ticker,
            "generated_at": self.generated_at.isoformat(),
            "signals": [
                {
                    "strategy": s.strategy,
                    "direction": s.direction,
                    "metrics": s.metrics,
                }
                for s in self.signals
            ],
        }
