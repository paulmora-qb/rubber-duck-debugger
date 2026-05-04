# Rubber Duck Debugger

An algorithmic trading research platform for stock prediction and strategy testing. Connects to a live broker API, runs automated data ingestion pipelines, and generates AI-powered portfolio construction and market analysis.

## Architecture

```
data ingestion  →  feature engineering  →  strategies  →  portfolio performance
(daily, auto)       (valuation ratios)    (monthly, AI)    (weekly email)
```

## Automated Jobs

All jobs run via macOS launchd. Install with:

```bash
bash scripts/install_launchd.sh
```

| Agent | Schedule | Script | What it does |
|---|---|---|---|
| `com.rdd.daily-ingest` | Mon–Fri 10:00 local | `run_daily_ingest.sh` | Fetches OHLCV, company info, news, financials, valuation ratios, analyst consensus, earnings history. Sends a summary email on completion. |
| `com.rdd.weekly-performance` | Every Friday 12:00 local | `run_weekly_performance.sh` | Runs the portfolio performance pipeline and emails a report with a 3-month cumulative-return chart, KPI table, and holdings breakdown. |
| `com.rdd.monthly-strategy` | 1st of each month 12:00 local | `run_monthly_strategy.sh` | Runs the `ai_fundamental_screen` strategy: Claude Haiku scores ~500 S&P 500 tickers on fundamentals, Claude Sonnet selects a concentrated 10-stock portfolio, and rebalancing trades are logged. |
| `com.rdd.weekly-news-analysis` | Every Friday 12:00 local | `run_weekly_news_analysis.sh` | Runs the `news_analysis` pipeline: generates AI-powered bull/bear research reports for each ticker using recent news and financials. |

All jobs sync to `main` before running (force checkout + hard reset) and send a failure alert email if they exit non-zero.

## Data Pipelines (Kedro)

| Pipeline | Trigger | Output |
|---|---|---|
| `stock_prices` | Daily | OHLCV parquets per ticker, incremental from last stored date |
| `company_info` | Daily | Snapshot parquet per ticker (sector, market cap, employees, …) |
| `company_news` | Daily | Cumulative news parquets per ticker via Finnhub, incremental |
| `company_financials` | Daily | Quarterly + annual financials per ticker via yfinance, refreshed every 7 days |
| `valuation_ratios` | Daily | P/E, P/B, EV/EBITDA, etc. derived from financials + OHLCV |
| `analyst_consensus` | Daily | Wall Street buy/hold/sell ratings per ticker |
| `earnings_history` | Daily | Historical EPS surprises per ticker |
| `strategies` | On-demand | Feature engineering and signal computation |
| `ai_fundamental_screen` | Monthly (1st) | AI portfolio construction — scoring, selection, rebalancing |
| `portfolio_performance` | Weekly (Friday) | Cumulative returns, Sharpe, max drawdown per strategy |
| `news_analysis` | Weekly (Friday) | AI bull/bear research reports per ticker |

## Logs

All job logs land in `logs/` (gitignored):

| File | Job |
|---|---|
| `logs/daily_ingest.log` | Daily ingest |
| `logs/weekly_performance.log` | Weekly performance |
| `logs/monthly_strategy.log` | Monthly strategy |
| `logs/weekly_news_analysis.log` | Weekly news analysis |
| `logs/launchd.log` | launchd stdout/stderr |
| `logs/run_manifest.jsonl` | Per-run OHLCV/news counts (used by report charts) |

## Setup

### Prerequisites

- Python 3.13+, [uv](https://github.com/astral-sh/uv)
- API keys: `ANTHROPIC_API_KEY`, `FINNHUB_API_KEY`
- SMTP credentials for email reports: `RDD_SMTP_USER`, `RDD_SMTP_PASS`, `RDD_EMAIL_TO`

### Install

```bash
# 1. Clone and install dependencies
git clone <repo>
cd rubber-duck-debugger
uv sync

# 2. Copy and fill in credentials
cp .env.example .env   # edit with your keys

# 3. Install launchd agents
bash scripts/install_launchd.sh
```

### Development

```bash
# Run tests
uv run pytest tests/

# Lint / format
uv run pre-commit run --files <changed files>

# Run a pipeline manually
uv run kedro run --pipeline stock_prices

# Dry-run the daily ingest (prints commands, no execution)
bash scripts/run_daily_ingest.sh --dry-run
```

Feature branches and experiments should be developed in a worktree to keep `main` clean:

```bash
# In Claude Code
/worktree my-feature
```
