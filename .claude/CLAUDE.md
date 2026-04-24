# CLAUDE.md

This file provides guidance to Claude Code when working with this repository.

## Project Overview

**Rubber Duck Debugger** — an algorithmic trading research platform for stock prediction and strategy testing. The system connects to a live broker API to execute and test strategies in real time, includes a backtesting engine for historical validation, and pulls financial data and news for signal generation.

### Core modules (planned)

| Module | Purpose |
|---|---|
| `broker` | Broker API integration — order placement, portfolio state, live execution |
| `backtest` | Historical strategy simulation engine |
| `data` | Financial data ingestion: OHLCV, fundamentals, news feeds |
| `strategies` | Strategy definitions and signal logic |
| `models` | Prediction models (ML/statistical) |

## Commands

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest tests/

# Lint / format
uv run pre-commit run --files <changed files>
```

## Worktrees

Worktrees live in `.claude/worktrees/` and are excluded from git tracking. Use Claude Code's `EnterWorktree` to spin up an isolated branch for a feature or experiment.

```
.claude/
  worktrees/       ← git worktrees (gitignored)
  agents/          ← custom subagent definitions
  CLAUDE.md        ← this file
```

## Conventions

- All commands prefixed with `uv run` (no venv activation needed).
- Strategy and model experiments should be developed in a worktree to keep `main` clean.
- Data artifacts (`data/`, `models/`) are gitignored — never commit raw data or trained models.
