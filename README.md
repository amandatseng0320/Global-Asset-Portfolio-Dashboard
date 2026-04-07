# 🌐 Global Asset Portfolio Dashboard

Multi-asset portfolio analysis with Python, BigQuery, and Tableau

**[📊 Live Dashboard →](https://public.tableau.com/app/profile/.41092116/viz/GlobalAssetPortfolioDashboard/GlobalAssetPortfolioDashboard?publish=yes)**

---

## Overview

This project automatically collects historical data across multiple financial markets using Python, computes key portfolio performance metrics, stores them in BigQuery, and presents an interactive analysis dashboard via Tableau.

**Assets covered:** BTC-USD, ETH-USD, SPY, QQQ (2020–2025)

---

## Tech Stack

| Layer | Tools | Purpose |
|-------|-------|---------|
| Data Collection | yfinance, requests | Stock prices, crypto, VIX, Fear & Greed Index |
| Web Scraping | BeautifulSoup, Google News RSS | News headlines for key market events |
| Data Processing | pandas | Feature engineering, metrics calculation |
| Database | Google BigQuery | Storing 4 structured tables |
| Visualization | Tableau Public | Interactive dashboard |

---

## Project Structure

```
global-asset-dashboard/
├── 1 src/
│   └── 4_assets_dashboard_pipeline_src.py   # Modular production script
├── 2 notebooks/
│   └── 4_assets_dashboard_pipeline.ipynb        # Exploratory notebook
├── 3 data/
│   ├── prices_master.csv
│   ├── metrics.csv
│   ├── key_events.csv
│   └── key_events_news.csv
└── README.md
```

---

## Dataset

| File | Description |
|------|-------------|
| `prices_master.csv` | Daily close price, daily return, cumulative return for each asset |
| `metrics.csv` | CAGR, Max Drawdown, average 30-day volatility |
| `key_events.csv` | Top 10 anomaly events per year scored by VIX + price + sentiment signals |
| `key_events_news.csv` | News headlines and sources matched to each key event date |

---

## Metrics

- **CAGR** — Compound Annual Growth Rate
- **Max Drawdown** — Largest peak-to-trough decline in cumulative return
- **Volatility (30d)** — 30-day rolling standard deviation of daily returns
- **Key Events Score** — Composite score combining price anomaly + VIX spike + sentiment shift; top 10 per year selected

---

## Pipeline Overview

The modular script (`src/`) runs the following steps in order:

1. Fetch historical prices via yfinance
2. Compute portfolio metrics (CAGR, Max Drawdown, Volatility)
3. Fetch Fear & Greed Index
4. Fetch VIX data
5. Upload raw tables to BigQuery
6. Build key events table via BigQuery SQL
7. Scrape news headlines for key event dates
8. Export all final tables to CSV

> To run the pipeline, update the config variables (`PROJECT_PATH`, `PROJECT_ID`, `DATASET_ID`) in `4_assets_dashboard_pipeline_src.py` before executing.
