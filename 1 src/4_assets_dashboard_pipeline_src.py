# -*- coding: utf-8 -*-
"""
4_assets_dashboard_pipeline_src.py
Global Asset Portfolio Dashboard — Modular Version

Refactored from: 01_market_data_pipeline.ipynb
Description: Fetches multi-asset price data, computes portfolio metrics,
             scrapes news for key market events, and uploads to BigQuery.
"""

# ⚠️ Colab only — remove if running locally
# !pip install yfinance

import os
import time
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# ⚠️ Colab only — mount Google Drive
# from google.colab import drive
# drive.mount("/content/drive")

# ⚠️ Colab only — Google Cloud authentication
# from google.colab import auth
# auth.authenticate_user()

from google.cloud import bigquery

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_PATH = "your/google/drive/path"
PROJECT_ID   = "your-gcp-project-id"
DATASET_ID   = "your-dataset-id"
TICKERS      = ["BTC-USD", "ETH-USD", "SPY", "QQQ"]
START_DATE   = "2020-01-01"
END_DATE     = "2025-12-31"

# ── 1. Fetch Price Data ───────────────────────────────────────────────────────

def fetch_prices(tickers, start, end, save_path):
    """
    Download historical close prices for each ticker via yfinance.
    Computes daily return and cumulative return.
    Returns a combined DataFrame and saves to CSV.
    """
    all_data = []

    for ticker in tickers:
        df = yf.download(ticker, start=start, end=end, auto_adjust=True)
        df = df[["Close"]].copy()
        df.columns = ["close"]
        df["asset"] = ticker
        df.index.name = "date"
        df = df.reset_index()

        df["daily_return"] = df["close"].pct_change()
        df["cum_return"]   = (1 + df["daily_return"]).cumprod().fillna(1)

        all_data.append(df)

    prices = pd.concat(all_data, ignore_index=True)
    prices.to_csv(f"{save_path}/prices_master.csv", index=False)
    print(f"prices_master.csv saved — {prices.shape[0]} rows")
    return prices


# ── 2. Compute Portfolio Metrics ──────────────────────────────────────────────

def calc_cagr(group):
    """Compound Annual Growth Rate"""
    start  = group["close"].iloc[0]
    end    = group["close"].iloc[-1]
    days   = (pd.to_datetime(group["date"].iloc[-1]) - pd.to_datetime(group["date"].iloc[0])).days
    years  = days / 365
    return (end / start) ** (1 / years) - 1


def calc_max_drawdown(group):
    """Maximum peak-to-trough decline in cumulative return"""
    cummax   = group["cum_return"].cummax()
    drawdown = (group["cum_return"] - cummax) / cummax
    return drawdown.min()


def calc_metrics(prices, save_path):
    """
    Compute CAGR, Max Drawdown, and average 30-day volatility per asset.
    Returns a metrics DataFrame and saves to CSV.
    """
    prices["volatility_30d"] = prices.groupby("asset")["daily_return"].transform(
        lambda x: x.rolling(30).std()
    )

    cagr_df = prices.groupby("asset").apply(calc_cagr, include_groups=False).reset_index()
    cagr_df.columns = ["asset", "cagr"]

    mdd_df = prices.groupby("asset").apply(calc_max_drawdown, include_groups=False).reset_index()
    mdd_df.columns = ["asset", "max_drawdown"]

    vol_df = prices.groupby("asset")["volatility_30d"].mean().reset_index()
    vol_df.columns = ["asset", "avg_volatility_30d"]

    metrics = cagr_df.merge(mdd_df, on="asset").merge(vol_df, on="asset")
    metrics.to_csv(f"{save_path}/metrics.csv", index=False)
    print("metrics.csv saved")
    print(metrics)
    return metrics


# ── 3. Fetch Fear & Greed Index ───────────────────────────────────────────────

def fetch_fear_greed(start, end, save_path):
    """
    Fetch Fear & Greed Index from alternative.me API.
    Filters to the specified date range and saves to CSV.
    """
    url      = "https://api.alternative.me/fng/?limit=2000"
    response = requests.get(url)
    data     = response.json()

    fng_df = pd.DataFrame(data["data"])
    fng_df["date"] = pd.to_datetime(fng_df["timestamp"].astype(int), unit="s")
    fng_df = fng_df[["date", "value", "value_classification"]]
    fng_df["value"] = fng_df["value"].astype(int)
    fng_df.columns = ["date", "fng_value", "fng_classification"]
    fng_df = fng_df.sort_values("date").reset_index(drop=True)

    fng_df = fng_df[(fng_df["date"] >= start) & (fng_df["date"] <= end)].reset_index(drop=True)
    fng_df.to_csv(f"{save_path}/fear_greed.csv", index=False)
    print(f"fear_greed.csv saved — {fng_df.shape[0]} rows")
    return fng_df


# ── 4. Fetch VIX Data ─────────────────────────────────────────────────────────

def fetch_vix(start, end, save_path):
    """
    Download VIX historical data via yfinance and save to CSV.
    """
    vix = yf.download("^VIX", start=start, end=end, auto_adjust=True)
    vix_df = vix[["Close"]].copy()
    vix_df.columns = ["vix_close"]
    vix_df.index.name = "date"
    vix_df = vix_df.reset_index()

    vix_df.to_csv(f"{save_path}/vix.csv", index=False)
    print(f"vix.csv saved — {vix_df.shape[0]} rows")
    return vix_df


# ── 5. Upload Raw Tables to BigQuery ─────────────────────────────────────────

def upload_to_bigquery(client, project_id, dataset_id, save_path):
    """
    Upload prices, metrics, fear_greed, and vix CSVs to BigQuery.
    Overwrites existing tables.
    """
    tables = {
        "prices":     f"{save_path}/prices_master.csv",
        "metrics":    f"{save_path}/metrics.csv",
        "fear_greed": f"{save_path}/fear_greed.csv",
        "vix":        f"{save_path}/vix.csv",
    }

    for table_name, file_path in tables.items():
        df        = pd.read_csv(file_path)
        table_ref = f"{project_id}.{dataset_id}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"{table_name} uploaded — {len(df)} rows")


# ── 6. Build Key Events Table in BigQuery ────────────────────────────────────

def build_key_events(client, project_id, dataset_id):
    """
    Run a BigQuery SQL query to detect anomaly events
    (price anomaly + VIX spike + sentiment shift).
    Selects top 10 events per year and stores as key_events table.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.key_events` AS

    WITH price_anomaly AS (
        WITH stats AS (
            SELECT
                asset,
                AVG(daily_return) AS mean_return,
                STDDEV(daily_return) AS std_return
            FROM `{project_id}.{dataset_id}.prices`
            WHERE daily_return IS NOT NULL
            GROUP BY asset
        )
        SELECT
            DATE(p.date) AS date,
            "價格異常" AS anomaly_type,
            MAX(ABS(p.daily_return - s.mean_return) / s.std_return) AS severity
        FROM `{project_id}.{dataset_id}.prices` AS p
        JOIN stats AS s ON p.asset = s.asset
        WHERE p.daily_return IS NOT NULL
            AND ABS(p.daily_return - s.mean_return) > 2 * s.std_return
        GROUP BY DATE(p.date)
    ),
    fng_anomaly AS (
        WITH fng_change AS (
            SELECT
                date,
                ABS(fng_value - LAG(fng_value) OVER (ORDER BY date)) AS daily_change
            FROM `{project_id}.{dataset_id}.fear_greed`
        )
        SELECT
            DATE(date) AS date,
            "情緒異常" AS anomaly_type,
            daily_change / 10.0 AS severity
        FROM fng_change
        WHERE daily_change > 10
    ),
    vix_anomaly AS (
        WITH vix_change AS (
            SELECT
                date,
                (vix_close - LAG(vix_close) OVER (ORDER BY date)) /
                LAG(vix_close) OVER (ORDER BY date) * 100 AS daily_change_pct
            FROM `{project_id}.{dataset_id}.vix`
        )
        SELECT
            DATE(date) AS date,
            "VIX異常" AS anomaly_type,
            daily_change_pct / 20.0 AS severity
        FROM vix_change
        WHERE daily_change_pct > 20
    ),
    all_anomalies AS (
        SELECT date, anomaly_type, severity FROM price_anomaly
        UNION ALL
        SELECT date, anomaly_type, severity FROM fng_anomaly
        UNION ALL
        SELECT date, anomaly_type, severity FROM vix_anomaly
    ),
    daily_score AS (
        SELECT
            date,
            EXTRACT(YEAR FROM date) AS year,
            COUNT(DISTINCT anomaly_type) AS anomaly_count,
            SUM(severity) AS severity_score,
            COUNT(DISTINCT anomaly_type) * 2 + SUM(severity) AS total_score,
            STRING_AGG(anomaly_type, " + " ORDER BY anomaly_type) AS anomaly_types
        FROM all_anomalies
        GROUP BY date
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_score DESC) AS rank
        FROM daily_score
    )
    SELECT
        date,
        year,
        rank,
        anomaly_count,
        anomaly_types,
        ROUND(severity_score, 2) AS severity_score,
        ROUND(total_score, 2) AS total_score
    FROM ranked
    WHERE rank <= 10
    ORDER BY year ASC, rank ASC
    """

    job = client.query(query)
    job.result()
    print("key_events table created in BigQuery — top 10 events per year")


# ── 7. Fetch News for Key Events ──────────────────────────────────────────────

def fetch_news(key_dates, project_id, dataset_id, client, save_path):
    """
    Scrape Google News RSS for each key event date.
    Fetches up to 3 headlines per date, saves to CSV and uploads to BigQuery.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    all_news = []

    for date in key_dates:
        dt     = datetime.strptime(date, "%Y-%m-%d")
        before = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
        after  = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
        url    = f"https://news.google.com/rss/search?q=bitcoin+after:{after}+before:{before}&hl=en-US&gl=US&ceid=US:en"

        try:
            response = requests.get(url, headers=headers, timeout=10)
            soup     = BeautifulSoup(response.text, "xml")
            items    = soup.find_all("item")

            for item in items[:3]:
                all_news.append({
                    "event_date": date,
                    "title":      item.title.text,
                    "pub_date":   item.pubDate.text,
                    "source":     item.source.text if item.source else None,
                    "link":       item.link.text if item.link else None
                })

            print(f"{date}: found {len(items)} articles, saved 3")
            time.sleep(1)

        except Exception as e:
            print(f"{date}: error — {e}")

    news_df = pd.DataFrame(all_news)
    news_df.to_csv(f"{save_path}/key_events_news.csv", index=False)
    print(f"key_events_news.csv saved — {len(news_df)} rows")

    # Upload to BigQuery
    table_ref  = f"{project_id}.{dataset_id}.key_events_news"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(news_df, table_ref, job_config=job_config)
    job.result()
    print(f"key_events_news uploaded to BigQuery — {len(news_df)} rows")

    return news_df


# ── 8. Export Final Tables from BigQuery to CSV ───────────────────────────────

def export_tables(client, project_id, dataset_id, save_path):
    """
    Export all four final tables from BigQuery to CSV.
    Also merges key_events with news titles into a single export.
    """
    tables_to_export = {
        "prices":          f"SELECT * FROM `{project_id}.{dataset_id}.prices`",
        "metrics":         f"SELECT * FROM `{project_id}.{dataset_id}.metrics`",
        "key_events":      f"SELECT * FROM `{project_id}.{dataset_id}.key_events`",
        "key_events_news": f"SELECT * FROM `{project_id}.{dataset_id}.key_events_news`",
    }

    for table_name, query in tables_to_export.items():
        df        = client.query(query).to_dataframe()
        file_path = f"{save_path}/{table_name}_export.csv"
        df.to_csv(file_path, index=False)
        print(f"{table_name}_export.csv saved — {len(df)} rows")

    # Merge key_events with news titles
    merge_query = f"""
    SELECT
        k.date,
        k.year,
        k.rank,
        k.anomaly_count,
        k.anomaly_types,
        k.severity_score,
        k.total_score,
        STRING_AGG(n.title, ' | ' ORDER BY n.title) AS news_titles,
        STRING_AGG(n.source, ' | ' ORDER BY n.source) AS news_sources
    FROM `{project_id}.{dataset_id}.key_events` AS k
    LEFT JOIN `{project_id}.{dataset_id}.key_events_news` AS n
        ON k.date = DATE(n.event_date)
    GROUP BY k.date, k.year, k.rank, k.anomaly_count, k.anomaly_types, k.severity_score, k.total_score
    ORDER BY k.year ASC, k.rank ASC
    """

    key_events_with_news = client.query(merge_query).to_dataframe()
    key_events_with_news.to_csv(f"{save_path}/key_events_with_news.csv", index=False)
    print(f"key_events_with_news.csv saved — {len(key_events_with_news)} rows")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    os.makedirs(PROJECT_PATH, exist_ok=True)
    client = bigquery.Client(project=PROJECT_ID)

    # 1. Fetch prices
    prices = fetch_prices(TICKERS, START_DATE, END_DATE, PROJECT_PATH)

    # 2. Compute metrics
    metrics = calc_metrics(prices, PROJECT_PATH)

    # 3. Fetch Fear & Greed Index
    fng = fetch_fear_greed(START_DATE, END_DATE, PROJECT_PATH)

    # 4. Fetch VIX
    vix = fetch_vix(START_DATE, END_DATE, PROJECT_PATH)

    # 5. Upload raw tables to BigQuery
    upload_to_bigquery(client, PROJECT_ID, DATASET_ID, PROJECT_PATH)

    # 6. Build key events table in BigQuery
    build_key_events(client, PROJECT_ID, DATASET_ID)

    # 7. Fetch news for key events
    key_events_df = client.query(
        f"SELECT CAST(date AS STRING) AS date FROM `{PROJECT_ID}.{DATASET_ID}.key_events`"
    ).to_dataframe()
    key_dates = key_events_df["date"].tolist()
    fetch_news(key_dates, PROJECT_ID, DATASET_ID, client, PROJECT_PATH)

    # 8. Export all final tables to CSV
    export_tables(client, PROJECT_ID, DATASET_ID, PROJECT_PATH)

    print("\nAll done ✅")
