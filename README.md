# 🛢️ Building a Data Lake with Python and Polars - Grupy-SP

This project was built for a talk at [Grupy-SP](https://www.grupysp.org/) on **July 10, 2025**, and showcases how to build a lightweight data lake using **Python**, **Polars**, and **MinIO**.

The project includes:
- A Dockerized MinIO server (S3-compatible object storage)
- ETL pipelines for economic and sports data
- Time series and chart visualizations with `matplotlib`
- A minimal FastAPI interface for data access (optional)

---

## 🧱 Tech Stack

- **Python 3.13**
- **[Polars](https://pola.rs/)** for fast and expressive data processing
- **MinIO** as local object storage (S3-compatible)
- **FastAPI** for exposing APIs (optional)
- **Matplotlib** for visualizations

---

## 🚀 Quick Start

### 1. Start the Data Lake

This spins up a local MinIO instance and creates a public bucket called `datalake`.

```bash
docker compose up
```

Access MinIO Console at: http://localhost:9001

Login: `minioadmin` / `minioadmin`

### 2. Run Ingestion Pipelines (ETL)

Use `make` or run the scripts directly to ingest and transform datasets.

```bash
make run-ingestion-pipeline
```

This pipeline fetches and stores exchange rate and inflation (IPCA) data into the data lake.

### 3. Run Data Analysis

Generate visualizations from the data stored in the lake:

#### 📈 Economic Indicators (Exchange Rates & IPCA)

```bash
make run-analysis-exchange-rates
```

#### ⚽ Club World Cup Overview

```bash
make run-analysis-club-world-cup
```

### 4. (Optional) Start FastAPI

Launch a basic API to expose an endpoint to upload data:

```bash
make run-api
```

## 📁 Project Structure

```bash
.
├── app/
│   ├── ingestion/
│   │   ├── api/                     # FastAPI app
│   │   └── pipelines/              # ETL scripts
│   └── analysis/                   # Analysis & visualization
├── docker-compose.yml              # MinIO setup
├── Makefile                        # Easy task runner
└── README.md
```

## 🗃️ Bucket Info

- **Bucket Name**: datalake

- **Public Access**: Enabled for demonstration purposes

- **Stored Formats**: Parquet (via Polars and Delta Lake), CSV (when required)

## 🙋‍♂️ About the Talk

This is a lightweight demo of how to structure an analytical data lake without heavyweight tools like Spark or Hadoop. The stack is intentionally minimal to be accessible, fast, and educational.

For questions, suggestions, or if you'd like to collaborate, feel free to reach out after the talk!
