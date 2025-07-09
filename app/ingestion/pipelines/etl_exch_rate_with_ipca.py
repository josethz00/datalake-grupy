import io
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import polars as pl
import requests
from prefect import flow, task

# --- CONFIGS ---
MINIO_ENDPOINT = "http://localhost:9000"
S3_DELTA_PATH = "s3://datalake/exchange_rates"

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "endpoint_url": MINIO_ENDPOINT,
}
DATE_FORMAT_MASK = "%d/%m/%Y"


def download_currency_batch(
    year: int, series_id: int, currency_name: str
) -> pl.DataFrame | None:
    start_date = f"01/01/{year}"
    end_date = (
        f"31/12/{year}"
        if year < datetime.today().year
        else datetime.today().strftime(DATE_FORMAT_MASK)
    )

    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
        f"?formato=csv&dataInicial={start_date}&dataFinal={end_date}"
    )

    try:
        response = requests.get(url, timeout=10)
        response.encoding = "utf-8"
        df = pl.read_csv(io.StringIO(response.text), separator=";")

        if df.is_empty():
            print(f"⚠️ {currency_name} {year}: vazio.")
            return None

        print(f"✅ {currency_name} {year}: OK")
        return df.with_columns(
            [
                pl.col("data").str.strptime(pl.Date, DATE_FORMAT_MASK).alias("date"),
                pl.col("valor").str.replace(",", ".").cast(pl.Float64).alias("rate"),
                pl.lit(currency_name).alias("currency"),
            ]
        ).select(["date", "currency", "rate"])

    except Exception as e:
        print(f"❌ Erro {currency_name} {year}: {e}")
        return None


@task
def fetch_exchange_currency_all_years(
    series_id: int, currency_name: str, start_year: int = 1999
) -> pl.DataFrame:
    print(f"\n🚀 Iniciando download de {currency_name} desde {start_year}...\n")
    start = time.perf_counter()

    end_year = datetime.today().year
    years = list(range(start_year, end_year + 1))

    with ThreadPoolExecutor(max_workers=12) as executor:
        dfs = list(
            executor.map(
                lambda y: download_currency_batch(y, series_id, currency_name), years
            )
        )

    elapsed = time.perf_counter() - start
    print(
        f"\n✅ Finalizado {currency_name} ({len([d for d in dfs if d is not None])} anos) em {elapsed:.2f}s\n"
    )
    return pl.concat([df for df in dfs if df is not None])


@task
def fetch_ipca_data(start_year: int = 1999) -> pl.DataFrame:
    print(f"\n📥 Buscando IPCA desde {start_year}...\n")
    start = time.perf_counter()

    today = datetime.today()
    start_date = f"01/01/{start_year}"
    end_date = today.strftime(DATE_FORMAT_MASK)

    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados"
        f"?formato=csv&dataInicial={start_date}&dataFinal={end_date}"
    )

    response = requests.get(url, timeout=10)
    response.encoding = "utf-8"
    df = pl.read_csv(io.StringIO(response.text), separator=";")

    df = df.with_columns(
        [
            pl.col("data").str.strptime(pl.Date, DATE_FORMAT_MASK).alias("month_date"),
            pl.col("valor")
            .str.replace(",", ".")
            .cast(pl.Float64)
            .alias("monthly_inflation"),
        ]
    ).select(["month_date", "monthly_inflation"])

    elapsed = time.perf_counter() - start
    print(f"✅ IPCA carregado com {df.shape[0]} meses em {elapsed:.2f}s\n")
    return df


@task
def join_and_write(currency_df: pl.DataFrame, ipca_df: pl.DataFrame) -> None:
    print("🔗 Fazendo join e salvando no Delta Lake...\n")
    start = time.perf_counter()

    df = (
        currency_df.with_columns(pl.col("date").dt.truncate("1mo").alias("month_date"))
        .join(ipca_df, on="month_date", how="left")
        .select(["date", "currency", "rate", "monthly_inflation"])
    )

    df.write_delta(
        S3_DELTA_PATH,
        mode="overwrite",
        storage_options=STORAGE_OPTIONS,
        delta_write_options={"schema_mode": "overwrite"},
    )

    elapsed = time.perf_counter() - start
    print(f"✅ Dados salvos no Delta Lake com {df.shape[0]} linhas em {elapsed:.2f}s\n")


@flow(name="Mini Data Lake - Blazing Fast")
def pipeline():
    print("\n🚀 Iniciando ETL completa...\n")
    start_all = time.perf_counter()

    usd = fetch_exchange_currency_all_years(1, "USD")
    eur = fetch_exchange_currency_all_years(21619, "EUR")
    gbp = fetch_exchange_currency_all_years(21623, "GBP")
    cad = fetch_exchange_currency_all_years(21635, "CAD")
    jpy = fetch_exchange_currency_all_years(21621, "JPY")

    all_currency = pl.concat([usd, eur, gbp, cad, jpy])
    print(f"\n🧾 Total de linhas de câmbio: {all_currency.shape[0]}\n")

    ipca_df = fetch_ipca_data()
    join_and_write(all_currency, ipca_df)

    total_time = time.perf_counter() - start_all
    print(f"\n🏁 Pipeline finalizada em {total_time:.2f}s\n")


if __name__ == "__main__":
    pipeline()
