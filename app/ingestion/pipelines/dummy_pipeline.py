import os

import polars as pl

# ===============================
# 🔐 Configurações do MinIO
# ===============================
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_BUCKET = "datalake"
S3_DELTA_PATH = f"s3://{MINIO_BUCKET}/dummy-test-table"

os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["AWS_REGION"] = (
    "us-east-1"  # necessário mesmo que MinIO não use regiões reais
)

# ===============================
# 1. Criar DataFrame simulado
# ===============================
df = pl.DataFrame(
    {
        "customer": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "price": [100, 200, 150, 300, 50],
        "quantity": [1, 2, 1, 4, 5],
    }
).with_columns((pl.col("price") * pl.col("quantity")).alias("total"))

# ===============================
# 2. Escrever como Delta no MinIO
# ===============================
print("🚀 Escrevendo Delta Table no MinIO...")

df.write_delta(
    S3_DELTA_PATH,
    mode="overwrite",
    storage_options={
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "endpoint_url": MINIO_ENDPOINT,  # importante para MinIO
    },
    delta_write_options={
        "schema_mode": "overwrite",
    },
)

print("✅ Escrita concluída.")

# ===============================
# 3. Leitura com scan_delta + filtro
# ===============================
print("🔍 Lendo Delta Table do MinIO com filtro lazy...")

scan = (
    pl.scan_delta(
        S3_DELTA_PATH,
        storage_options={
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "endpoint_url": MINIO_ENDPOINT,
        },
    )
    .filter(pl.col("total") > 300)
    .select([pl.col("customer"), pl.col("total")])
)

result = scan.collect()

print("📊 Resultado filtrado:")
print(result)
