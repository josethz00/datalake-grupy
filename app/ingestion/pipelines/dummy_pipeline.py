import polars as pl

from app.lake.scan_delta import scan_delta
from app.lake.write_delta import WriteMode, write_delta

df = pl.DataFrame(
    {
        "customer": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "price": [100, 200, 150, 300, 50],
        "quantity": [1, 2, 1, 4, 5],
    }
).with_columns((pl.col("price") * pl.col("quantity")).alias("total"))

print("🚀 Escrevendo Delta Table no MinIO...")

write_delta(
    df,
    table="dummy-test-table",
    write_mode=WriteMode.OVERWRITE,
)

print("✅ Escrita concluída.")

print("🔍 Lendo Delta Table do MinIO com filtro lazy...")

scan = (
    scan_delta(
        "dummy-test-table",
    )
    .filter(pl.col("total") > 300)
    .select([pl.col("customer"), pl.col("total")])
)

result = scan.collect()

print("📊 Resultado filtrado:")
print(result)
