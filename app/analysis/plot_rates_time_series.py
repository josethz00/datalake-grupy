import matplotlib.pyplot as plt
import polars as pl

from app.lake.scan_delta import scan_delta


def main():
    print("📦 Lendo dados do Delta Lake...")
    df = (
        scan_delta("exchange_rates")
        .filter(pl.col("currency").is_in(["USD", "EUR", "GBP"]))
        .sort("date")
        .collect()
    )

    print("✅ Dados carregados:", df.shape)

    fig, ax = plt.subplots(figsize=(14, 7))

    fig.patch.set_facecolor("#f9f9f9")
    ax.set_facecolor("#ffffff")

    currencies = df["currency"].unique()

    soft_colors = [
        "#3C6EED",
        "#F4A261",
        "#2A9D8F",
    ]

    for i, currency in enumerate(currencies):
        df_currency = df.filter(pl.col("currency") == currency)
        plt.plot(
            df_currency["date"],
            df_currency["rate"],
            label=f"{currency} (R$)",
            color=soft_colors[i % len(soft_colors)],
            linewidth=1.5,
            marker="",
            alpha=0.9,
        )

    plt.title(
        "Taxas de Câmbio das Principais Moedas (USD, EUR, GBP) vs. Real Brasileiro",
        fontsize=18,
        fontweight="bold",
        color="#444444",
    )

    plt.xlabel("Data", fontsize=14, color="#555555")
    plt.ylabel("Taxa de Câmbio (R$)", fontsize=14, color="#555555")

    legend = plt.legend(
        loc="upper left",
        fontsize=11,
        frameon=True,
        shadow=True,
        facecolor="#ffffff",
        edgecolor="#cccccc",
    )

    for text in legend.get_texts():
        text.set_color("#555555")

    plt.grid(True, linestyle="-", alpha=0.4, color="#cccccc")

    ax.tick_params(axis="x", colors="#666666")
    ax.tick_params(axis="y", colors="#666666")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#cccccc")
    ax.spines["bottom"].set_color("#cccccc")

    plt.tight_layout()

    plt.show()


if __name__ == "__main__":
    main()
