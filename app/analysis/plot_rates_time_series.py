import matplotlib.pyplot as plt
import polars as pl

from app.lake.scan_delta import scan_delta


def main():
    print("📦 Lendo dados do Delta Lake...")
    # Load data from Delta Lake, filter for specific currencies, sort by date, and collect into a Polars DataFrame.
    df = (
        scan_delta("exchange_rates")
        .filter(pl.col("currency").is_in(["USD", "EUR", "GBP"]))
        .sort("date")
        .collect()
    )

    print("✅ Dados carregados:", df.shape)

    # We will not use a predefined style like 'ggplot' to allow for more custom soft styling.
    # plt.style.use('ggplot') # Removed to allow for a softer, custom look

    # Create a figure and a set of subplots for the plot.
    # figsize (width, height) in inches.
    fig, ax = plt.subplots(figsize=(14, 7))

    # Set a very light, soft background color for the figure to enhance the overall soft feel.
    fig.patch.set_facecolor("#f9f9f9")  # Light grey/off-white for a soft background
    ax.set_facecolor("#ffffff")  # White background for the plot area

    # Get unique currencies to iterate through them.
    currencies = df["currency"].unique()

    # Define a custom soft color palette for better visual distinction.
    # These colors are chosen for their muted, pastel-like qualities.
    # Updated to a new set of soft, earthy/muted tones.
    soft_colors = [
        "#3C6EED",
        "#F4A261",
        "#2A9D8F",
    ]  # Soft Teal, Muted Orange, Dark Teal/Green

    # Iterate over each currency to plot its exchange rate.
    for i, currency in enumerate(currencies):
        # Filter the DataFrame for the current currency.
        df_currency = df.filter(pl.col("currency") == currency)
        # Plot the date vs. rate, assigning a specific soft color.
        plt.plot(
            df_currency["date"],
            df_currency["rate"],
            label=f"{currency} (R$)",  # More descriptive label
            color=soft_colors[
                i % len(soft_colors)
            ],  # Cycle through defined soft colors
            linewidth=1.5,  # Made lines more slim for a lighter feel
            marker="",  # Removed markers for a cleaner, softer line
            alpha=0.9,  # Slightly more opaque lines for better visibility
        )

    # Set the title of the plot with a softer font color.
    plt.title(
        "Taxas de Câmbio das Principais Moedas (USD, EUR, GBP) vs. Real Brasileiro",
        fontsize=18,
        fontweight="bold",
        color="#444444",  # Dark grey for a softer title
    )
    # Set the label for the X-axis (Date) with a softer font color.
    plt.xlabel("Data", fontsize=14, color="#555555")  # Medium grey for labels
    # Set the label for the Y-axis (Exchange Rate) with a softer font color.
    plt.ylabel("Taxa de Câmbio (R$)", fontsize=14, color="#555555")

    # Add a legend to identify each currency line.
    # loc='upper left' places the legend in a less obstructive position.
    # frameon=True adds a background frame to the legend.
    # Set legend text color to be soft.
    legend = plt.legend(
        loc="upper left",
        fontsize=11,
        frameon=True,
        shadow=True,
        facecolor="#ffffff",  # White background for legend
        edgecolor="#cccccc",  # Light grey border for legend
    )
    # Apply color to legend text
    for text in legend.get_texts():
        text.set_color("#555555")

    # Enable the grid for easier reading of values, using a very light and transparent color.
    plt.grid(True, linestyle="-", alpha=0.4, color="#cccccc")  # Lighter grid lines

    # Adjust tick parameters for softer appearance.
    ax.tick_params(axis="x", colors="#666666")  # Soft grey for x-axis ticks
    ax.tick_params(axis="y", colors="#666666")  # Soft grey for y-axis ticks

    # Remove the top and right spines for a cleaner look.
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#cccccc")  # Soft grey for left spine
    ax.spines["bottom"].set_color("#cccccc")  # Soft grey for bottom spine

    # Automatically adjust subplot parameters for a tight layout.
    plt.tight_layout()

    # Display the plot.
    plt.show()


if __name__ == "__main__":
    main()
