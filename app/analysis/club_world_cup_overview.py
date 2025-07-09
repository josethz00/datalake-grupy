import matplotlib.pyplot as plt
import polars as pl

from app.lake.scan_delta import scan_delta

# Carrega dados
df = scan_delta("club_world_cup_20250709").collect()

# Parse da data e gols separados
df = df.with_columns(
    [
        pl.col("Date").str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M").alias("date"),
        pl.col("Result")
        .str.split(" - ")
        .list.first()
        .cast(pl.Int8)
        .alias("home_goals"),
        pl.col("Result").str.split(" - ").list.last().cast(pl.Int8).alias("away_goals"),
    ]
)

# Placar formatado "x - y"
df = df.with_columns(
    (
        pl.col("home_goals").cast(pl.Utf8) + " - " + pl.col("away_goals").cast(pl.Utf8)
    ).alias("score")
)

# Média de gols por time (somando gols feitos tanto como "home" quanto "away")
goals_home = df.select(
    [
        pl.col("Home Team").alias("team"),
        pl.col("home_goals").alias("goals"),
    ]
)
goals_away = df.select(
    [
        pl.col("Away Team").alias("team"),
        pl.col("away_goals").alias("goals"),
    ]
)
goals_all = pl.concat([goals_home, goals_away])

avg_goals_by_team = (
    goals_all.group_by("team")
    .agg(pl.col("goals").mean().alias("avg_goals"))
    .sort("avg_goals", descending=True)
)

# Placar mais comum
score_counts = df["score"].value_counts().sort("count", descending=True)

# Média de gols por rodada
goals_by_round = (
    df.group_by("Round Number")
    .agg((pl.col("home_goals") + pl.col("away_goals")).mean().alias("avg_goals"))
    .sort("Round Number")
)

# Jogos por local
games_by_location = df.group_by("Location").count().sort("count", descending=True)

# Plotagem

fig, axes = plt.subplots(2, 2, figsize=(18, 14), facecolor="#f9f9f9")

# Gráfico 1: Top 10 placares mais comuns
top_scores = score_counts.head(10)
axes[0, 0].bar(top_scores["score"], top_scores["count"], color="#264653")
axes[0, 0].set_title(
    "Top 10 Placares Mais Comuns", fontsize=14, weight="semibold", color="#333"
)
axes[0, 0].set_ylabel("Número de jogos", fontsize=12, color="#555")
axes[0, 0].set_xticklabels(top_scores["score"], rotation=45, ha="right", fontsize=11)
axes[0, 0].tick_params(axis="both", which="major", labelsize=11)
axes[0, 0].grid(axis="y", linestyle="--", alpha=0.3)
# remove spines
axes[0, 0].spines["top"].set_visible(False)
axes[0, 0].spines["right"].set_visible(False)

# Gráfico 2: Média de gols por time (top 10)
axes[0, 1].barh(
    avg_goals_by_team["team"].head(10),
    avg_goals_by_team["avg_goals"].head(10),
    color="#2A9D8F",
)
axes[0, 1].set_title(
    "Média de Gols por Time (Top 10)", fontsize=14, weight="semibold", color="#333"
)
axes[0, 1].invert_yaxis()
axes[0, 1].tick_params(axis="both", which="major", labelsize=11)
axes[0, 1].grid(axis="x", linestyle="--", alpha=0.3)
axes[0, 1].spines["top"].set_visible(False)
axes[0, 1].spines["right"].set_visible(False)

# Gráfico 3: Média de gols por rodada
axes[1, 0].bar(
    goals_by_round["Round Number"],
    goals_by_round["avg_goals"],
    color="#F4A261",
)
axes[1, 0].set_title(
    "Média de Gols por Rodada", fontsize=14, weight="semibold", color="#333"
)
axes[1, 0].set_xlabel("Rodada", fontsize=12, color="#555")
axes[1, 0].set_ylabel("Gols por jogo", fontsize=12, color="#555")
axes[1, 0].tick_params(axis="both", which="major", labelsize=11)
axes[1, 0].grid(axis="y", linestyle="--", alpha=0.3)
axes[1, 0].spines["top"].set_visible(False)
axes[1, 0].spines["right"].set_visible(False)

# Gráfico 4: Top 10 locais com mais jogos
axes[1, 1].barh(
    games_by_location["Location"].head(10),
    games_by_location["count"].head(10),
    color="#E76F51",
)
axes[1, 1].set_title(
    "Top 10 Locais com Mais Jogos", fontsize=14, weight="semibold", color="#333"
)
axes[1, 1].invert_yaxis()
axes[1, 1].tick_params(axis="both", which="major", labelsize=11)
axes[1, 1].grid(axis="x", linestyle="--", alpha=0.3)
axes[1, 1].spines["top"].set_visible(False)
axes[1, 1].spines["right"].set_visible(False)

# Espaçamento entre os gráficos
plt.subplots_adjust(
    left=0.07, right=0.95, top=0.93, bottom=0.08, hspace=0.35, wspace=0.25
)

plt.show()
