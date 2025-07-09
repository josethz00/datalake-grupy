# Run FastAPI API
run-api:
	PYTHONPATH=. python3 app/ingestion/api/__init__.py

# Run ingestion pipeline (ETL)
run-ingestion-pipeline:
	PYTHONPATH=. python3 app/ingestion/pipelines/etl_exch_rate_with_ipca.py

# Run analysis script to plot time series
run-analysis-exchange-rates:
	PYTHONPATH=. python3 app/analysis/plot_rates_time_series.py

# Run analysis script to plot charts about the Club World Cup
run-analysis-club-world-cup:
	PYTHONPATH=. python3 app/analysis/club_world_cup_overview.py
