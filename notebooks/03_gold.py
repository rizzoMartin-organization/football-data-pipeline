# Databricks notebook source
from pyspark.sql.functions import current_timestamp, count, round, when, col, avg, rank
from pyspark.sql.window import Window

# COMMAND ----------

df_classification = (
  spark.read
  .format("delta")
  .table("football.silver.standings")
  .drop("ingestion_timestamp","ingestion_date","silver_timestamp","season_id","season_end_date","team_id","team_short_name","team_tla")
  .withColumn("gold_timestamp", current_timestamp())
)

df_classification.write.mode("overwrite").format("delta").saveAsTable("football.gold.classification")

# COMMAND ----------

print(f"Gold classification: {df_classification.count()} registros")

# COMMAND ----------

df_matches = (
    spark.read
    .format("delta")
    .table("football.silver.matches")
)


# COMMAND ----------

df_performance = (
    df_matches.groupBy("competition_code", "competition_name", "season_start_date")
    .agg(
        count("*").alias("total_matches"),
        count(when(col("winner") == "HOME_TEAM", 1)).alias("home_wins"),
        count(when(col("winner") == "AWAY_TEAM", 1)).alias("away_wins"),
        count(when(col("winner") == "DRAW", 1)).alias("draws")
    )
    .withColumn("home_win_pct", round(col("home_wins") / col("total_matches") * 100, 2))
    .withColumn("away_win_pct", round(col("away_wins") / col("total_matches") * 100, 2))
    .withColumn("draw_pct", round(col("draws") / col("total_matches") * 100, 2))
    .withColumn("gold_timestamp", current_timestamp())
)

df_performance.write.mode("overwrite").format("delta").saveAsTable("football.gold.performance_home_away")

# COMMAND ----------

print(f"Gold performance: {df_performance.count()} registros")

# COMMAND ----------

df_evolution = (
    df_matches.groupBy("competition_code", "competition_name", "season_start_date", "matchday")
    .agg(
        count("*").alias("total_matches"),
        round(avg(col("score_full_time_home") + col("score_full_time_away")), 2).alias("avg_goals_per_match")
    )
    .withColumn("gold_timestamp", current_timestamp())
)

df_evolution.write.mode("overwrite").format("delta").saveAsTable("football.gold.evolution_goals")

# COMMAND ----------

print(f"Gold evolution: {df_evolution.count()} registros")

# COMMAND ----------

df_scorers = (
    spark.read
    .format("delta")
    .table("football.silver.scorers")
)

window = Window.partitionBy("competition_code","season_start_date").orderBy(col("goals").desc())

df_ranking = (
    df_scorers
    .withColumn("rank", rank().over(window))
    .filter(col("rank") <= 10)
    .drop("ingestion_timestamp","ingestion_date","silver_timestamp","player_first_name","player_last_name","player_last_updated","player_shirt_number","rank", "season_end_date","team_id","team_short_name","team_tla","player_id","season_id")
    .withColumn("gold_timestamp", current_timestamp())
)

df_ranking.write.mode("overwrite").format("delta").saveAsTable("football.gold.ranking_scorers")

# COMMAND ----------

print(f"Gold ranking scorers: {df_ranking.count()} registros")