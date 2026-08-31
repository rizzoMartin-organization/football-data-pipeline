# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType
from pyspark.sql.functions import from_json, col, current_timestamp, explode, coalesce, lit, to_timestamp

# COMMAND ----------

df_matches_raw = spark.read.format("delta").table("bronze.football_matches")
df_standings_raw = spark.read.format("delta").table("bronze.football_standings")
df_scorers_raw = spark.read.format("delta").table("bronze.football_scorers")

# COMMAND ----------

# Matches Schema
schema_matches = StructType([
    StructField("id", LongType(), False),
    StructField("utcDate", StringType(), False),
    StructField("status", StringType(), False),
    StructField("matchday", IntegerType(), False),
    StructField("stage", StringType(), False),
    StructField("competition", StructType([
        StructField("code", StringType(), False),
        StructField("name", StringType(), False)
    ]), False),
    StructField("season", StructType([
        StructField("id", LongType(), False),
        StructField("startDate", StringType(), False),
        StructField("endDate", StringType(), False)
    ]), False),
    StructField("homeTeam", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("shortName", StringType(), True),
        StructField("tla", StringType(), True)

    ]), True),
    StructField("awayTeam", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("shortName", StringType(), True),
        StructField("tla", StringType(), True)

    ]), True),
    StructField("score", StructType([
        StructField("winner", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("fullTime", StructType([
            StructField("home", IntegerType(), True),
            StructField("away", IntegerType(), True),
        ]), True),
        StructField("halfTime", StructType([
            StructField("home", IntegerType(), True),
            StructField("away", IntegerType(), True)
        ]), True)
    ]), True)
])


# COMMAND ----------

# Standings Schema
schema_standings = StructType([
    StructField("competition", StructType([
        StructField("code", StringType(), False),
        StructField("name", StringType(), False)
    ]), False),
    StructField("season", StructType([
        StructField("id", LongType(), False),
        StructField("startDate", StringType(), False),
        StructField("endDate", StringType(), False)
    ]), False),
    StructField("standings", ArrayType(StructType([
        StructField("table", ArrayType(StructType([
            StructField("position", IntegerType(), True),
            StructField("team", StructType([
                StructField("id", LongType(), True),
                StructField("name", StringType(), True),
                StructField("shortName", StringType(), True),
                StructField("tla", StringType(), True)
            ]), True),
            StructField("playedGames", IntegerType(), True),
            StructField("won", IntegerType(), True),
            StructField("draw", IntegerType(), True),
            StructField("lost", IntegerType(), True),
            StructField("points", IntegerType(), True),
            StructField("goalsFor", IntegerType(), True),
            StructField("goalsAgainst", IntegerType(), True),
            StructField("goalDifference", IntegerType(), True)
        ])), True)
    ])), True)
])


# COMMAND ----------

# Scorers Schema
schema_scorers = StructType([
    StructField("competition", StructType([
        StructField("code", StringType(), False),
        StructField("name", StringType(), False)
    ]), False),
    StructField("season", StructType([
        StructField("id", LongType(), False),
        StructField("startDate", StringType(), False),
        StructField("endDate", StringType(), False)
    ]), False),
    StructField("scorers", ArrayType(StructType([
        StructField("player", StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), False),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("dateOfBirth", StringType(), True),
            StructField("nationality", StringType(), True),
            StructField("position", StringType(), True),
            StructField("shirtNumber", IntegerType(), True),
            StructField("lastUpdated", StringType(), True)
        ]), True),
        StructField("team", StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("shortName", StringType(), True),
            StructField("tla", StringType(), True)
        ]), True),
        StructField("playedMatches", IntegerType(), True),
        StructField("goals", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("penalties", IntegerType(), True)
    ]), False))
])

# COMMAND ----------

df_matches_parsed = df_matches_raw.withColumn("data", from_json(col("data"), schema_matches))

df_standings_parsed = df_standings_raw.withColumn("data", from_json(col("data"), schema_standings))
df_standings_exploded = df_standings_parsed.withColumn("standings", explode(col("data.standings")))
df_standings_exploded = df_standings_exploded.withColumn("table", explode(col("standings.table")))

df_scorers_parsed = df_scorers_raw.withColumn("data", from_json(col("data"), schema_scorers))
df_scorers_exploded = df_scorers_parsed.withColumn("scorers", explode(col("data.scorers")))

# COMMAND ----------

df_matches_silver = df_matches_parsed.select(
    col("data.id").alias("match_id"),
    col("data.utcDate").alias("utc_date"),
    col("data.status"),
    col("data.matchday"),
    col("data.stage"),
    col("data.competition.code").alias("competition_code"),
    col("data.competition.name").alias("competition_name"),
    col("data.season.id").alias("season_id"),
    col("data.season.startDate").alias("season_start_date"),
    col("data.season.endDate").alias("season_end_date"),
    col("data.homeTeam.id").alias("home_team_id"),
    col("data.homeTeam.name").alias("home_team_name"),
    col("data.homeTeam.shortName").alias("home_team_short_name"),
    col("data.homeTeam.tla").alias("home_team_tla"),
    col("data.awayTeam.id").alias("away_team_id"),
    col("data.awayTeam.name").alias("away_team_name"),
    col("data.awayTeam.shortName").alias("away_team_short_name"),
    col("data.awayTeam.tla").alias("away_team_tla"),
    col("data.score.winner").alias("winner"),
    col("data.score.fullTime.home").alias("score_full_time_home"),
    col("data.score.fullTime.away").alias("score_full_time_away"),
    col("data.score.halfTime.home").alias("score_half_time_home"),
    col("data.score.halfTime.away").alias("score_half_time_away"),
    col("ingestion_timestamp"),
    col("ingestion_date"),
    current_timestamp().alias("silver_timestamp")
)

# COMMAND ----------

df_standings_silver = df_standings_exploded.select(
    col("data.competition.code").alias("competition_code"),
    col("data.competition.name").alias("competition_name"),
    col("data.season.id").alias("season_id"),
    col("data.season.startDate").alias("season_start_date"),
    col("data.season.endDate").alias("season_end_date"),
    col("table.position").alias("position"),
    col("table.team.id").alias("team_id"),
    col("table.team.name").alias("team_name"),
    col("table.team.shortName").alias("team_short_name"),
    col("table.team.tla").alias("team_tla"),
    col("table.playedGames").alias("played_games"),
    col("table.won").alias("won"),
    col("table.draw").alias("draw"),
    col("table.lost").alias("lost"),
    col("table.points").alias("points"),
    col("table.goalsFor").alias("goals_for"),
    col("table.goalsAgainst").alias("goals_against"),
    col("table.goalDifference").alias("goal_difference"),
    col("ingestion_timestamp"),
    col("ingestion_date"),
    current_timestamp().alias("silver_timestamp")
)

# COMMAND ----------

df_scorers_silver = df_scorers_exploded.select(
    col("data.competition.code").alias("competition_code"),
    col("data.competition.name").alias("competition_name"),
    col("data.season.id").alias("season_id"),
    col("data.season.startDate").alias("season_start_date"),
    col("data.season.endDate").alias("season_end_date"),
    col("scorers.player.id").alias("player_id"),
    col("scorers.player.name").alias("player_name"),
    col("scorers.player.firstName").alias("player_first_name"),
    col("scorers.player.lastName").alias("player_last_name"),
    col("scorers.player.dateOfBirth").alias("player_date_of_birth"),
    col("scorers.player.nationality").alias("player_nationality"),
    col("scorers.player.position").alias("player_position"),
    col("scorers.player.shirtNumber").alias("player_shirt_number"),
    col("scorers.player.lastUpdated").alias("player_last_updated"),
    col("scorers.team.id").alias("team_id"),
    col("scorers.team.name").alias("team_name"),
    col("scorers.team.shortName").alias("team_short_name"),
    col("scorers.team.tla").alias("team_tla"),
    col("scorers.playedMatches").alias("played_matches"),
    col("scorers.goals").alias("goals"),
    col("scorers.assists").alias("assists"),
    col("scorers.penalties").alias("penalties"),
    col("ingestion_timestamp"),
    col("ingestion_date"),
    current_timestamp().alias("silver_timestamp")
)

# COMMAND ----------

df_matches_silver = df_matches_silver.filter(col("status") == "FINISHED")

df_matches_silver = (
    df_matches_silver
    .withColumn("utc_date", to_timestamp(col("utc_date")))
)

df_matches_silver = df_matches_silver.dropDuplicates(["match_id"])

df_scorers_silver = (
    df_scorers_silver
    .withColumn("player_last_updated", to_timestamp(col("player_last_updated")))
    .withColumn("assists", coalesce(col("assists"), lit(0)))
    .withColumn("penalties", coalesce(col("penalties"), lit(0)))
)

# COMMAND ----------

# Matches
if not spark.catalog.tableExists("football.silver.matches"):
    df_matches_silver.write.format("delta").saveAsTable("football.silver.matches")
else:
    df_matches_silver.createOrReplaceTempView("matches_silver_temp")
    spark.sql("""
        MERGE INTO football.silver.matches AS target
        USING matches_silver_temp AS source
        ON target.match_id = source.match_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

print(f"Silver matches: {df_matches_silver.count()} processed records")

# COMMAND ----------

# Standings
if not spark.catalog.tableExists("football.silver.standings"):
    df_standings_silver.write.format("delta").saveAsTable("football.silver.standings")
else:
    df_standings_silver.createOrReplaceTempView("standings_silver_temp")
    spark.sql("""
        MERGE INTO football.silver.standings AS target
        USING standings_silver_temp AS source
        ON target.team_id = source.team_id
        AND target.season_id = source.season_id
        AND target.competition_code = source.competition_code
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

print(f"Silver standings: {df_standings_silver.count()} processed records")

# COMMAND ----------

# Scorers
if not spark.catalog.tableExists("football.silver.scorers"):
    df_scorers_silver.write.format("delta").saveAsTable("football.silver.scorers")
else:
    df_scorers_silver.createOrReplaceTempView("scorers_silver_temp")
    spark.sql("""
        MERGE INTO football.silver.scorers AS target
        USING scorers_silver_temp AS source
        ON target.player_id = source.player_id
        AND target.season_id = source.season_id
        AND target.competition_code = source.competition_code
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

print(f"Silver scorers: {df_scorers_silver.count()} processed records")