# Databricks notebook source
import requests
from datetime import date
import time
from pyspark.sql.functions import lit, current_timestamp
import json

today_date = str(date.today())

# Read API key from Databricks Secrets
api_key = dbutils.secrets.get(scope="football_data_api_key", key="football_data_api_key")
# Authentication Header
headers = { "X-Auth-Token": api_key }

ligas = ["PD", "SA", "PL", "BL1", "FL1"]

# COMMAND ----------

# This request checks if there is a match today
# PD = Primera division, SA = Serie A, PL = Premier League, BL1 = Bundesliga, FL1 = Ligue 1
response = requests.get(
    f"https://api.football-data.org/v4/matches?date={today_date}&competitions=PD,SA,PL,BL1,FL1",
    headers=headers
)

data = response.json()

# COMMAND ----------

# Check if there are new matches
if data.get("resultSet").get("count") == 0:
    print("No hay partidos hoy, pipeline detenido")
    dbutils.notebook.exit("NO_MATCHES")

# COMMAND ----------

all_matches = []
all_standings = []
all_scorers = []

for liga in ligas:
    
    # 1st Call — Matches
    r_matches = requests.get(
        f"https://api.football-data.org/v4/competitions/{liga}/matches",
        headers=headers
    )
    all_matches.extend(r_matches.json().get("matches", []))
    
    # 2nd Call — Standings
    r_standings = requests.get(
        f"https://api.football-data.org/v4/competitions/{liga}/standings",
        headers=headers
    )
    all_standings.append(r_standings.json())
    
    # 3rd Call — Scorers
    r_scorers = requests.get(
        f"https://api.football-data.org/v4/competitions/{liga}/scorers?limit=50",
        headers=headers
    )
    all_scorers.append(r_scorers.json())
    time.sleep(25)

# COMMAND ----------

matches_json = [{"data": json.dumps(m)} for m in all_matches]
standings_json = [{"data": json.dumps(s)} for s in all_standings]
scorers_json = [{"data": json.dumps(sc)} for sc in all_scorers]


df_matches = spark.createDataFrame(matches_json).withColumn("ingestion_timestamp", current_timestamp()).withColumn("ingestion_date", lit(today_date))

df_standings = spark.createDataFrame(standings_json).withColumn("ingestion_timestamp", current_timestamp()).withColumn("ingestion_date", lit(today_date))

df_scorers = spark.createDataFrame(scorers_json).withColumn("ingestion_timestamp", current_timestamp()).withColumn("ingestion_date", lit(today_date))

df_matches.write.mode("overwrite").format("delta").saveAsTable("football.bronze.football_matches")
df_standings.write.mode("overwrite").format("delta").saveAsTable("football.bronze.football_standings")
df_scorers.write.mode("overwrite").format("delta").saveAsTable("football.bronze.football_scorers")

# COMMAND ----------

print(f"Bronze completed — {len(all_matches)} matches, {len(all_standings)} standings, {len(all_scorers)} scorers")