# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IPL Data Analysis").getOrCreate()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType,DecimalType
from pyspark.sql.functions import col,sum,avg,when
from pyspark.sql.window import Window

# COMMAND ----------

# Define the schema
ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])


# COMMAND ----------

ball_by_ball_df = spark.read.csv('dbfs:/FileStore/ipl_till_2017/ball_by_ball.csv', header=True, schema=ball_by_ball_schema)

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

# COMMAND ----------

match_df = spark.read.schema(match_schema).format('csv').option('header','true').load('dbfs:/FileStore/ipl_till_2017/match.csv')

# COMMAND ----------

players_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

# COMMAND ----------

players_df = spark.read.schema(players_schema).format('csv').option('header','true').load('dbfs:/FileStore/ipl_till_2017/player.csv')

# COMMAND ----------

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(18, 0), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

# COMMAND ----------

player_match_df = spark.read.schema(player_match_schema).format('csv').option('header','true').load('dbfs:/FileStore/ipl_till_2017/player_match.csv')

# COMMAND ----------

team_schema = StructType([
    StructField('team_sk',IntegerType(),True)
    ,StructField('team_id',IntegerType(),True)
    ,StructField('team_name',StringType(),True)
])

# COMMAND ----------

team_df = spark.read.format('csv').schema(team_schema).option('header','true').load('dbfs:/FileStore/ipl_till_2017/team.csv')

# COMMAND ----------

#filter the balls with no wides and no_balls
ball_by_ball_df = ball_by_ball_df.filter((col('wides')==0) & (col('noballs')==0))

# COMMAND ----------

#Total run and avg run by each match and innings
total_avg_run_df = ball_by_ball_df.groupBy('match_id', 'innings_no').agg(
    sum('runs_scored').alias('total_runs'),
    avg('runs_scored').alias('avg_runs')
)

# COMMAND ----------



# COMMAND ----------

#windows function: calculate running total runs in each match in each over
Windows_spec = Window.partitionBy('match_id', 'innings_no').orderBy('over_id')

ball_by_ball_df = ball_by_ball_df.withColumn(
    'running_total_runs',
    sum('runs_scored').over(Windows_spec)
)

# COMMAND ----------

#conditional columns: create flag column based on either more than 6 including extras or take wicket
ball_by_ball_df = ball_by_ball_df.withColumn(
    'high_impact',
    when((col('runs_scored') + col('extra_runs')> 6) | (col('bowler_wicket') == True),True).otherwise(False)
    )

# COMMAND ----------

from pyspark.sql.functions import year,month,dayofmonth,when

#extract year,month,day from match_date column
match_df = match_df.withColumn("year",year('match_date')) \
                    .withColumn("month",month('match_date')) \
                    .withColumn("day",dayofmonth('match_date'))

#display when win_margin is more than 100 then high , 100-50 medium and < 50 low 
match_df = match_df.withColumn(
    'win_margin_category',
    when(col('win_margin') >= 100, 'High')
    .when((col('win_margin') <100) & (col('win_margin') > 50), 'medium')
    .otherwise('low')
)

# Analyaze impact of match on toss win or loss
match_df = match_df.withColumn(
    'toss_match_win',
    when(col('toss_winner') == col('match_winner'), 'Yes')
    .otherwise('No')
)

# COMMAND ----------

from pyspark.sql.functions import lower,regexp_replace

#remove unwanted characters form players_name using lower and regexp_replace
players_df = players_df.withColumn('player_name', lower(regexp_replace('player_name',"[^a-zA-Z0-9\\s]",''))) \
                        .withColumn('bowling_skill', regexp_replace('bowling_skill',"[^a-zA-Z0-9\\s]",'')) \
                        .withColumn('batting_hand', regexp_replace('batting_hand','[^a-zA-Z0-9\\s]',''))

#Handling missing value for  default batting hand and bowling skill with unknown
players_df = players_df.na.fill({'batting_hand':'unknown','bowling_skill':'unknown'})

#catgegories with players with batting hand
players_df = players_df.withColumn(
    'batting_style',
    when(col('batting_hand') ==  'Left-hand bat','Left handed')
    .otherwise('Right handed')
)

#display(players_df)

# COMMAND ----------

from pyspark.sql.functions import col,when,current_date

#Add a Vetran_status column based on players age
player_match_df = player_match_df.withColumn(
    'vetran_status',
    when(col('age_as_on_match') >= 35, 'Vetran')
    .otherwise('non-vetran')
) 

#Dynamic age calute since they joined
player_match_df = player_match_df.withColumn(
    'years_since_debut',
    (year(current_date()) - col('season_year'))
)

player_match_df = player_match_df.na.fill({'bowling_status':'unknown','batting_status':'unknown','player_captain':'unknown'})

display(player_match_df)

# COMMAND ----------

ball_by_ball_df.createOrReplaceTempView('ball_by_ball')
match_df.createOrReplaceTempView('match')
players_df.createOrReplaceTempView('player')
player_match_df.createOrReplaceTempView('player_match')
team_df.createOrReplaceTempView('team')

# COMMAND ----------

top_scoring_batsman_per_season = spark.sql("""
    SELECT 
        p.player_name,
        m.season_year,
        SUM(b.runs_scored) AS total_runs 
    FROM ball_by_ball b
        JOIN match m ON b.match_id = m.match_id   
        JOIN player_match pm ON m.match_id = pm.match_id AND b.striker = pm.player_id     
        JOIN player p ON p.player_id = pm.player_id
    GROUP BY p.player_name, m.season_year
    ORDER BY m.season_year, total_runs DESC    
""")

# COMMAND ----------

display(top_scoring_batsman_per_season)

# COMMAND ----------

economical_bowlers_powerplay = spark.sql("""
        SELECT 
            p.player_name, 
            AVG(b.runs_scored) AS avg_runs_per_ball, 
            COUNT(b.bowler_wicket) AS total_wickets
        FROM ball_by_ball b
            JOIN player_match pm ON b.match_id = pm.match_id AND b.bowler = pm.player_id
            JOIN player p ON pm.player_id = p.player_id
        WHERE b.over_id <= 6
            GROUP BY p.player_name
            HAVING COUNT(*) >= 1
        ORDER BY avg_runs_per_ball, total_wickets DESC
""")

economical_bowlers_powerplay.show()

# COMMAND ----------

toss_impact_individual_matches = spark.sql("""
SELECT m.match_id, m.toss_winner, m.toss_name, m.match_winner,
       CASE WHEN m.toss_winner = m.match_winner THEN 'Won' ELSE 'Lost' END AS match_outcome
FROM match m
WHERE m.toss_name IS NOT NULL
ORDER BY m.match_id
""")


display(toss_impact_individual_matches)

# COMMAND ----------

average_runs_in_wins = spark.sql("""
SELECT p.player_name, AVG(b.runs_scored) AS avg_runs_in_wins, COUNT(*) AS innings_played
FROM ball_by_ball b
JOIN player_match pm ON b.match_id = pm.match_id AND b.striker = pm.player_id
JOIN player p ON pm.player_id = p.player_id
JOIN match m ON pm.match_id = m.match_id
WHERE m.match_winner = pm.player_team
GROUP BY p.player_name
ORDER BY avg_runs_in_wins ASC
""")

average_runs_in_wins.show()

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

# Assuming 'economical_bowlers_powerplay' is already executed and available as a Spark DataFrame
economical_bowlers_pd = economical_bowlers_powerplay.toPandas()

# Visualizing using Matplotlib
plt.figure(figsize=(12, 8))
# Limiting to top 10 for clarity in the plot
top_economical_bowlers = economical_bowlers_pd.nsmallest(10, 'avg_runs_per_ball')
plt.bar(top_economical_bowlers['player_name'], top_economical_bowlers['avg_runs_per_ball'], color='skyblue')
plt.xlabel('Bowler Name')
plt.ylabel('Average Runs per Ball')
plt.title('Most Economical Bowlers in Powerplay Overs (Top 10)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------


import seaborn as sns

# COMMAND ----------

toss_impact_pd = toss_impact_individual_matches.toPandas()

# Creating a countplot to show win/loss after winning toss
plt.figure(figsize=(10, 6))
sns.countplot(x='toss_winner', hue='match_outcome', data=toss_impact_pd)
plt.title('Impact of Winning Toss on Match Outcomes')
plt.xlabel('Toss Winner')
plt.ylabel('Number of Matches')
plt.legend(title='Match Outcome')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

average_runs_pd = average_runs_in_wins.toPandas()

# Using seaborn to plot average runs in winning matches
plt.figure(figsize=(12, 8))
top_scorers = average_runs_pd.nlargest(10, 'avg_runs_in_wins')
sns.barplot(x='player_name', y='avg_runs_in_wins', data=top_scorers)
plt.title('Average Runs Scored by Batsmen in Winning Matches (Top 10 Scorers)')
plt.xlabel('Player Name')
plt.ylabel('Average Runs in Wins')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# Execute SQL Query
scores_by_venue = spark.sql("""
SELECT venue_name, AVG(total_runs) AS average_score, MAX(total_runs) AS highest_score
FROM (
    SELECT ball_by_ball.match_id, match.venue_name, SUM(runs_scored) AS total_runs
    FROM ball_by_ball
    JOIN match ON ball_by_ball.match_id = match.match_id
    GROUP BY ball_by_ball.match_id, match.venue_name
)
GROUP BY venue_name
ORDER BY average_score DESC
""")


# COMMAND ----------

scores_by_venue_pd = scores_by_venue.toPandas()

# Plot
plt.figure(figsize=(14, 8))
sns.barplot(x='average_score', y='venue_name', data=scores_by_venue_pd)
plt.title('Distribution of Scores by Venue')
plt.xlabel('Average Score')
plt.ylabel('Venue')
plt.show()

# COMMAND ----------

# Execute SQL Query
dismissal_types = spark.sql("""
SELECT out_type, COUNT(*) AS frequency
FROM ball_by_ball
WHERE out_type IS NOT NULL
GROUP BY out_type
ORDER BY frequency DESC
""")


# COMMAND ----------

# Convert to Pandas DataFrame
dismissal_types_pd = dismissal_types.toPandas()

# Plot
plt.figure(figsize=(12, 6))
sns.barplot(x='frequency', y='out_type', data=dismissal_types_pd, palette='pastel')
plt.title('Most Frequent Dismissal Types')
plt.xlabel('Frequency')
plt.ylabel('Dismissal Type')
plt.show()
     

# COMMAND ----------

# Execute SQL Query
team_toss_win_performance = spark.sql("""
SELECT team1, COUNT(*) AS matches_played, SUM(CASE WHEN toss_winner = match_winner THEN 1 ELSE 0 END) AS wins_after_toss
FROM match
WHERE toss_winner = team1
GROUP BY team1
ORDER BY wins_after_toss DESC
""")


# COMMAND ----------

# Convert to Pandas DataFrame
team_toss_win_pd = team_toss_win_performance.toPandas()

# Plot
plt.figure(figsize=(15, 8))
sns.barplot(x='wins_after_toss', y='team1', data=team_toss_win_pd)
plt.title('Team Performance After Winning Toss')
plt.xlabel('Wins After Winning Toss')
plt.ylabel('Team')
plt.show()


# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC git config --global user.name "SurajBoity"
# MAGIC git config --global user.email "surajkumar.boity@gmail.com"

# COMMAND ----------

# MAGIC %sh
# MAGIC git clone https://github.com/SurajBoity/Databricks.git /Users/surajkumarboity@gmail.com/IPL/IPL

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Users/surajkumarboity@gmail.com/IPL/IPL
# MAGIC ls
# MAGIC pwd
# MAGIC git commit -m "hi"

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Users/surajkumarboity@gmail.com/IPL/IPL
# MAGIC #mkdir IPL
# MAGIC #cd IPL
# MAGIC pwd
# MAGIC #echo hii i am toufeeq > dursj.txt
# MAGIC ls
# MAGIC
# MAGIC #git add IPL
# MAGIC git status --short
# MAGIC
# MAGIC #git commit -m "commit"
# MAGIC
# MAGIC git push origin main

# COMMAND ----------

print('hello')