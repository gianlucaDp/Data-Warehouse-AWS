import configparser


# CONFIG
CONFIG = configparser.ConfigParser()
CONFIG.read('dwh.cfg')
ARN = CONFIG.get("IAM_ROLE","ARN")
LOG_DATA = CONFIG.get("S3","LOG_DATA")
LOG_DATA_JSON_PATH = CONFIG.get("S3","LOG_JSONPATH")
SONG_DATA = CONFIG.get("S3","SONG_DATA")
REGION = CONFIG.get("S3","REGION")
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS s_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS s_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS s_events(
artist TEXT,
auth TEXT NOT NULL,
firstName TEXT,
gender CHAR(1),
itemInSession INT NOT NULL,
lastName TEXT,
length FLOAT,
level TEXT NOT NULL,
location TEXT,
method TEXT NOT NULL,
page TEXT NOT NULL,
registration NUMERIC,
sessionId BIGINT NOT NULL,
song TEXT,
status INT,
ts BIGINT NOT NULL,
userAgent TEXT,
userId INT);""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS s_songs (
num_songs INT NOT NULL,
artist_id TEXT NOT NULL,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location TEXT,
artist_name TEXT NOT NULL,
song_id TEXT NOT NULL,
title TEXT NOT NULL,
duration FLOAT,
year INT
);
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays(
songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
 start_time BIGINT NOT NULL, 
 user_id BIGINT NOT NULL, 
 level TEXT NOT NULL, 
 song_id TEXT, 
 artist_id TEXT, 
 session_id INT NOT NULL, 
 location TEXT, 
 user_agent TEXT 
);""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users 
(
user_id TEXT PRIMARY KEY,
first_name TEXT NOT NULL,
last_name TEXT NOT NULL,
gender TEXT NOT NULL,
level TEXT NOT NULL
);""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs 
(
song_id TEXT PRIMARY KEY,
title TEXT NOT NULL,
artist_id TEXT NOT NULL,
year INT,
duration FLOAT
);""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists
(
artist_id TEXT PRIMARY KEY,
name TEXT NOT NULL,
location TEXT,
latitude FLOAT,
longitude FLOAT
);""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time
(
start_time BIGINT PRIMARY KEY,
hour INT NOT NULL,
day INT NOT NULL,
week INT NOT NULL,
month INT NOT NULL,
year INT NOT NULL,
weekday INT NOT NULL);""")

# STAGING TABLES

staging_events_copy = (""" COPY s_events FROM {}
iam_role {}
REGION {}
FORMAT AS json {};
""").format(LOG_DATA,ARN,REGION,LOG_DATA_JSON_PATH)

staging_songs_copy = (""" COPY s_songs FROM {}
iam_role {} 
REGION {}
FORMAT AS json 'auto';
""").format(SONG_DATA,ARN,REGION)

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays(
 start_time, 
 user_id, 
 level, 
 song_id, 
 artist_id, 
 session_id, 
 location, 
 user_agent
 ) SELECT
    s_events.ts as start_time,
    s_events.userId as user_id,
    s_events.level as level,
    s_songs.song_id as song_id,
    s_songs.artist_id as artist_id,
    s_events.sessionId as session_id,
    s_events.location as location,
    s_events.userAgent as user_agent
  FROM s_events INNER JOIN s_songs 
  ON s_events.song = s_songs.title AND s_events.artist = s_songs.artist_name
  WHERE s_events.page = 'NextSong';
""")

user_table_insert = (""" INSERT INTO users (
user_id,
first_name,
last_name,
gender,
level
) SELECT DISTINCT
    userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender as gender,
    level as level
  FROM s_events
  WHERE user_id IS NOT NULL;
""")

song_table_insert = (""" INSERT INTO songs (
song_id,
title,
artist_id,
year,
duration
) SELECT DISTINCT
    song_id as song_id,
    title as title,
    artist_id as artist_id,
    year as year,
    duration as duration
 FROM s_songs
 WHERE song_id IS NOT NULL;
""")

artist_table_insert = (""" INSERT INTO artists (
artist_id ,
name,
location,
latitude,
longitude
) SELECT DISTINCT
    artist_id as artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
  FROM s_songs
  WHERE artist_id IS NOT NULL;
""")

time_table_insert = (""" INSERT INTO time (
start_time,
hour,
day,
week,
month,
year,
weekday
) SELECT
    conv_time.epotime,
    EXTRACT(hour FROM conv_time.start_time) as hour,
    EXTRACT(day FROM conv_time.start_time) as day,
    EXTRACT(week FROM conv_time.start_time) as week,
    EXTRACT(month FROM conv_time.start_time) as month,
    EXTRACT(year FROM conv_time.start_time) as year,
    EXTRACT(weekday FROM conv_time.start_time) as weekday
    FROM (SELECT DISTINCT
            TIMESTAMP 'epoch' + s_events.ts /1000 *INTERVAL '1 second' as start_time,
            s_events.ts as epotime 
            FROM s_events) AS conv_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

