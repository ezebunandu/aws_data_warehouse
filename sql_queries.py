import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

IAM_ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events(
artist          VARCHAR,
auth            VARCHAR,
firstName       VARCHAR,
gender          vARCHAR,
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR,
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id             VARCHAR,
num_songs           INTEGER,
title               VARCHAR,
artist_name         VARCHAR,
artist_latitude     FlOAT,
year                INTEGER,
duration            FLOAT,
artist_id           VARCHAR,
artist_longitude    FLOAT,
artist_location     VARCHAR
);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
songplay_id INTEGER IDENTITY(0, 1) PRIMARY KEY sortkey,
start_time  TIMESTAMP,
user_id     INTEGER,
level       VARCHAR,
song_id     VARCHAR,
artist_id   VARCHAR,
session_id  INTEGER,
location    VARCHAR,
user_agent  VARCHAR
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
user_Id     INTEGER PRIMARY KEY distkey,
first_name  VARCHAR,
last_name   VARCHAR,
gender      VARCHAR,
level       VARCHAR
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs(
song_id     VARCHAR PRIMARY KEY,
title       VARCHAR,
artist_id   VARCHAR distkey,
year        INTEGER,
duration    FLOAT
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists(
artist_id   VARCHAR PRIMARY KEY distkey,
name        VARCHAR,
location    VARCHAR,
latitude    FLOAT,
longitude   FlOAT
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time(
start_time  TIMESTAMP PRIMARY KEY sortkey distkey,
hour        INTEGER,
day         INTEGER,
week        INTEGER,
month       INTEGER,
year        INTEGER,
weekday     INTEGER
);
"""

# STAGING TABLES
staging_events_copy = f"""COPY staging_events from {LOG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ARN}'
    COMPUPDATE OFF REGION 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    FORMAT AS JSON {LOG_JSONPATH};
"""

staging_songs_copy = f"""COPY staging_songs FROM {SONG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ARN}'
    COMPUPDATE OFF REGION 'us-west-2'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
"""

# FINAL TABLES
songplay_table_insert = """INSERT INTO songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'), 'YYYY-MM-DD HH24:MI:SS'),
                se.userId,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                se.location,
                se.userAgent
    FROM staging_events se
    JOIN staging_songs ss ON se.artist = ss.artist_name AND se.song = ss.title;
"""

user_table_insert = """INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT userId,
                firstName,
                lastName,
                gender,
                level
    FROM staging_events
    WHERE userId IS NOT NULL;
"""

song_table_insert = """INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
"""

artist_table_insert = """INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
"""

time_table_insert = """INSERT INTO time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT ts,
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
    FROM staging_events
    WHERE ts IS NOT NULL;
"""

# QUERY LISTS
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
