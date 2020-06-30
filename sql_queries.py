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
length          NUMERIC,
level           VARCHAR,
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    NUMERIC,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              BIGINT,
userAgent       VARCHAR,
userId          INTEGER
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id             VARCHAR,
num_songs           INTEGER,
title               VARCHAR(MAX),
artist_name         VARCHAR(MAX),
artist_latitude     VARCHAR(MAX),
year                INTEGER,
duration            NUMERIC,
artist_id           VARCHAR,
artist_longitude    VARCHAR(MAX),
artist_location     VARCHAR(MAX)
);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
songplay_id INTEGER IDENTITY(0, 1) PRIMARY KEY,
start_time  TIMESTAMP,
user_id     INTEGER,
level       VARCHAR,
song_id     VARCHAR,
artist_id   VARCHAR,
session_id  INTEGER,
location    TEXT,
user_agent  VARCHAR
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
user_id     INTEGER PRIMARY KEY,
first_name  VARCHAR,
last_name   VARCHAR,
gender      VARCHAR,
level       VARCHAR
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs(
song_id     VARCHAR PRIMARY KEY,
title       VARCHAR(MAX),
artist_id   VARCHAR,
year        INTEGER,
duration    NUMERIC
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists(
artist_id   VARCHAR PRIMARY KEY,
name        VARCHAR(MAX),
location    VARCHAR(MAX),
latitude    VARCHAR(MAX),
longitude   VARCHAR(MAX)
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time(
start_time  TIMESTAMP PRIMARY KEY,
hour        INTEGER,
day         INTEGER,
week        INTEGER,
month       VARCHAR,
year        INTEGER,
weekday     VARCHAR
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
    FORMAT AS JSON 'auto';
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
SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second',
                se.userId,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                se.location,
                se.userAgent
    FROM staging_events se INNER JOIN staging_songs ss
    ON se.artist = ss.artist_name AND se.song = ss.title
    AND se.page = 'NextSong';
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
    WHERE userId IS NOT NULL
    AND page = 'NextSong';
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
    FROM staging_songs;
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
SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' AS start_time,
                EXTRACT(HOUR FROM start_time),
                EXTRACT(DAY FROM start_time),
                EXTRACT(WEEKS FROM start_time),
                EXTRACT(MONTH FROM start_time),
                EXTRACT(YEAR FROM start_time),
                to_char(start_time, 'DAY')
    FROM staging_events;
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
