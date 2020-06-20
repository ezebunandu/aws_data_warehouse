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
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXSITS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events(
artist          VARCHAR,
auth            VARCHAR,
firstName       varchar(64),
gender          char(1),
itemInSession   INTEGER,
lastName        VARCHAR(64),
length          FLOAT,
level           VARCHAR(32),
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
song_id             VARCHAR(64),
num_songs           INTEGER,
title               VARCHAR(256),
artist_name         VARCHAR(256),
artist_latitude     DOUBLE PRECISION,
year                INTEGER,
duration            FLOAT,
artist_id           VARCHAR(64),
artist_longitude    DOUBLE PRECISION,
artist_location     VARCHAR(256)
);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS fact_songplays (
start_time  TIMESTAMP,
user_id     INTEGER,
level       VARCHAR(32),
song_id     VARCHAR(64),
artist_id   VARCHAR(64),
session_id  INTEGER,
location    VARCHAR,
user_agent  VARCHAR,
PRIMARY KEY (start_time, user_id)
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS dim_users (
user_Id     INTEGER PRiMARY KEY distkey,
first_name  VARCHAR(64),
last_name   VARCHAR(64),
gender      CHAR(1),
level       VARCHAR(32)
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS dim_songs(
song_id     VARCHAR PRIMARY KEY,
title       VARCHAR(256),
artist_id   VARCHAR(64) distkey,
year        INTEGER,
duration    FLOAT
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS dim_artist(
artist_id   VARHCAR(64) PRIMARY KEY distkey,
name        VARCHAR(256),
location    VARCHAR,
latitude    DOUBLE PRECISION
longitude   DOUBLE PRECISION
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS dim_time(
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
    FORMAT AS json {LOG_JSONPATH};
"""

staging_songs_copy = f"""COPY staging_songs FROM {SONG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ARN}'
    COMPUPDATE OFF REGION 'us-west-2'
    FORMAT AS JSON 'auto';
"""

# FINAL TABLES

songplay_table_insert = """
"""

user_table_insert = """
"""

song_table_insert = """
"""

artist_table_insert = """
"""

time_table_insert = """
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
