import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    event_id        int           IDENTITY(0,1),
    artist          varchar,
    auth            varchar,
    first_name      varchar,
    gender          char(1),
    item_in_session int,
    last_name       varchar,
    length          numeric,
    level           varchar,
    location        varchar,
    method          varchar,
    page            varchar,
    registration    numeric,
    session_id      int,
    song            varchar,
    status          int,
    ts              bigint,
    user_agent      text,
    user_id         int,
    PRIMARY KEY(event_id)
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs      int,
    artist_id      varchar,
    latitude       numeric,
    longitude      numeric,
    location       varchar,
    artist_name    varchar,
    song_id        varchar,
    title          varchar,
    duration       numeric,
    year           int,
    PRIMARY KEY(artist_id, song_id)
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id    int              IDENTITY(0,1)    PRIMARY KEY, 
    start_time     timestamp, 
    user_id        int, 
    level          varchar, 
    song_id        varchar, 
    artist_id      varchar, 
    session_id     int, 
    location       varchar, 
    user_agent     text
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id        int              PRIMARY KEY, 
    first_name     varchar, 
    last_name      varchar, 
    gender         char(1), 
    level          varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
     song_id       varchar          PRIMARY KEY, 
     title         varchar, 
     artist_id     varchar, 
     year          int, 
     duration      numeric
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
     artist_id     varchar          PRIMARY KEY, 
     name          varchar, 
     location      varchar, 
     latitude      numeric, 
     longitude     numeric
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
     start_time    timestamp        PRIMARY KEY, 
     hour          int, 
     day           int, 
     week          int, 
     month         int, 
     year          int, 
     weekday       int
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from '{}'
iam_role '{}'
region 'us-west-2'
FORMAT AS JSON '{}'
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from '{}'
credentials 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS JSON 'auto'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
           TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
           se.user_id,
           se.level,
           ss.song_id,
           ss.artist_id,
           se.session_id,
           se.location,
           se.user_agent
    FROM staging_events se JOIN staging_songs ss
    ON se.artist = ss.artist_name
    AND se.song = ss.title
    AND se.length = ss.duration
    WHERE page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) 
    SELECT DISTINCT 
           user_id, 
           first_name, 
           last_name, 
           gender, 
           level
    FROM (
            SELECT DISTINCT
                   user_id,
                   first_name,
                   last_name,
                   gender,
                   level,
                   ROW_NUMBER() over (PARTITION BY user_id ORDER BY ts DESC) as row_num
            FROM staging_events
            WHERE page = 'NextSong'
            AND user_id IS NOT NULL
        ) tmp
    WHERE tmp.row_num = 1
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
           song_id, 
           title, 
           artist_id, 
           year, 
           duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""") 

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
           artist_id, 
           artist_name as name, 
           location, 
           latitude, 
           longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
           start_time, 
           EXTRACT(hour from start_time)as hour, 
           EXTRACT(day from start_time) as day, 
           EXTRACT(week from start_time) as week, 
           EXTRACT(month from start_time) as month, 
           EXTRACT(year from start_time) as year, 
           EXTRACT(weekday from start_time) as weekday
    FROM (
        SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time FROM staging_events
        WHERE page = 'NextSong'
        )tmp
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
