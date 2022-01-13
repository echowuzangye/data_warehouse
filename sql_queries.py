import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS songs;"
artist_table_drop = "DROP table IF EXISTS artists;"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events
                                (event_id int IDENTITY(0,1),
                                artist varchar,
                                auth varchar,
                                user_first_name varchar,
                                gender varchar,
                                item_in_session int,
                                user_last_name varchar,
                                length double precision,
                                level varchar,
                                location varchar,
                                method varchar,
                                page varchar,
                                registration double precision,
                                session_id int,
                                song varchar,
                                status int,
                                ts bigint,
                                user_agent varchar,
                                user_id int);
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs
                                (event_id int IDENTITY(0,1),
                                artist_id varchar,
                                artist_latitude double precision,
                                artist_location varchar,
                                artist_longitude double precision,                                
                                artist_name varchar,
                                duration double precision,
                                num_songs int,
                                song_id varchar,
                                title varchar,                                
                                year int);                                
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
                            (songplay_id INT IDENTITY(0,1), 
                            start_time timestamp NOT NULL, 
                            user_id int NOT NULL, 
                            level varchar, 
                            song_id varchar, 
                            artist_id varchar, 
                            session_id int, 
                            location varchar, 
                            user_agent varchar);
""")

    
user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                    (user_id int PRIMARY KEY, 
                    first_name varchar,
                    last_name varchar, 
                    gender varchar, 
                    level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
                    (song_id varchar PRIMARY KEY, 
                    title varchar NOT NULL, 
                    artist_id varchar, 
                    year int, 
                    duration double precision);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
                    (artist_id varchar PRIMARY KEY, 
                    name varchar, 
                    location varchar, 
                    latitude double precision, 
                    longitude double precision);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                    (start_time timestamp PRIMARY KEY, 
                    hour int, 
                    day int, 
                    week int, 
                    month int, 
                    year int, 
                    weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {} 
CREDENTIALS 'aws_iam_role={}'
JSON {};
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))


staging_songs_copy = ("""
copy staging_songs from {} 
CREDENTIALS 'aws_iam_role={}'
JSON 'auto';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time,
        se.user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.session_id,
        se.location,
        se.user_agent
FROM staging_events as se, staging_songs as ss
WHERE se.page = 'NextSong'
AND se.artist = ss.artist_name
AND se.song = ss.title
AND se.length = ss.duration
AND start_time IS NOT NULL
AND se.user_id IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id,
                user_first_name,
                user_last_name,
                gender,
                level
FROM staging_events
WHERE page = 'NextSong'
AND user_id IS NOT NULL ;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM staging_songs
WHERE song_id IS NOT NULL
AND title IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;                
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' as start_time, 
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
