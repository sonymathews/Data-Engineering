import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
	CREATE TABLE IF NOT EXISTS staging_events (
		artist varchar, 
		auth varchar, 
		firstname varchar, 
		gender char, 
		iteminsession int, 
		lastname varchar, 
		length numeric, 
		level varchar, 
		location varchar, 
		method varchar, 
		page varchar, 
		registration float, 
		sessionid int, 
		song varchar, 
		status int, 
		ts bigint, 
		useragent varchar, 
		userid int);
""")

staging_songs_table_create = ("""
	CREATE TABLE IF NOT EXISTS staging_songs (
		num_songs int, 
		artist_id varchar, 
		artist_latitude varchar, 
		artist_longitude varchar, 
		artist_location text, 
		artist_name text,
		song_id varchar, 
		title varchar, 
		duration numeric, 
		year int);
""")

songplay_table_create = ("""
	CREATE TABLE IF NOT EXISTS songplays (
		songplay_id bigint IDENTITY(1,1) NOT NULL, 
		start_time timestamp NOT NULL, 
		user_id int NOT NULL,
		level varchar NOT NULL, 
		song_id varchar NOT NULL, 
		artist_id varchar NOT NULL, 
		session_id int, 
		location varchar, 
		user_agent varchar, 
		PRIMARY KEY(songplay_id));
""")

user_table_create = ("""
	CREATE TABLE IF NOT EXISTS users (
		user_id int NOT NULL, 
		first_name varchar, 
		last_name varchar, 
		gender char, 
		level varchar, 
		PRIMARY KEY(user_id));
""")

song_table_create = ("""
	CREATE TABLE IF NOT EXISTS songs (
		song_id varchar NOT NULL, 
		title varchar, 
		artist_id varchar, 
		year int, 
		duration numeric, 
		PRIMARY KEY(song_id));
""")

artist_table_create = ("""
	CREATE TABLE IF NOT EXISTS artists (
		artist_id varchar NOT NULL, 
		name text, 
		location text, 
		latitude float, 
		longitude float, 
		PRIMARY KEY(artist_id));
""")

time_table_create = ("""
	CREATE TABLE IF NOT EXISTS time (
		start_time timestamp NOT NULL, 
		hour int NOT NULL, 
		day int NOT NULL, 
		week int NOT NULL, 
		month int NOT NULL, 
		year int NOT NULL, 
		weekday int NOT NULL, 
		PRIMARY KEY(start_time));
""")

# STAGING TABLES

staging_events_copy = ("""
	copy staging_events 
	from {} 
	iam_role {}
	json  {};
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
	copy staging_songs 
	from {} 
	iam_role {}
	json  'auto'
	maxerror as 10;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
	INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
	SELECT (timestamp 'epoch' + (ts / 1000) * INTERVAL '1 Second ') as start_time, 
		user_id as user_id, 
		level as level, 
		song_id as song_id, 
		artist_id as artist_id, 
		session_id as session_id, 
		location as location, 
		useragent as user_agent
	FROM staging_events as ev
	JOIN staging_songs as sg ON (ev.song = sg.title AND ev.artist = sg.artist_name);
""")

user_table_insert = ("""
	INSERT INTO users (user_id, first_name, last_name, gender, level)
	SELECT DISTINCT(userid) as user_id,
		firstname as first_name,
		lastname as last_name,
		gender as gender,
		level as level
	FROM staging_events
	WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
	INSERT INTO songs (song_id, title, artist_id, year, duration)
	SELECT DISTINCT(song_id) as song_id,
		title as title,
		artist_id as artist_id,
		year as year,
		duration as duration
	FROM staging_songs
	WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
	INSERT INTO artists (artist_id, name, location, latitude, longitude)
	SELECT DISTINCT(artist_id) AS artist_id, 
		artist_name as name, 
		artist_location as location, 
		artist_latitude as latitude, 
		artist_longitude as longitude 
	FROM staging_songs
	WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
	INSERT INTO time (start_time, hour, day, week, month, year, weekday)
	SELECT DISTINCT(timestamp 'epoch' + (ts / 1000) * INTERVAL '1 Second ') as start_time, 
		EXTRACT(hour FROM start_time) as hour,
		EXTRACT(day FROM start_time) as day,
		EXTRACT(week FROM start_time) as week,
		EXTRACT(month FROM start_time) as month,
		EXTRACT(year FROM start_time) as year,
		EXTRACT(dow FROM start_time) as weekday
	FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
