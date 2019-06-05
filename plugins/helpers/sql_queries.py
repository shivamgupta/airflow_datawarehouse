class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                events.start_time AS start_time, 
                events.userid AS user_id, 
                events.level AS level,  
                songs.song_id AS song_id, 
                songs.artist_id AS artist_id, 
                events.sessionid AS session_id, 
                events.location AS location, 
                events.useragent AS user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    create_users_table = ("""
        CREATE TABLE IF NOT EXISTS public.users (
            userid int4 NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(256),
            CONSTRAINT users_pkey PRIMARY KEY (userid)
        );
    """)
    
    create_songs_table = ("""
        CREATE TABLE IF NOT EXISTS public.songs (
            songid varchar(256) NOT NULL,
            title varchar(256),
            artistid varchar(256),
            "year" int4,
            duration numeric(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (songid)
        );
    """)
    
    create_artist_table = ("""
        CREATE TABLE IF NOT EXISTS public.artists (
            artistid varchar(256) NOT NULL,
            name varchar(256),
            location varchar(256),
            lattitude numeric(18,0),
            longitude numeric(18,0)
        );
    """)
    
    create_time_table = ("""
        CREATE TABLE IF NOT EXISTS public.time (
            start_time timestamp NOT NULL,
            "hour" int4,
            "day" int4,
            week int4,
            month int4,
            "year" int4,
            weekday int4
        );
    """)
    
    create_songplays_table = ("""
        CREATE TABLE IF NOT EXISTS public.songplays (
            playid BIGINT identity(0, 1), 
            start_time timestamp NOT NULL,
            user_id int4 NOT NULL,
            "level" varchar(256),
            song_id varchar(256),
            artist_id varchar(256),
            session_id int4,
            location varchar(256),
            user_agent varchar(256),
            CONSTRAINT songplays_pkey PRIMARY KEY (playid)
        );
    """)
    
    create_staging_events_table = ("""
         CREATE TABLE IF NOT EXISTS public.staging_events
         ( 
            artist          VARCHAR,
            auth            VARCHAR,
            firstName       VARCHAR,
            gender          VARCHAR,
            itemInSession   INTEGER,
            lastName        VARCHAR,
            length          REAL, 
            level           VARCHAR,       
            location        VARCHAR,            
            method          VARCHAR,            
            page            VARCHAR,          
            registration    REAL,                 
            sessionId       VARCHAR,                
            song            VARCHAR,              
            status          VARCHAR,               
            ts              BIGINT,               
            userAgent       VARCHAR,            
            userId          VARCHAR    
        );
    """)
    
    create_staging_songs_table = ("""
        CREATE TABLE IF NOT EXISTS public.staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" int4
        );
    """)
    