
create_db = ("""
    CREATE KEYSPACE IF NOT EXISTS sparkifydb 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)


# TABLE 1 DATA
# 1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4¶

create_table_1 = ("""
    CREATE TABLE IF NOT EXISTS table_1 (
        session_id int
        , item_in_session int
        , artist text
        , song_title text
        , song_length double
    , PRIMARY KEY (session_id, item_in_session))
""")

insert_table_1 = ("""
    INSERT INTO table_1 (
        session_id
        , item_in_session
        , artist
        , song_title
        , song_length
    )
    VALUES (%s, %s, %s, %s, %s)
""")

select_table_1 = ("""
    SELECT
        artist
        , song_title
        , song_length
    FROM table_1 
    WHERE session_id = 338
    AND item_in_session = 4
""")

drop_table_1 = ("""
    DROP TABLE table_1
""")


# TABLE 2 DATA
# 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

create_table_2 = ("""
    CREATE TABLE IF NOT EXISTS table_2 (
        user_id int
        , session_id int
        , item_in_session int
        , artist text
        , song_title text
        , user_first_name text
        , user_last_name text
    , PRIMARY KEY (user_id, session_id, item_in_session))
""")

insert_table_2 = ("""
    INSERT INTO table_2 (
        user_id
        , session_id
        , item_in_session
        , artist
        , song_title
        , user_first_name
        , user_last_name
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

select_table_2 = ("""
    SELECT
        artist
        , song_title
        , user_first_name
        , user_last_name
    FROM table_2
    WHERE user_id = 10
    AND session_id = 182
    ORDER BY session_id, item_in_session
""")

drop_table_2 = ("""
    DROP TABLE table_2
""")


# TABLE 3 DATA
# 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

create_table_3 = ("""
    CREATE TABLE IF NOT EXISTS table_3 (
        song_title text
        , user_id int
        , user_first_name text
        , user_last_name text
    , PRIMARY KEY (song_title, user_id))
""")

insert_table_3 = ("""
    INSERT INTO table_3 (
        song_title
        , user_id
        , user_first_name
        , user_last_name
    )
    VALUES (%s, %s, %s, %s)
""")

select_table_3 = ("""
    SELECT
        user_first_name
        , user_last_name
    FROM table_3
    WHERE song_title = 'All Hands Against His Own'
""")

drop_table_3 = ("""
    DROP TABLE table_3
""")


create_queries = [create_table_1, create_table_2, create_table_3]
insert_queries = [insert_table_1, insert_table_2, insert_table_3]
select_queries = [select_table_1, select_table_2, select_table_3]
drop_queries = [drop_table_1, drop_table_2, drop_table_3]

