import pandas as pd
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database= "youtube_db"  #sql_database_name
)

# # print(mydb)
mycursor = mydb.cursor(buffered=True)



def sql_q1(): #What are the names of all the videos and their corresponding channels?

    query1 = f"""SELECT videos.video_name, channels.channel_name FROM videos INNER JOIN channels ON videos.playlist_id = channels.playlist_id;"""

    mycursor.execute(query1)

    result1 = mycursor.fetchall()

    query1_df = pd.DataFrame(result1, columns= ['Video Name', 'Channel Name'])

    return query1_df



def sql_q2(): #which channels have the most number of videos, and how many videos do they have?

    query2 = f"""SELECT channel_name, total_videos FROM channels order by Total_Videos Desc;"""

    mycursor.execute(query2)

    result2 = mycursor.fetchall()

    query2_df = pd.DataFrame(result2, columns= ['Channel Name', 'Total Videos'])

    return query2_df


def sql_q3(): #What are the top 10 most viewed videos and their respective channels?

    query3 = f"""SELECT videos.video_name, videos.view_count, channels.channel_name FROM videos \
        INNER JOIN channels ON videos.playlist_id = channels.playlist_id ORDER BY view_count DESC LIMIT 10;"""
    
    mycursor.execute(query3)

    result3 = mycursor.fetchall()

    query3_df = pd.DataFrame(result3, columns= ['Video Name', 'View count',  'Channel Name'])

    return query3_df



def sql_q4(): #How many comments were made on each video, and what are their corresponding video names?

    query4 = f"""SELECT v.video_id, v.video_name, COUNT(*) FROM videos v \
    INNER JOIN comments c ON v.video_id = c.video_id \
    GROUP BY v.video_id;"""

    mycursor.execute(query4)

    result4 = mycursor.fetchall()

    query4_df = pd.DataFrame(result4, columns= ['Video ID', 'Video Name', 'Comment Count'])

    return query4_df


def sql_q5(): #Which videos have the highest number of likes, and what are their corresponding channel names?

    query5 = f"""SELECT videos.video_name, videos.like_count, channels.channel_name FROM videos \
        INNER JOIN channels ON videos.playlist_id = channels.playlist_id ORDER BY like_count DESC LIMIT 10;"""
    
    mycursor.execute(query5)

    result5 = mycursor.fetchall()

    query5_df = pd.DataFrame(result5, columns= ['Video Name', 'Like count',  'Channel Name'])

    return query5_df


def sql_q6(): #What is the total number of likes and dislikes for each video, and what are their corresponding video names?

    query6 = f"""SELECT videos.video_name, videos.like_count FROM youtube_db.videos;"""

    mycursor.execute(query6)

    result6 = mycursor.fetchall()

    query6_df = pd.DataFrame(result6, columns= ['Video Name', 'Like count'])

    return query6_df



def sql_q7(): #What is the total number of views for each channel, and what are their corresponding channel names?

    query7 = f"""SELECT channels.views, channels.channel_name FROM youtube_db.channels;"""

    mycursor.execute(query7)

    result7 = mycursor.fetchall()

    query7_df = pd.DataFrame(result7, columns= ['Total Views', 'Channel Name'])

    return query7_df




def sql_q8(): #What are the names of all the channels that have published videos in the year 2022?

    query8 = f"""SELECT channels.channel_name, videos.published_At \
    FROM youtube_db.videos \
    JOIN youtube_db.channels ON videos.playlist_id = channels.playlist_id \
    WHERE YEAR(published_At) = 2023;"""

    mycursor.execute(query8)

    result8 = mycursor.fetchall()

    query8_df = pd.DataFrame(result8, columns= ['Channel Name', 'Published At'])

    return query8_df


def sql_q9(): #What is the average duration of all videos in each channel, and what are their corresponding channel names?

    query9 = f"""SELECT channels.channel_name, AVG(videos.duration) AS average_duration \
    FROM channels JOIN videos ON channels.playlist_id = videos.playlist_id 
    GROUP BY channels.channel_name;"""

    mycursor.execute(query9)

    result9 = mycursor.fetchall()

    query9_df = pd.DataFrame(result9, columns= ['Channel Name', 'Average duration'])

    return query9_df



def sql_q10(): #Which videos have the highest number of comments, and what are their corresponding channel names?

    query10 = f"""SELECT c.channel_name, v.video_name, COUNT(*) AS con FROM channels c \
    INNER JOIN playlists p ON c.playlist_id = p.playlist_id \
    INNER JOIN videos v ON v.playlist_id = p.playlist_id \
    INNER JOIN comments co ON co.video_id = v.video_id \
    GROUP BY v.video_id ORDER BY con DESC LIMIT 10;"""

    mycursor.execute(query10)

    result10 = mycursor.fetchall()

    query10_df = pd.DataFrame(result10, columns= ['Channel Name', 'Video Name', 'Comment Count'])

    return query10_df