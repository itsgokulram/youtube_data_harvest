import json
import pandas as pd

import certifi
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

import mysql.connector

# import google_auth_oauthlib.flow
import googleapiclient.discovery
# import googleapiclient.errors
from googleapiclient.discovery import build



Api_Key = ''  #api_key
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey=Api_Key)

ch_id = str(input("Enter the channel ID: "))

def get_channel_data(youtube, channel_id):
    ch_request = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    )

    ch_data = ch_request.execute()
    channel_details = {"Channel_Name": ch_data['items'][0]['snippet']['title'],
                       "Channel_Id": channel_id,
                       "Subscribers_Count": ch_data['items'][0]['statistics']['subscriberCount'],
                       "Views": ch_data['items'][0]['statistics']['viewCount'],
                       "Channel_Description": ch_data['items'][0]['snippet']['description'],
                       "Total_Videos": ch_data['items'][0]['statistics']['videoCount'],
                       "channel_Playlist_Id": ch_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                       }
    return channel_details


def get_video_id_details(youtube, play_id):
    vid_id_data = []
    vid_request = youtube.playlistItems().list(
        playlistId=play_id, part="snippet,contentDetails",
        maxResults=50
    )
    response = vid_request.execute()
    for i in response['items']:
        vid_id_data.append(i['contentDetails']['videoId'])
    return vid_id_data


def get_video_and_comment_info(youtube, video_ids):
    video_details = []
    for video_id in video_ids:
        comment_details = []
        vid_det_request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=video_id, maxResults=50
        )
        video_response = vid_det_request.execute()

        if video_response['items']:
            video_info = {
                "Video_Id": video_id,
                "Video_Name": video_response['items'][0]['snippet']['title'],
                "Video_Description": video_response['items'][0]['snippet']['description'],
                "Video_Statistics": video_response['items'][0]['statistics'].get('commentCount', 0),
                "Comment_Count": video_response['items'][0]['statistics'].get('commentCount', 0),
                "View_Count": video_response['items'][0]['statistics'].get('viewCount', 0),
                "Like_Count": video_response['items'][0]['statistics'].get('likeCount', 0),
                # "Dislike_Count": video_response['items'][0]['statistics'].get('dislikeCount', 0),
                "Favorite_Count": video_response['items'][0]['statistics'].get('favoriteCount', 0),
                "Published_At": video_response['items'][0]['snippet']['publishedAt'],
                "Duration": video_response['items'][0]['contentDetails']['duration'],
                "Thumbnail": video_response['items'][0]['snippet']['thumbnails']['default']['url'],
                "Caption_Status": video_response['items'][0]['contentDetails'].get('caption'),
            }

            # Fetch comments associated with that video
            try:
                comment_request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=str(video_id),
                    maxResults=10
                )
                comment_response = comment_request.execute()
                for comment in comment_response['items']:
                    comment_info = {
                        "Video_ID": video_id,
                        "Comment_Id": comment['snippet']['topLevelComment']['id'],
                        "Comment_Text": comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                        "Comment_Author": comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        "Comment_PublishedAt": comment['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                    comment_details.append(comment_info)
            except Exception as error:
                print(error)
            video_info["comments"] = comment_details
            video_details.append(video_info)
    return video_details


try:
    output_data = {}

    channel_response = get_channel_data(youtube, ch_id)  # get this channel id in the input
    output_data["channel"] = channel_response

    channel_playlist_id = channel_response.get("channel_Playlist_Id")
    video_lists = get_video_id_details(youtube, channel_playlist_id)

    # print(video_lists)
    # print(type(video_lists))
    video_response = get_video_and_comment_info(youtube, video_lists)
    output_data["videos"] = video_response


    # print(json.dumps(output_data))

except Exception as error:
    print(error)


# insert this output data into mongo
uri = (connection string)
# Create a new client and connect to the server
client = MongoClient(uri, tlsCAFile=certifi.where())
db = client.youtube    #database created
records = db.harvest



# database
# db = myclient.youtube_db

# collection
# collection = db.harvest

# Inserting the entire list in the collection
records.insert_many([output_data])

# Establishing MYSQL connection

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database= "youtube_db"  #sql_database_name
)

# # print(mydb)
mycursor = mydb.cursor(buffered=True)

# # mycursor.execute("SHOW DATABASES")

# # for x in mycursor:
# #     print(x)

# def insert_data_intosql():

#   mongo_data = collection.find() # Fetch data from MongoDB collection

# Insert data into MySQL table
def insert_channel_data():

    mongo_data = records.find() # Fetch data from MongoDB collection

    for document in mongo_data:
        
        print(document["channel"]["Channel_Id"])
        Channel_name = document["channel"]["Channel_Name"]
        Channel_ID = document["channel"]["Channel_Id"]
        Subscribers = document["channel"]["Subscribers_Count"]
        Views = document["channel"]["Views"]
        Description = document["channel"]["Channel_Description"]
        Total_Videos = document["channel"]["Total_Videos"]
        Playlist_id = document["channel"]["channel_Playlist_Id"]

        ch_exist_query = ("SELECT * FROM channels WHERE channel_id = '{}';").format(Channel_ID)
        mycursor.execute(ch_exist_query)
        result = mycursor.fetchone()

        if result:
            print("Channel already exists: ", Channel_ID)
            continue

        sql_query = f"""
        INSERT INTO channels(
            channel_name, Channel_id, subscribers, views, description, total_videos, playlist_id
        ) VALUES(%s, %s, %s, %s, %s, %s, %s
        )"""

        sql_values = (
            Channel_name, Channel_ID, Subscribers, Views, Description, Total_Videos, Playlist_id
        )
        mycursor.execute(sql_query, sql_values)
        mydb.commit()

        #Inserting playlist data

        sql_query1 = f"""INSERT INTO playlists(playlist_id, channel_id) 
        VALUES (%s, %s)"""

        sql_values1 = (
            Playlist_id, Channel_ID
        )

        mycursor.execute(sql_query1, sql_values1)
        mydb.commit()

#   def insert_playlist_data():

#     for document in mongo_data:

#       Playlist_id = document["Channel"]["Playlist_id"]
#       Channel_ID = document["Channel"]["Channel_ID"]
#       Playlist_name = document["Channel"]["playlistName"]

#       sql_query1 = f"""INSERT INTO playlists(playlist_id, channel_id, playlist_name) 
#       VALUES (%s, %s, %s)"""

#       sql_values1 = (
#           Playlist_id, Channel_ID, Playlist_name
#       )

#       mycursor.execute(sql_query1, sql_values1)
#       mydb.commit()

def insert_video_data():

    mongo_data = records.find()
    for document in mongo_data:

        Playlist_id = document["channel"]["channel_Playlist_Id"]

        for videos in document["videos"]:

            Video_Id = videos["Video_Id"]
            Video_Name = videos["Video_Name"]
            Video_Description = videos["Video_Description"]
            Video_Statistics = videos["Video_Statistics"]
            Comment_Count = videos["Comment_Count"]
            View_Count = videos["View_Count"]
            Like_Count = videos["Like_Count"]
            Favorite_Count = videos["Favorite_Count"]
            Published_At = videos["Published_At"]
            Duration =videos["Duration"]
            Thumbnail =videos["Thumbnail"]
            Caption_Status =videos["Caption_Status"]

            vid_exist_query = ("SELECT * FROM videos WHERE video_id = '{}';").format(Video_Id)
            mycursor.execute(vid_exist_query)
            result = mycursor.fetchone()

            if result:
                print("Video already exists: ", Video_Id)
                continue

            sql_query2 = f"""INSERT INTO videos(
            video_id, playlist_id, video_name, video_description, video_statistics, comment_count,
            view_count, like_count, favorite_Count, published_At, duration, thumbnail, caption_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            
            sql_values2 = (
                Video_Id, Playlist_id, Video_Name, Video_Description, Video_Statistics, Comment_Count,
                View_Count, Like_Count, Favorite_Count, Published_At, Duration, Thumbnail, Caption_Status
            )

            mycursor.execute(sql_query2, sql_values2)
            mydb.commit()

def insert_comment_data():

    mongo_data = records.find()

    for document in mongo_data:

        for comment in document["videos"]:

            for comments in comment["comments"]:

                Video_Id = comments["Video_ID"]
                Comment_Id = comments["Comment_Id"]
                Comment_Text = comments["Comment_Text"]
                Comment_Author = comments["Comment_Author"]
                Comment_PublishedAt = comments["Comment_PublishedAt"]

                vid_exist_query = ("SELECT * FROM comments WHERE comment_id = '{}';").format(Comment_Id)
                mycursor.execute(vid_exist_query)
                result = mycursor.fetchone()

                if result:
                    print("comment already exists: ", Comment_Id)
                    continue

                sql_query3 = f"""
                INSERT INTO comments(
                comment_id, video_id, comment_text, comment_author, comment_published_date
                ) VALUES(
                %s, %s, %s, %s, %s
                )"""

                sql_values3 = (
                    Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_PublishedAt
                )
                mycursor.execute(sql_query3, sql_values3)
                mydb.commit()

# execute = insert_data_intosql()
execute1 = insert_channel_data()
execute2 = insert_video_data()
execute3 = insert_comment_data()
