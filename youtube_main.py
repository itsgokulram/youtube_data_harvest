import json
import pandas as pd

import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

import mysql.connector

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
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


    print(json.dumps(output_data))

except Exception as error:
    print(error)


# insert this output data into mongo

myclient = MongoClient("connection_string")

# database
db = myclient["youtube"]

# collection
collection = db["harvest"]

# Inserting the entire list in the collection
collection.insert_many([output_data])

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

def insert_data_intosql():

  mongo_data = collection.find() # Fetch data from MongoDB collection

# Insert data into MySQL table
  def insert_channel_data():

    for document in mongo_data:
        
      Channel_name = document["Channel"]["Channel_name"]
      Channel_ID = document["Channel"]["Channel_ID"]
      Subscribers = document["Channel"]["Subscribers"]
      Views = document["Channel"]["Views"]
      Description = document["Channel"]["Description"]
      Total_Videos = document["Channel"]["Total_Videos"]
      Playlist_id = document["Channel"]["Playlist_id"]

      sql_query = f"""
      INSERT INTO channels(
          Channel_id, channel_name, subscribers, views, description
      ) VALUES(%s, %s, %s, %s, %s
      )"""

      sql_values = (
          Channel_name, Channel_ID, Subscribers, Views, Description
      )
      mycursor.execute(sql_query, sql_values)
      mydb.commit()
  def insert_playlist_data():

    for document in mongo_data:

      Playlist_id = document["Channel"]["Playlist_id"]
      Channel_ID = document["Channel"]["Channel_ID"]
      Playlist_name = document["Channel"]["playlistName"]

      sql_query1 = f"""INSERT INTO playlists(playlist_id, channel_id, playlist_name) 
      VALUES (%s, %s, %s)"""

      sql_values1 = (
          Playlist_id, Channel_ID, Playlist_name
      )

      mycursor.execute(sql_query1, sql_values1)
      mydb.commit()

  def insert_video_data():

    for document in mongo_data:





  # mycursor.execute(f"""select * from youtube_db.channeltable""")

  # rows = mycursor.fetchall() # Fetch the results of the query

  # # Print the rows
  # for row in rows:
  #     print(row)

  # return rows
