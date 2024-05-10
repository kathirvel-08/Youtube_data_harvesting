#Scrapping data from Youtube via using google-api-python-client.   
import googleapiclient.discovery        
import pandas as pd
from sqlalchemy import *  
from datetime import *
import time
import streamlit as st
import isodate

#setup connection for youtube API
def api_connection():
        api_service_name = "youtube"
        api_version = "v3"
        api_key = "AIzaSyCDs56VB8ZsYinqZrVzHY8vwcVOTRcFEwc"     
        youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey=api_key)
        return youtube
youtube = api_connection()


#setup connection to sql database
def connect_to_sql():
    hostname = "localhost"
    port = "3306"
    username = "root"
    password = "1234"
    database_name = "sample"
    connection = f"mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{database_name}"
    return connection
connection = connect_to_sql()


# Channel details.
def channel_details(channel_id):
    request = youtube.channels().list(
        id = channel_id,
        part = "snippet,contentDetails,statistics"
    )
    response = request.execute()
    data = {"Channel_id" : channel_id,
        "Channel_name" : response ['items'][0]['snippet']['title'],
        "Channel_description" : response ['items'][0]['snippet']['description'],
        "Channel_subcribers" : response ['items'][0]['statistics']['subscriberCount'],
        "Channel_views" : response ['items'][0]['statistics']['viewCount'],
        "Channel_video_count" : response ['items'][0]['statistics']['videoCount'],
        "Playlist_ID" : response ['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    #convert data into df
    df_channel_details = pd.DataFrame(data,index=[0])
    #migrate all the data sql
    table_name = "channel_table"
    data_types = {"Channel_id" : Text(),
        "Channel_name" : Text(),
        "Channel_description" : Text(),
        "Channel_subcribers" : Integer(),
        "Channel_views" : Integer(),
        "Channel_video_count" : Integer(),
        "Playlist_ID" : Text() }
    df_channel_details.to_sql(name=table_name, con=connection, if_exists='append', index=False,dtype=data_types)
    return df_channel_details


#checking channel id valid or not
def channel_id_validation(channel_id):
    try:
            request = youtube.channels().list(
            id = channel_id,
            part = "snippet,contentDetails,statistics"
            )
            response = request.execute()
            checking_item = response['items']       #checking as valid from youtube
            df_channel_details = channel_details(channel_id)        #channel details execution function calling
            return df_channel_details

    except:
            Warning_message = "Please Enter valid Channel ID"        
            return Warning_message
    

# Video ids.        
def get_video_Ids():
    Playlist_ID = df_channel_details['Playlist_ID'][0]     
    video_ids = []      
    request = youtube.playlistItems().list(
            part="contentDetails",playlistId = Playlist_ID)
    response = request.execute()
    
    for item in range (len(response['items'])):       #loop to get video ids
        video_ids.append(response['items'][item]['contentDetails']['videoId'])
    
    next_page_token = response.get('nextPageToken')     #get next page token for furthur related videos from the current channel
    while next_page_token is not None:      #if next page token is none the loop will break
        request = youtube.playlistItems().list(
                part="snippet,contentDetails",playlistId = Playlist_ID, pageToken = next_page_token)
        response = request.execute()
        
        for item in range(len(response['items'])):       #loop to get video ids
            video_ids.append(response['items'][item]['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
    return video_ids


# Video details from video ids.
def get_video_details(video_Ids):     
    video_details_lister=[]
    for video_id in video_Ids:
        request = youtube.videos().list(
            part = "snippet,contentDetails,statistics", id= video_id
        )
        response = request.execute()
        data = { "channel_id" : channel_id,
            "video_id" : video_id,
            "video_name" : response['items'][0]['snippet']['title'],
            "video_description" : response['items'][0]['snippet']['description'],
            "published_date" : response['items'][0]['snippet']['publishedAt'],
            "duration_in_sec" : response['items'][0]['contentDetails']['duration'],
            "thumbnail" : response['items'][0]['snippet']['thumbnails']['default']['url'],
            "view_count" : response['items'][0]['statistics']['viewCount'],
            "favorite_count" : response['items'][0]['statistics']['favoriteCount']
        }
        
        try:#comment
            comment_to_list = {"comments" : response['items'][0]['statistics']['commentCount']}
            data.update(comment_to_list)

        except:
            comment_to_list = {"comments" : "0"}
            data.update(comment_to_list)
           
        try:#like count
            like_count_to_list = {"like_count" : response['items'][0]['statistics']['likeCount']}
            data.update(like_count_to_list)

        except:
            like_count_to_list = {"like_count" : "0"}
            data.update(like_count_to_list)

        video_details_lister.append(data)
    #convert data into df
    df_video_details = pd.DataFrame(video_details_lister)
    # converting duration in sec
    df_video_details['duration_in_sec'] = df_video_details['duration_in_sec'].apply(isodate.parse_duration)
    df_video_details['duration_in_sec'] = df_video_details['duration_in_sec'].apply(lambda td: td.total_seconds())

    #coverting datetime format
    df_video_details['published_date'] = pd.to_datetime(df_video_details['published_date'],format="%Y-%m-%dT%H:%M:%SZ")
    df_video_details['published_date'] = df_video_details['published_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

    #migrate all the data sql
    table_name = "video_table"
    data_types = { "channel_id" :Text() ,
            "video_id" : Text(),
            "playlist_id" : Text(),
            "video_name" : Text(),
            "video_description" : Text(),
            "published_date" : DateTime() ,
            "duration_in_sec" : Integer(),
            "thumbnail" : Text(),
            "view_count" : Integer(),
            "favorite_count" : Integer(),
            "comments" : Integer(),
            "like_count" : Integer()
        }
    df_video_details.to_sql(name=table_name, con=connection, if_exists='append', index=False,dtype=data_types)
    return df_video_details


#get comments
def get_comments(video_Ids):
    comment_details = []
    try:
            for video_id in video_Ids:
                    request = youtube.commentThreads().list(
                            part = "snippet",
                            videoId= video_id,
                            maxResults=50
                    )
                    response = request.execute()
                    for item in response['items']:
                            comment_dict = { "channel_id": channel_id,
                            "video_id":item['snippet']['videoId'],
                            "comment_id":item['snippet']['topLevelComment']['id'],
                            "comment_text":item['snippet']['topLevelComment']['snippet']['textOriginal'],
                            "comment_author":item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            "comment_published_date":item['snippet']['topLevelComment']['snippet']['publishedAt']
                            }
                            comment_details.append(comment_dict)
    except:
            pass
    #convert data into df
    df_comment_details = pd.DataFrame(comment_details)
    df_comment_details['comment_published_date'] = pd.to_datetime(df_comment_details['comment_published_date'],format="%Y-%m-%dT%H:%M:%SZ")
    df_comment_details['comment_published_date'] = df_comment_details['comment_published_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    #migrate all the data sql
    table_name = "comment_table"
    data_types = { "channel_id":Text(),
                "video_id":Text(),
                "comment_id":Text(),
                "comment_text":Text(),
                "comment_author":Text(),
                "comment_published_date":DateTime()
                }
    df_comment_details.to_sql(name=table_name, con=connection, if_exists='append', index=False,dtype=data_types)
    return df_comment_details



#INITIAL EXECUTION   
with st.sidebar:
     st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")   
     st.header(":blue[Steps to follow :]")
     st.text("1. Enter the Channel Id")
     st.text("2. Click on Scrap and Store")
     st.text("3. Select and view Analytics")
channel_id = st.text_input(":red[ENTER CHANNEL ID]")
#channel_id = "UCnS8ze4rAugPxWe7LqcbBxg"
try:
    df_channel_details = 0
except: 
     pass

if st.button(':red[Scrap and store]'):
    df_channel_details = channel_id_validation(channel_id)
    with st.spinner("wait please"):
        time.sleep(5)
        st.success("Data collected and Stored")
try:#main function to scrap data
    video_Ids = get_video_Ids()
    df_video_details = get_video_details(video_Ids)
    df_comment_details = get_comments(video_Ids)
except:
     pass

Question=st.selectbox("Select your question.",("1. What are the names of all the videos and their corresponding channels ?",
                                               "2. Which channels have the most number of videos, and many videos do they have ?",
                                               "3. What are the top 10 most viewed videos and their respextive channels ?",
                                               "4. How many comments were made on each video, and what are their corresponding video names ?",
                                               "5. Which videos have the highest number of likes, and what are their corresponding channel names ?",
                                               "6. What is the total number of likes for each video, and what are their corresponding channel names ?",
                                               "7. What is the total number of views for each channel, and what are their corresponding channel names ?",
                                               "8. What are the names of all the channels that have published videos in the year 2022 ?",
                                               "9. What is the average of all videos in each channel, and what are their corresponding channel names ?",
                                               "10. Which videos have the highest number of comments, and what are their corresponding channel names ?"))
if Question=="1. What are the names of all the videos and their …":
    query_1 = text("select video_table.video_name,channel_table.channel_name from video_table left join channel_table on video_table.channel_id = channel_table.channel_id;")
    connection.execute(query_1)
    table_1 = connection.fetchall()
    df1 = pd.DataFrame(table_1,columns=['Title','Channel Name'])