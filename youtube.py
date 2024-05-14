#Scrapping data from Youtube via using google-api-python-client.   
import googleapiclient.discovery        
import pandas as pd
from sqlalchemy import *  
import mysql.connector
from datetime import *
import time
import isodate
import streamlit as st

#setup connection for youtube API
def api_connection():
        api_service_name = "youtube"
        api_version = "v3"
        api_key = "AIzaSyCDs56VB8ZsYinqZrVzHY8vwcVOTRcFEwc"     
        youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey=api_key)
        return youtube
youtube = api_connection()


#setup connection to sql database
class my_sql():
    def connect_for_create():
        hostname = "localhost"
        port = "3306"
        username = "root"
        password = "1234"
        database_name = "sample"
        connection = f"mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{database_name}"
        return connection
    
    def connect_for_query():
        connection = mysql.connector.connect(host = "localhost",
        port = "3306",
        user = "root",
        password = "1234",
        database = "sample")
        return connection
    
connection_for_create = my_sql.connect_for_create()
connection_for_dql = my_sql.connect_for_query()


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
    df_channel_details.to_sql(name=table_name, con=connection_for_create, if_exists='append', index=False,dtype=data_types)
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
            "duration_in_mins" : response['items'][0]['contentDetails']['duration'],
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
    df_video_details['duration_in_mins'] = df_video_details['duration_in_mins'].apply(isodate.parse_duration)
    df_video_details['duration_in_mins'] = df_video_details['duration_in_mins'].apply(lambda td: round(td.total_seconds()/60))

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
            "duration_in_mins" : Integer(),
            "thumbnail" : Text(),
            "view_count" : Integer(),
            "favorite_count" : Integer(),
            "comments" : Integer(),
            "like_count" : Integer()
        }
    df_video_details.to_sql(name=table_name, con=connection_for_create, if_exists='append', index=False,dtype=data_types)
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
    df_comment_details.to_sql(name=table_name, con=connection_for_create, if_exists='append', index=False,dtype=data_types)
    return df_comment_details



#INITIAL EXECUTION   
with st.sidebar:
     st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")   
     st.header(":blue[Steps to follow :]")
     st.text("1. Enter the Channel Id")
     st.text("2. Click on Scrap and Store")
     st.text("3. Select and view Analytics")
st.header("ENTER CHANNEL ID")
channel_id = st.text_input("Give input here")

try:
    df_channel_details = 0
except: 
     pass

if st.button('Scrap and store'):
    df_channel_details = channel_id_validation(channel_id)
    with st.spinner("wait please"):
        time.sleep(5)
        st.success("Data collected and Stored")

# show updated channel details
st.header("Updated Channel Details")
try:
    cursor = connection_for_dql.cursor()
    cursor.execute("""select Channel_name,Channel_video_count,Channel_views from channel_table  ;""")
    data = cursor.fetchall()
    df_query = pd.DataFrame(data,columns=["Channel name","Total videos","Total Views"])
    st.dataframe(df_query,hide_index=True)
except:
    st.warning("No data in Database")

try:#main function to scrap data
    video_Ids = get_video_Ids()
    df_video_details = get_video_details(video_Ids)
    df_comment_details = get_comments(video_Ids)
except:
     pass
st.header("QUESTIONS FOR ANALYTICS")
Question=st.selectbox("Select your question.",("1. What are the names of all the videos and their corresponding channels ?",
                                               "2. Which channels have the most number of videos and how many videos do they have ?",
                                               "3. What are the top 10 most viewed videos and their respective channels ?",
                                               "4. How many comments were made on each video, and what are their corresponding video names ?",
                                               "5. Which videos have the highest number of likes, and what are their corresponding channel names ?",
                                               "6. What is the total number of likes for each video, and what are their corresponding channel names ?",
                                               "7. What is the total number of views for each channel, and what are their corresponding channel names ?",
                                               "8. What are the names of all the channels that have published videos in the year 2022 ?",
                                               "9. What is the average of all videos in each channel, and what are their corresponding channel names ?",
                                               "10. Which videos have the highest number of comments, and what are their corresponding channel names ?"))
#Question 1
if Question=="1. What are the names of all the videos and their corresponding channels ?":
    try:
        cursor = connection_for_dql.cursor()
        cursor.execute("""select video_table.video_name,channel_table.channel_name 
                    from video_table 
                    left join channel_table on video_table.channel_id = channel_table.channel_id;""")
        data = cursor.fetchall()
        df_query_1 = pd.DataFrame(data,columns=["Title","Channel name"])
        st.dataframe(df_query_1,hide_index=True)
    except:
         st.warning("No data in Database")

#Question 2
elif Question=="2. Which channels have the most number of videos and how many videos do they have ?":
     try:
        cursor = connection_for_dql.cursor()
        cursor.execute("""select Channel_name,Channel_video_count from channel_table 
                        order by channel_table.Channel_video_count ;""")
        data = cursor.fetchall()
        df_query = pd.DataFrame(data,columns=["Channel name","Total videos"])
        st.dataframe(df_query,hide_index=True)
     except:
         st.warning("No data in Database")

#Question 3
elif Question=="3. What are the top 10 most viewed videos and their respective channels ?":
     try:
        cursor = connection_for_dql.cursor()
        cursor.execute("""select  video_table.video_name, video_table.view_count, channel_table.Channel_name
                        from video_table left join  channel_table on channel_table.channel_id=video_table.channel_id 
                        where video_table.view_count is not null order by video_table.view_count desc limit 10;""")
        data = cursor.fetchall()
        df_query = pd.DataFrame(data,columns=["Video Title","Total views","Channel name"])
        st.dataframe(df_query,hide_index=True)
     except:
         st.warning("No data in Database")

#Question 4
elif Question=="4. How many comments were made on each video, and what are their corresponding video names ?":
     try:
        cursor = connection_for_dql.cursor()
        cursor.execute("""select video_table.video_name, channel_table.Channel_name,  video_table.comments 
                        from video_table left join channel_table on video_table.channel_id = channel_table.channel_id; """)
        data = cursor.fetchall()
        df_query = pd.DataFrame(data,columns=["Video Title","Channel name","Comments"])
        st.dataframe(df_query,hide_index=True)
     except:
         st.warning("No data in Database")

#Question 5
elif Question=="5. Which videos have the highest number of likes, and what are their corresponding channel names ?":
     try:
        cursor = connection_for_dql.cursor()
        cursor.execute("""select  video_table.video_name, video_table.like_count, channel_table.Channel_name
                            from video_table
                            left join channel_table on channel_table.Channel_id = video_table.Channel_id
                            where video_table.like_count is not null order by video_table.like_count desc """)
        data = cursor.fetchall()
        df_query= pd.DataFrame(data,columns=["Video Title","Like Count","Channel name"])
        st.dataframe(df_query,hide_index=True)
     except:
         st.warning("No data in Database")

#Question 6
elif Question=="6. What is the total number of likes for each video, and what are their corresponding channel names ?":
    try:    
        cursor = connection_for_dql.cursor()
        cursor.execute("""select  video_table.video_name, video_table.like_count, channel_table.Channel_name 
                        from video_table left join channel_table on channel_table.Channel_id = video_table.Channel_id;""")
        data = cursor.fetchall()
        df_query= pd.DataFrame(data,columns=["Video Title","Like Count","Channel name"])
        st.dataframe(df_query,hide_index=True)     
    except:
         st.warning("No data in Database")

#Question 7
elif Question=="7. What is the total number of views for each channel, and what are their corresponding channel names ?":
     try:
        cursor = connection_for_dql.cursor()
        cursor.execute("""select channel_table.Channel_name,channel_table.Channel_views from channel_table;""")
        data = cursor.fetchall()
        df_query= pd.DataFrame(data,columns=["Channel Name","Total Views"])
        st.dataframe(df_query,hide_index=True)     
     except:
         st.warning("No data in Database")
         
#Question 8
elif Question=="8. What are the names of all the channels that have published videos in the year 2022 ?":
     try:
        cursor = connection_for_dql.cursor()
        cursor.execute("""select  video_table.video_name, channel_table.Channel_name, video_table.published_date
                            from video_table
                            left join channel_table on channel_table.channel_id = video_table.channel_id
                            where extract(year from video_table.published_date)=2022;""")
        data = cursor.fetchall()
        df_query= pd.DataFrame(data,columns=["Video Title","Channel Name","Published Date"])
        st.dataframe(df_query,hide_index=True)  
     except:
         st.warning("No data in Database")

#Question 9
elif Question=="9. What is the average of all videos in each channel, and what are their corresponding channel names ?":
    
        #channel_id & Average of duartion
        cursor = connection_for_dql.cursor()
        cursor.execute("""select  channel_id, avg(video_table.duration_in_mins) as d from video_table group by channel_id""")
        data = cursor.fetchall()
        df_query= pd.DataFrame(data,columns=['channel_id','Average Duration (in mins)'])
        query_series = pd.Series(df_query['Average Duration (in mins)'])

        #channel_id & channel_name
        cursor = connection_for_dql.cursor()
        cursor.execute("""select  channel_table.Channel_id,channel_table.Channel_name from channel_table; """)
        data = cursor.fetchall()
        df_query11= pd.DataFrame(data,columns=['channel_id','Channel Name'])
        df_9_series = pd.Series(df_query11['Channel Name'])
        df_9 = pd.concat([df_9_series,query_series],axis=1)
        st.dataframe(df_9,hide_index=True)
    
  

#Question 10
elif Question=="10. Which videos have the highest number of comments, and what are their corresponding channel names ?":
     try:
        cursor = connection_for_dql.cursor()
        cursor.execute("""select video_table.video_name,video_table.comments,channel_table.Channel_name
                            from video_table
                            left join channel_table on channel_table.channel_id = video_table.channel_id
                            where video_table.comments order by video_table.comments desc""")
        data = cursor.fetchall()
        df_query= pd.DataFrame(data,columns=["Video Title","Comments","Channel Name"])
        st.dataframe(df_query,hide_index=True)  
     except:
         st.warning("No data in Database")

