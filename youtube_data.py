#Scrapping data from Youtube via using google-api-python-client.   
import googleapiclient.discovery        
import pandas as pd         

#setup connection for youtube API
api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyCDs56VB8ZsYinqZrVzHY8vwcVOTRcFEwc"     
youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey=api_key)


# Channel details.
def channel_details(youtube,channel_id):
    request = youtube.channels().list(
            id = channel_id,
            part = "snippet,contentDetails,statistics",
        )
    response = request.execute()

    data = dict(Channel_id = str(channel_id),
            Channel_name = str(response ['items'][0]['snippet']['title']),
            Channel_description = str(response ['items'][0]['snippet']['description']),
            Channel_subcribers = int(response ['items'][0]['statistics']['subscriberCount']),
            Channel_views = int(response ['items'][0]['statistics']['viewCount']),
            Channel_video_count = int(response ['items'][0]['statistics']['videoCount']),
            Channel_playlist_ID = str(response ['items'][0]['contentDetails']['relatedPlaylists']['uploads']))
    
    return data


# Video ids.        
def get_video_Ids(Playlist_ID):     
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
def get_video_details(video_Ids,Playlist_ID):
    video_details_lister=[]
    for video_id in video_Ids:
        request = youtube.videos().list(
            part = "snippet,contentDetails,statistics", id= video_id
        )
        response = request.execute()
        data = { "video_id" : video_id,
            "playlist_id" : Playlist_ID,
            "video_name" : response['items'][0]['snippet']['title'],
            "video_description" : response['items'][0]['snippet']['description'],
            "published_date" : response['items'][0]['snippet']['publishedAt'],
            "duration" : response['items'][0]['contentDetails']['duration'],
            "thumbnail" : response['items'][0]['snippet']['thumbnails']['default']['url'],
            "caption_status" : response['items'][0]['contentDetails']['caption']
        }
        try:#view
            view_count_to_list = {"view_count" : response['items'][0]['statistics']['viewCount']}
            data.update(view_count_to_list)
        except:
                view_count_to_list={"view_count":"0"}
                data.update(view_count_to_list)
        try:#like
            like_count_to_list ={"like_count" : response['items'][0]['statistics']['likeCount']}
            data.update(like_count_to_list)
        except:
            like_count_to_list ={"like_count" : "0"}
            data.update(like_count_to_list)
        try:#favorite
            favorite_count_to_list = {"favorite_count" : response['items'][0]['statistics']['favoriteCount']}
            data.update(favorite_count_to_list)
        except:
            favorite_count_to_list = {"favorite_count" : "0"}
            data.update(favorite_count_to_list)
        try:#comment
            comment_to_list = {"comments" : response['items'][0]['statistics']['commentCount']}
            data.update(comment_to_list)
        except:
            comment_to_list = {"comments" : "0"}
            data.update(comment_to_list)
        video_details_lister.append(data)
    return video_details_lister


#get comments
def get_comments(youtube,video_Ids):
        comment_details = []
        for i in range(0,50):
                request = youtube.commentThreads().list(
                        part = "snippet",
                        videoId= video_Ids[i],
                        maxResults=50
                )
                response = request.execute()
                comment_dict = {"comment_id":response['items'][0]['snippet']['topLevelComment']['id'],
                                "video_id":video_Ids[i],
                                "comment_text":response['items'][0]['snippet']['topLevelComment']['snippet']['textOriginal'],
                                "comment_author":response['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                "Published_date":response['items'][0]['snippet']['topLevelComment']['snippet']['publishedAt']
                                }
                comment_details.append(comment_dict)
        return comment_details


channel_ID = input()
def main_exe():
    channel_details = channel_details(youtube,channel_ID)   #get channel details
    Playlist_ID = channel_details['Channel_playlist_ID']    #Playlist_id from channel details
    video_Ids = get_video_Ids(Playlist_ID)  #get video ids
    video_details = get_video_details(video_Ids,Playlist_ID)    #video_details
    comment_details = get_comments(youtube,video_Ids)   #comment_details

    return channel_details,Playlist_ID,video_Ids,video_details,comment_details
execute_all = main_exe()