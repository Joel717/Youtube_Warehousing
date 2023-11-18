#installing required packages 
import pymongo
import pandas as pd
from googleapiclient.discovery import build
import streamlit as st
from sqlalchemy import create_engine,types  
from sqlalchemy.dialects.postgresql import VARCHAR
from sqlalchemy.types import Text, TIMESTAMP, Interval, BigInteger, Integer

#connecting with youtube api
def Api_connect():
    Api_Id = 'AIzaSyDX6OReiqJzjm1zwQQMmhtrQCdc7g7Bszg'
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_Id)
    return youtube

youtube = Api_connect()

#requesting channel info from youtube api
def request_channel_info(channel_id):
    request=youtube.channels().list(
                    part='snippet,ContentDetails,Statistics',
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_name=i['snippet']['title'],
                Channel_id=i['id'],
                Subscribers_count=i['statistics']['subscriberCount'],
                Views=i['statistics']['viewCount'],
                Total_videos=i['statistics']['videoCount'],
                description=i['snippet']['description'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
        return data 


#requesting playlist info  from youtube api
def request_playlist_info(channel_id):
    playlist_info = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        responsep = request.execute()

        for item in responsep['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            playlist_info.append(data)
        next_page_token = responsep.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return playlist_info

#requesting video ids
def request_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token= None

    while True:
        responsev=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=playlist_id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(responsev['items'])):
            video_ids.append(responsev['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=responsev.get('nextPageToken')

        if next_page_token is None:
            break 
    return video_ids    

#requesting video info
def request_video_info(video_ids):

    video_info = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id)
        responsev2 = request.execute()

        for item in responsev2['items']:
            data = dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags', []),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet']['description'],
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics']['viewCount'],
                    Likes=item['statistics'].get('likeCount', 0),
                    Comments=item['statistics'].get('commentCount', 0),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_info.append(data)

    return video_info



#requesting comment info

def request_comment_info(video_ids):
    comment_info=[]
    try:
        for video_id in video_ids:

            request = youtube.commentThreads().list(
                part='snippet',
                videoId='XLabASFnS_s',
                maxResults=10
                )
            responsec=request.execute()

            for item in responsec['items']:
                comments=dict(comment_id = item["snippet"]["topLevelComment"]["id"],
                              Video_Id = item["snippet"]["videoId"],
                              Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                              Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                              Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                comment_info.append(comments)
    except:
            pass
    return comment_info          

#connecting to mongoDB server 
client=pymongo.MongoClient('mongodb+srv://devlinjoel717:toughpassword29@cluster0.oisdiyg.mongodb.net/?retryWrites=true&w=majority')
db=client['yt_harvest']


#uploading to mondodb

def channel_details(channel_id):
    channel_info=request_channel_info(channel_id)
    playlist_info=request_playlist_info(channel_id)
    videoIds=request_video_ids(channel_id)
    video_info=request_video_info(videoIds)
    comment_info=request_comment_info(videoIds)

    coll1=db['channel_details']

    coll1.insert_one({'channel':channel_info,'playlist':playlist_info,'video':video_info,'comments':comment_info})
    
    return 'Upload : SUCCESSFUL'
    
#using SQLalchemy to create postgres engine for database connection
engine=engine=create_engine('postgresql://postgres:postgres@localhost:5432/Youtube_data')

#creating a function to upload channel data to sql
def channels_sql():
    
    db = client["yt_harvest"]
    coll1 = db["channel_details"]

    channel_list = [ch_data["channel"] for ch_data in coll1.find({}, {"_id": 0, "channel": 1})]
    global dfc #making dfc a global variable to make it easier to be called later on
    
    dfc = pd.DataFrame(channel_list)

    dfc=dfc.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)

    dtypes = {
    'Channel_name': VARCHAR(length=100),
    'Channel_id': VARCHAR(length=80),
    'Subscribers_count': BigInteger(),
    'Views': BigInteger(),
    'Total_videos': Integer(),
    'description': Text(),
    'playlist_id': VARCHAR(length=50)
    }


    table_name = 'channels'

    
    dfc.to_sql(table_name,con=engine,index=False,if_exists='replace',dtype=dtypes)
    return dfc
channels_sql()
#creating a function to upload playlist data to sql

def playlist_sql():
    db = client["yt_harvest"]
    coll1 = db["channel_details"]
    
    play_list = []
    for plays in coll1.find({}, {"_id": 0, "playlist": 1}):
        for i in range(len(plays["playlist"])):
            play_list.append(plays["playlist"][i])
    
    
    global dfp
    dfp = pd.DataFrame(play_list)

    dtypes = {
        'PlaylistId': VARCHAR(length=100),
        'Title': VARCHAR(length=80),
        'ChannelId': VARCHAR(length=100),
        'ChannelName': VARCHAR(length=100),
        'PublishedAt': TIMESTAMP(),
        'VideoCount': Integer()
    }

    table_name = 'playlists'

    dfp.to_sql(table_name,con=engine,index=False,if_exists='replace',dtype=dtypes)
    return dfp
playlist_sql()

#creating a function to upload video data to sql
def videos_sql():
   
    db = client["yt_harvest"]
    coll1 = db["channel_details"]
    v_list=[]
    for v_data in coll1.find({}, {"_id": 0, "video": 1}):
        v_list.extend(v_data.get("video", []))

    global dfv
    dfv = pd.DataFrame(v_list)

    dtypes = {
    'Channel_Name': VARCHAR(length=150),
    'Channel_Id': VARCHAR(length=100),
    'Video_Id': VARCHAR(length=50),
    'Title': VARCHAR(length=150),
    'Tags': Text(),
    'Thumbnail': VARCHAR(length=225),
    'Description': Text(),
    'Published_Date': TIMESTAMP(),
    'Duration': Interval(),
    'Views': BigInteger(),
    'Likes': BigInteger(),
    'Comments': Integer(),
    'Favorite_Count': Integer(),
    'Definition': VARCHAR(length=10),
    'Caption_Status': VARCHAR(length=50)}



    table_name = 'videos'

    dfv.to_sql(table_name,con=engine,index=False,if_exists='replace',dtype=dtypes)
    return dfv
videos_sql()

#creating a function to upload comments data to sql
def comments_sql():
    db = client["yt_harvest"]
    coll1 = db["channel_details"]
    import pandas as pd

    com_list = []
    for comms in coll1.find({}, {'_id': 0, 'comments': 1}):
        com_list.extend(comms.get('comments', []))
    global dfco
    dfco = pd.DataFrame(com_list)

        
    dfco=pd.DataFrame(com_list)

    dtypes = {
        'comment_id': VARCHAR(length=100),
        'Video_Id': VARCHAR(length=80),
        'Comment_Text': Text(),
        'Comment_Author': VARCHAR(length=150),
        'Comment_Published': TIMESTAMP()}

    table_name='comments'

    dfco.to_sql(table_name,con=engine,index=False,if_exists='replace',dtype=dtypes)
    return dfco
comments_sql()

#creating a function that creates all tables at once
def tables():
    channels_sql()
    playlist_sql()
    videos_sql()
    comments_sql()
    return 'SQL Migration:SUCCESSFUL'

#creating streamlit sidebar and title 
st.title(':red[_YouTube_] _Data Harvesting & Warehousing_')
st.subheader(' ',divider='rainbow')
with st.sidebar:
     st.title(":red[_YouTube Data Harvesting & Warehousing_]")
     st.header(' ',divider='rainbow')
     st.header("Collect and store data using Youtube API",)
     st.caption('''This streamlit application enables users to collect and store
                data from Youtube channels using Youtube API''')
     st.caption("Hint: To find channel ID--> Channel Home > share>copy channel ID")
     st.caption("This app is created by Joel Gracelin")
     st.write("[Ghithub](https://github.com/Joel717)")

#creating streamlit input box for channel id
channel_id=st.text_input('ENTER CHANNEL ID')
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

#creating streamlit buttons to get data from api and load it in mongoDB
if st.button("Collect and load data to MongoDB"):
    for channel in channels:
        ch_ids = []
        db = client["yt_harvest"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel":1}):
            ch_ids.append(ch_data["channel"]["Channel_id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel)
            st.success(output)
st.subheader(' ',divider='rainbow')
#creating button to migrate data to SQL  

st.write("<h3 style='font-size:0.89em; font-weight: lighter;'>MIGRATE TO SQL</h3>",unsafe_allow_html=True)         
if st.button("Migrate Data to SQL"):
    display = tables()
    st.success(display)

#creating functions for streamlit radio
def display_channels_table():
   
    global dfc
    dfc=st.dataframe(dfc)
    

def display_playlists_table():
    
    global dfp
    dfp=st.dataframe(dfp)

     

def display_videos_table():
   
    global dfv
    dfv=st.dataframe(dfv)
    

def display_comments_table():
    
    global dfco
    dfco=st.write(dfco)
    
#creating streamlit radio to display tables 
show_table=st.radio('Select Table to View',
                    ('channels','videos','playlists','comments'),
                    index=None,
                    horizontal=True)
if show_table == "channels":
    display_channels_table()
elif show_table == "playlists":
    display_playlists_table()
elif show_table =="videos":
    display_videos_table()
elif show_table == "comments":
    display_comments_table()

st.subheader(' ',divider='rainbow')
#creating dropdown for questions 
question = st.selectbox(
    'PLEASE SELECT YOUR QUESTION',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Number ofComments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments',
     ),
     index=None)

#creating solutions for questions
engine=engine=create_engine('postgresql://postgres:postgres@localhost:5432/Youtube_data')

if question == '1. All the videos and the Channel Name':
    query1 = 'SELECT "Title" AS videonames, "Channel_Name" AS ChannelName FROM videos;'
    t1 = pd.read_sql(query1, con=engine)
    st.write(t1)

elif question == '2. Channels with most number of videos':
    query2 = 'SELECT "Channel_name" AS ChannelName, "Total_videos" AS No_of_Videos FROM channels ORDER BY "Total_videos" DESC;'
    t2 = pd.read_sql(query2, con=engine)
    st.write(t2)

elif question == '3. 10 most viewed videos':
    query3 = '''SELECT "Views" AS views, "Channel_Name" AS ChannelName, "Title" AS VideoTitle FROM videos 
                WHERE "Views" IS NOT NULL ORDER BY "Views" DESC LIMIT 10;'''
    t3 = pd.read_sql(query3, con=engine)
    st.write(t3)

elif question == '4. Number of Comments in each video':
    query4 = 'SELECT "Comments" AS No_comments, "Title" AS VideoTitle FROM videos WHERE "Comments" IS NOT NULL;'
    t4 = pd.read_sql(query4, con=engine)
    st.write(t4)

elif question == '5. Videos with highest likes':
    query5 = '''SELECT "Title" AS VideoTitle, "Channel_Name" AS ChannelName, "Likes" AS LikesCount FROM videos 
                WHERE "Likes" IS NOT NULL ORDER BY "Likes" DESC;'''
    t5 = pd.read_sql(query5, con=engine)
    st.write(t5)

elif question == '6. likes of all videos':
    query6 = '''SELECT "Likes" AS likeCount, "Title" AS VideoTitle FROM videos;'''
    t6 = pd.read_sql(query6, con=engine)
    st.write(t6)

elif question == '7. views of each channel':
    query7 = 'SELECT "Channel_name" AS ChannelName, "Views" AS Channelviews FROM channels;'
    t7 = pd.read_sql(query7, con=engine)
    st.write(t7)

elif question == '8. videos published in the year 2022':
    query8 = '''SELECT "Title" AS Video_Title, "Published_Date" AS VideoRelease, "Channel_Name" AS ChannelName FROM videos 
                WHERE EXTRACT(YEAR FROM "Published_Date") = 2022;'''
    t8 = pd.read_sql(query8, con=engine)
    st.write(t8)

elif question == '9. average duration of all videos in each channel':
    query9 = 'SELECT "Channel_Name" AS ChannelName, AVG(EXTRACT(EPOCH FROM "Duration")) AS average_duration_in_seconds FROM videos GROUP BY "Channel_Name";'
    t9 = pd.read_sql(query9, con=engine)
    t9 = t9.rename(columns={'ChannelName': 'Channel Title', 'average_duration': 'Average Duration'})
    st.write(t9)


elif question == '10. videos with highest number of comments':
    query10 = '''SELECT "Title" AS VideoTitle, "Channel_Name" AS ChannelName, "Comments" AS Comments FROM videos 
                WHERE "Comments" IS NOT NULL ORDER BY "Comments" DESC;'''
    t10 = pd.read_sql(query10, con=engine)
    t10 = t10.rename(columns={'VideoTitle': 'Video Title', 'ChannelName': 'Channel Name', 'Comments': 'No Of Comments'})
    st.write(t10)
