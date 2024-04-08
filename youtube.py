from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st


#API key connection

def Api_connect(): 
    Api_Id="AIzaSyCP2M-NZ6gOgXYqYYGB6xdyBO1EWii3TI8"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#get channels informationx``
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data\
    
    
#get videos Id
def get_videos_id(channel_id):
    request = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    )
    response = request.execute()
    
    playlist_id = response.get('items', [])[0].get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads', None)
    
    if playlist_id:
        video_ids = []
        next_page_token = None
        
        while True:
            playlist_items_request = youtube.playlistItems().list(playlistId=playlist_id,
                part='contentDetails',
                maxResults=50,
                pageToken=next_page_token
            )
            playlist_items_response = playlist_items_request.execute()
            
            for item in playlist_items_response.get('items', []):
                video_ids.append(item['contentDetails']['videoId'])
            
            next_page_token = playlist_items_response.get('nextPageToken')
            
            if not next_page_token:
                break
        
        return video_ids
    else:
        print("Unable to retrieve playlist ID. Check if the channel ID is correct or if the channel has videos.")
        return []

# To convert the duration in 00:00:00 format
def convert_dur(s):
  l = []
  f = ''
  for i in s:
    if i.isnumeric():
      f = f+i
    else:
      if f:
        l.append(f)
        f = ''
  if 'H' not in s:
    l.insert(0,'00')
  if 'M' not in s:
    l.insert(1,'00')
  if 'S' not in s:
    l.insert(-1,'00')
  return ':'.join(l)

#get video information
def get_video_info(Video_ids):
    video_data=[]

    for video_id in Video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        
        

        for item in response["items"]:
            data=dict(  Channel_Name=item["snippet"]["channelTitle"],
                        Channel_Id=item["snippet"]["channelId"],
                        Video_Id=item["id"],
                        Tags=item['snippet'].get("tags"),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Title=item["snippet"]["title"],
                        Description=item["snippet"]["description"],
                        Published_Date=item['snippet']['publishedAt'].replace("Z",""),
                        Duration=convert_dur(item["contentDetails"]["duration"]),
                        Views=item['statistics'].get("viewCount"),
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get("commentCount"),
                        Favorite_Count=item["statistics"]["favoriteCount"],
                        Definition=item["contentDetails"]["definition"],
                        Caption_Status=item["contentDetails"]["caption"]
                        )
            video_data.append(data)
    return video_data


#get comment information
def get_comment_info(video_ids):
    Comment_Data=[]
    try:
        for video_id in video_ids:
                request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50
                )
                response=request.execute()
                for item in response['items']:
                    data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published_time=item['snippet']['topLevelComment']['snippet']['publishedAt'].replace("Z",""))
                    Comment_Data.append(data)
    except:
        pass 
    return Comment_Data


#get_playlist_details
def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(    
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
                )
                response =request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                        Playlist_Title=item['snippet']['localized']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        Playlist_Published_at=item['snippet']['publishedAt'].replace("Z",""),
                        Videos_Count=item['contentDetails']["itemCount"],
                        )
                        All_data.append(data) 
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data        


#upload to mongoDB
import pymongo
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_data"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vi_ids=get_videos_id(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    
    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,
                      "playlist_information":pl_details,
                      "video_information":vi_details,
                      "comment_information":com_details})
    
    return "upload completed sucessfully"

#Table creation for channels,playlists,videos,comments
def channels_table(channel_name_s):
    mydb=mysql.connector.connect(host='localhost',
                        user='root',
                        password='Ushasingh@123',
                        database='youtube_data')
    cursor=mydb.cursor()

  
    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(80))'''
    cursor.execute(create_query)
    mydb.commit()

    
    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["channel_information"])

    df_single_channel= pd.DataFrame(single_channel_details)



    for index,row in df_single_channel.iterrows():
        insert_query='''insert into channels(Channel_Name ,
                                                    Channel_Id,
                                                    Subscribers,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                        row['Channel_Id'],
                        row['Subscribers'],
                        row['Views'],
                        row['Total_Videos'],
                        row['Channel_Description'],
                        row['Playlist_Id'])

        try:    
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            news=f"Your provided Channel Name {channel_name_s} is Already Exists"
            
            return news

#Table creation for Playlist
def playlist_table(channel_name_s):
    mydb=mysql.connector.connect(host='localhost',
                        user='root',
                        password='Ushasingh@123',
                        database='youtube_data')
    cursor=mydb.cursor()

    
    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Playlist_Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        Playlist_Published_at timestamp,
                                                        Videos_Count int
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["playlist_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])

    for index,row in df_single_channel.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                                Playlist_Title,
                                                Channel_Id,
                                                Channel_Name,
                                                Videos_Count,
                                                Playlist_Published_at 
                                                )
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
                    row['Playlist_Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    row['Videos_Count'],
                    row['Playlist_Published_at']
                    )


        cursor.execute(insert_query,values)
        mydb.commit()

#Table creation for Videos
def video_table(channel_name_s):
    mydb=mysql.connector.connect(host='localhost',
                        user='root',
                        password='Ushasingh@123',
                        database='youtube_data')
    cursor=mydb.cursor()

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(30) primary key,
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Duration varchar(50),
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(10),
                                                        Caption_Status varchar(50),
                                                        Published_Date timestamp
                                                            )'''
    cursor.execute(create_query)
    mydb.commit()

    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["video_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])

    for index,row in df_single_channel  .iterrows():
        insert_query='''insert into videos(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Tags,
                                                        Thumbnail,
                                                        Description,
                                                        Duration,
                                                        Views,
                                                        Likes,
                                                        Comments,
                                                        Favorite_Count,
                                                        Definition,
                                                        Caption_Status,
                                                        Published_Date
                                                    )
                                                    
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''' 
        values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    str(row['Tags']),
                    row['Thumbnail'],
                    row['Description'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'],
                    row['Published_Date']
                    )


        cursor.execute(insert_query,values)
        mydb.commit()


#Table creation for Comments
def comments_table(channel_name_s):
    mydb=mysql.connector.connect(host='localhost',
                        user='root',
                        password='Ushasingh@123',
                        database='youtube_data')
    cursor=mydb.cursor()

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                            Comment_Text text,
                                                            Comment_Author varchar(150),
                                                            Comment_Published_time timestamp
                                                            )'''
    cursor.execute(create_query)
    mydb.commit()

    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["comment_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])

    for index,row in df_single_channel.iterrows():
        insert_query='''insert into comments(Comment_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published_time)
                                            
                                            values(%s,%s,%s,%s)''' 
        values=(row['Comment_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published_time']
                        )


        cursor.execute(insert_query,values)
        mydb.commit()

def tables(channel_name):

    news= channels_table(channel_name)
    if news:
        st.write(news)
    else:
        playlist_table(channel_name)
        video_table(channel_name)
        comments_table(channel_name)

    return "Tables Created Successfully"

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df


def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1


def show_videos_table():    
    vd_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vd_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vd_data["video_information"])):
            vd_list.append(vd_data["video_information"][i])
    df2=st.dataframe(vd_list)

    return df2


def show_comments_table():
    Cmt_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for cmt_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(cmt_data["comment_information"])):
            Cmt_list.append(cmt_data["comment_information"][i])
    df3=st.dataframe(Cmt_list)

    return df3

#streamlit part

# Color Variables
color_title = "#2E86C1"  # Dark Blue
color_header = "#3498DB"  # Light Blue
color_caption = "#2ECC71"  # Green
color_button = "#58D68D"  # Light Green

with st.sidebar:
    st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert) 


all_channels=[]
db=client["Youtube_data"]
coll1=db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"channel_information":1,}):
    all_channels.append(ch_data["channel_information"]["Channel_Name"])


unique_channel= st.selectbox("Select the Channel",all_channels)

if st.button("Migrate to Sql"):
    Table=tables(unique_channel)
    st.success(Table)

show_table=st.radio("SELECT THE TABLE TO VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()

#SQL Connection

mydb=mysql.connector.connect(host='localhost',
                        user='root',
                        password='Ushasingh@123',
                        database='youtube_data')
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with higest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))
if question=="1. All the videos and the channel name":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question == "2. channels with most number of videos":
    query2 = '''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name", "No of videos"])
    st.write(df2)

elif question == "3. 10 most viewed videos":
    query3 = '''select views as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["views", "channel name", "videotitle"])
    st.write(df3)

elif question == "4. comments in each videos":
    query4 = '''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["no of comments", "videotitle"])
    st.write(df4)

elif question == "5. Videos with higest likes":
    query5 = '''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["videotitle", "channelname", "likecount"])
    st.write(df5)

elif question == "6. likes of all videos":
    query6 = '''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["likecount", "videotitle"])
    st.write(df6)

elif question == "7. views of each channel":
    query7 = '''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query7)
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["channel name", "totalviews"])
    st.write(df7)

elif question == "8. videos published in the year of 2022":
    query8 = '''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["videotitle", "published_date", "channelname"])
    st.write(df8)

elif question == "9. average duration of all videos in each channel":
    query9 = '''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["channelname", "averageduration"])

    T9 = []
    for index, row in df9.iterrows():
        channel_title = row["channelname"]
        average_duration = row["averageduration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle=channel_title, avgduration=average_duration_str))
    df1 = pd.DataFrame(T9)
    st.write(df1)

elif question == "10. videos with highest number of comments":
    query10 = '''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["video title", "channel name", "comments"])
    st.write(df10)
