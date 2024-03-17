from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st

#Api key connection

def Api_connect():
    Api_Id="AIzaSyB6La9IEwHmMvi1UGq4kCiYHOl0u68Wq44"

    Api_service_name="youtube"
    Api_version="v3"

    youtube=build(Api_service_name,Api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()


#get channel details

def get_channel_info(channel_id):
    request=youtube.channels().list(
        part="snippet,contentDetails,Statistics",
        id=channel_id
    )
    response=request.execute()

    for i in range(0,len(response['items'])):
        data=dict(Channel_Name=response['items'][i]['snippet']['title'],
                  Channel_Id=response['items'][i]['id'],
                  Subscription_Count=response['items'][i]['statistics']['subscriberCount'],
                  Views=response['items'][i]['statistics']['viewCount'],
                  Total_Videos=response['items'][i]['statistics']['videoCount'],
                  Channel_Description=response['items'][i]['snippet']['description'],
                  Playlist_Id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
    return data


#get video ids

def get_videos_ids(channel_id):
    video_ids=[]

    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    
    Playlist_Id=response['items'][0][ 'contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=Playlist_Id,
                                        maxResults=50,
                                        pageToken=next_page_token).execute()
        for i in range(len(response1["items"])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        if next_page_token==None:
            break
    return video_ids


#get video details

def get_videos_info(Video_ids):
    video_data=[]
    for video_id in Video_ids:
        response = youtube.videos().list(
                part="snippet,ContentDetails,statistics",
                id=video_id
            ).execute()
        for items in response["items"]:
            
            data=dict( 
                    Channel_Name = items['snippet']['channelTitle'],
                    Channel_Id = items['snippet']['channelId'],
                    Video_Id = items['id'],
                    Title =items['snippet']['title'],
                    Tags =items['snippet'].get('tags'),
                    Thumbnail =items['snippet']['thumbnails']['default']['url'],
                    Description = items['snippet'].get('description'),
                    Published_Date =items['snippet']['publishedAt'],
                    Duration =items['contentDetails']['duration'],
                    Views =items['statistics'].get('viewCount'),
                    Likes =items['statistics'].get('likeCount'),
                    Comments = items['statistics'].get('commentCount'),
                    Favorite_Count =items['statistics']['favoriteCount'],
                    Definition =items['contentDetails']['definition'],
                    Caption_Status =items['contentDetails']['caption'] 
            )
            video_data.append(data)
    return video_data


#get comment information

def get_Comment_Information(Video_ids):
    Comment_Information=[]
    try:
        for video_ids in Video_ids:
            response=youtube.commentThreads().list(
                part='snippet',
                videoId=video_ids,
                maxResults=100).execute()

            for items in response['items']:
                data=dict(Comment_Id= items['snippet']['topLevelComment']['id'],
                        Video_Id= items['snippet']['videoId'],
                        Comment_Text= items['snippet']['topLevelComment']['snippet']['textOriginal'],
                        Comment_Author= items ['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published= items ['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                Comment_Information.append(data)
    except:
        pass

    return Comment_Information


#get playlist information

def get_playlist_info(channel_id):
    All_data=[]
    next_page_token=None
    while True:
        response=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token).execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)
        
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data


#upload to MongoDB

import pymongo
import certifi

client = pymongo.MongoClient("mongodb+srv://ajaysuria78:1234@cluster0.siwceqi.mongodb.net/?retryWrites=true&w=majority", tlsCAFile=certifi.where())
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_info(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_videos_info(vi_ids)
    com_details=get_Comment_Information(vi_ids)
   
    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,
                      "playlist_information":pl_details,
                      "video_information":vi_details,
                     "comment_information":com_details})
    return "upload success"

mydb=mysql.connector.connect(
    host='localhost',
    user='root',
    passwd="ajaysuria@070",
    auth_plugin='mysql_native_password',
    database="youtube_data")
mycursor=mydb.cursor(buffered=True)


#Table creation for channels 

def channels_table():
    mydb=mysql.connector.connect(
        host='localhost',
        user='root',
        passwd="ajaysuria@070",
        auth_plugin='mysql_native_password',
        database="youtube_data")
    mycursor=mydb.cursor(buffered=True)

    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscription_Count bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
        mycursor.execute(create_query)
        mydb.commit()
    except:
        print("Channels table already created")


    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscription_Count,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("Channel values are already inserted")


#Table creation for playlist 

def playlist_table():
    mydb=mysql.connector.connect(
            host='localhost',
            user='root',
            passwd="ajaysuria@070",
            auth_plugin='mysql_native_password')
    mycursor=mydb.cursor(buffered=True)
        
    #Table creation for channels in MySql
    mycursor.execute("create database if not exists youtube_data")
    mycursor.execute("use youtube_data")
    mycursor.execute("drop table if exists playlists")
    mydb.commit()
    try:
        mycursor.execute('''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                                    Title varchar(100),
                                                                    Channel_Id varchar(100),
                                                                    Channel_Name varchar(100),
                                                                    Published_At timestamp,
                                                                    Video_Count int)''')
        mydb.commit()

    except:
        print("Error in creating table")
        
    #Extracting  playlists details from mongodb and making it to DataFrame
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=pd.DataFrame(pl_list)

    #Pushing data to Mysql
    for index,row in df1.iterrows():
        
        row['PublishedAt'] = pd.to_datetime(row['PublishedAt']).strftime('%Y-%m-%d %H:%M:%S')
        
        insert_query='''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            Published_At,
                                            Video_Count)
                                                                        
                                            values(%s,%s,%s,%s,%s,%s)'''
    

        values=(row['Playlist_Id'],
                                row['Title'],
                                row['Channel_Id'],
                                row['Channel_Name'],
                                row['PublishedAt'],
                                row['Video_Count'])
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        except mysql.connector.errors.IntegrityError:
                print("Channel values already inserted")


#Table creation for videos 

def videos_table():
    mydb=mysql.connector.connect(
        host='localhost',
        user='root',
        passwd="ajaysuria@070",
        auth_plugin='mysql_native_password',
        database="youtube_data")
    mycursor=mydb.cursor(buffered=True)

    drop_query='''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration TIME,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50)
                                                        )'''
    mycursor.execute(create_query)
    mydb.commit()


    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)

    def parse_duration(duration):
        # Remove the 'PT' prefix
        duration = duration[2:]

        hours = 0
        minutes = 0
        seconds = 0

        # Extract hours
        if 'H' in duration:
            hours, duration = duration.split('H')
            hours = int(hours)

        # Extract minutes
        if 'M' in duration:
            minutes, duration = duration.split('M')
            minutes = int(minutes)

        # Extract seconds
        if 'S' in duration:
            seconds, duration = duration.split('S')
            seconds = int(seconds)

        return "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds)

    # Apply the function to the 'Duration' column
    df2['Duration'] = df2['Duration'].apply(parse_duration)


    for index,row in df2.iterrows():
        
        row['Published_Date'] = pd.to_datetime(row['Published_Date']).strftime('%Y-%m-%d %H:%M:%S')

        insert_query='''insert into videos(Channel_Name,
                                        Channel_Id,
                                        Video_Id,
                                        Title,
                                        Tags,
                                        Thumbnail,
                                        Description,
                                        Published_Date,
                                        Duration,
                                        Views,
                                        Likes,
                                        Comments,
                                        Favorite_Count,
                                        Definition,
                                        Caption_Status
                                        )

                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        # Check if 'Tags' is a list before joining
        tags = ','.join(row['Tags']) if isinstance(row['Tags'], list) else ''

        values=(row['Channel_Name'],
        row['Channel_Id'],
        row['Video_Id'],
        row['Title'],
        tags,  # Use the joined string
        row['Thumbnail'],
        row['Description'],
        row['Published_Date'],
        row['Duration'],
        row['Views'],
        row['Likes'],
        row['Comments'],
        row['Favorite_Count'],
        row['Definition'],
        row['Caption_Status']
        )

        mycursor.execute(insert_query,values)
        mydb.commit()


#Table creation for comments 

def comments_table():
    mydb=mysql.connector.connect(
        host='localhost',
        user='root',
        passwd="ajaysuria@070",
        auth_plugin='mysql_native_password',
        database="youtube_data")
    mycursor=mydb.cursor(buffered=True)

    drop_query='''drop table if exists comments'''
    mycursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                        )'''
    mycursor.execute(create_query)
    mydb.commit()

    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
        
        row['Comment_Published'] = pd.to_datetime(row['Comment_Published']).strftime('%Y-%m-%d %H:%M:%S')

        insert_query='''insert into comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published
                                            )

                                            values(%s,%s,%s,%s,%s)'''
        
        
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
                )
        mycursor.execute(insert_query,values)
        mydb.commit()

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return"Tables Created Succesfully"



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
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=st.dataframe(pl_list)

    return df1


def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2


def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3


#streamlit part

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Colllection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to Sql"):
    Table=tables()
    st.succes(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()



#sql connection

mydb=mysql.connector.connect(
    host='localhost',
    user='root',
    passwd="ajaysuria@070",
    auth_plugin='mysql_native_password',
    database="youtube_data")
mycursor=mydb.cursor(buffered=True)

question=st.selectbox("select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. videos with highest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))

if question=="1. All the videos and the channel name":
    query1='''select channel_name,title as Video_title
                from videos'''
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=['channel_name','Video_title'])
    st.write(df)


elif question=="2. channels with most number of videos":
    query2='''select channel_name,total_videos as video_count 
                from channels
                order by total_videos desc'''
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel_name","video_count"])
    st.write(df2)


elif question=="3. 10 most viewed videos":
    query3='''select channel_name,title as video_title,views
                from videos
                where views is not null
                order by views desc
                limit 10'''
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=["channel_name","video_title","views"])
    st.write(df3)

elif question=="4. comments in each videos":
    query4='''select channel_name,title as video_title,comments as comment_count
                from videos
                where comments is not null
                order by comments desc'''
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=["channel_name","video_title","comment_count"])
    st.write(df4)

elif question=="5. videos with highest likes":
    query5='''select channel_name,title as video_title,likes as likes_count
                from videos
                where likes is not null
                order by likes desc
                limit 3'''
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=["channel_name","video_title","comment_count"])
    st.write(df5)

elif question=="6. likes of all videos":
    query6='''select channel_name,title as video_title,likes as likes_count
                from videos
                where likes is not null
                order by likes desc
                '''
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=["channel_name","video_title","likes_count"])
    st.write(df6)

elif question=="7. views of each channel":
    query7='''select channel_name,views as total_views
            from channels
            '''
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel_name","total_views"])
    st.write(df7)


elif question=="8. videos published in the year of 2022":
    query8='''select channel_name,title as video_title,Published_Date as released_2022 
                from videos
                where extract(year from published_date)=2022
                '''
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=["channel_name","video_title","released_2022"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":
    query9='''select  channel_name,avg(duration) as average_duration
                from videos
                group by channel_name
                '''
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channel_name","average_duration"])
    st.write(df9)

elif question=="10. videos with highest number of comments":
    query10='''select channel_name,title as video_title,comments as comment_count
                from videos
                where comments is not null
                order by comments desc
                limit 3
                '''
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=["channel_name","video_title","comment_count"])
    st.write(df10)

