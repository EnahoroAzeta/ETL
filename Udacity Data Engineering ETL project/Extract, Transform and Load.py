#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os


# In[2]:


import glob


# In[3]:


import psycopg2
import pandas as pd


# # DATA EXTRACTION

# # Extracting the song_data and merging all json files to one document

# In[174]:


song_filepath = ''
song_files = []
def get_data(filepath):
    # get all files matching extension from directory
    
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            with open(f,) as infile:
                song_files.append(json.load(infile))

        with open("merged_file.json",'w') as outfile:

            json.dump(song_files, outfile)
    print(song_files)

get_data(song_filepath)
song_files


# # Extracting the log_data and merging all json files to one document

# In[41]:


# Extract All Log data and combine them into one Json file
file_path =''
#Get a list of file names in the current folder  
filenames=os.listdir(filedir)
#Open the Result2.json file in the current directory, if not, create it
f=open('Result2.json','w')
#First traverse the file name `insert code piece here`
for filename in filenames:
    filepath = filedir+'/'+filename
    #Traverse a single file, read the number of lines
    for line in open(filepath):
        f.writelines(line)
        f.write('\n')
#Close file
f.close()


# # DATA TRANSFORMATION

# # Time dataframe

# In[96]:


# convert timestamp column to datetime
t = pd.to_datetime(df['ts'], unit='ms')

# insert time data records
time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear,
                t.dt.month, t.dt.year, t.dt.weekday)
column_labels = ('timestamp', 'hour', 'day', 'weekofyear', 'month',
                    'year', 'weekday')
time_df = pd.DataFrame({k:v for k, v in zip(column_labels, time_data)})

time_df


# # Song, Artist, Time, User and Song play Dataframes

# In[134]:


# Convert Combined JSON file into a dataframe to generate sub dataframes

f = pd.read_json('Result2.json', lines=True)

df = f[f['page'] == 'NextSong']

df

# log_df= df[["artist", "firstName", "gender", "lastName", "length", "level", "location", "page", "sessionId", "song", "userId"]]



# In[461]:


timestamp_df = time_df[['timestamp']]

random_data = np.random.randint(1, 10000000000, size=6820)
random_data

timestamp_insert = timestamp_df

timestamp_insert['time_id'] = random_data

timestamp_insert['date'] = [d.date() for d in timestamp_insert['timestamp']]
timestamp_insert['time'] = [d.time() for d in timestamp_insert['timestamp']]

timestamp_insert_df = timestamp_insert[['time_id', 'timestamp', 'date', 'time']]


timestamp_insert_df


# In[154]:


song_id = songs_df[['song_id']]

song_id


# In[444]:


song_play_df = df[['sessionId', 'userId', 'level', 'song', 'artist', 'length', 'location', 'userAgent', 'firstName', 'lastName', 'gender']]
song_play_df = song_play_df.join(timestamp_df)

song_play_df 


# In[362]:


import random
import string
import numpy as np
import hashlib

song_ref = pd.DataFrame(song_play_df.song.dropna().unique(), columns=['song'])
song_ref


# def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
#    return ''.join(random.SystemRandom().choice(chars) for _ in range(size))

# song_ref['song_id'] = song_ref['song'].apply(lambda x : id_generator(3))

rand_data = np.random.randint(1, 1000000000, size=5189)
rand_data


song_ref['song_id'] = rand_data

song_ref
# song_ref.nunique()



# In[354]:


artist_ref = pd.DataFrame(song_play_df.artist.dropna().unique(), columns=['artist'])
artist_ref


data = np.random.randint(1, 1000000000, size=3148)
data

artist_ref['artist_id']= data

artist_ref


# In[445]:


df_song_play = song_play_df.merge(song_ref, on='song', how='left')

df_song_play

df_song_play1 = df_song_play.merge(artist_ref, on='artist', how='left')

rawdata = np.random.randint(1, 10000000000, size=6820)
rawdata

df_song_play1['songplay_id']= rawdata

df_song_play1


# In[411]:


song_table_insert_df = df_song_play1[['song_id', 'song', 'artist_id']]

song_table_insert_df.columns = ['song_id', 'title', 'artist_id']

song_table_insert_df = song_table_insert_df.drop_duplicates()

song_table_insert_df.loc[song_table_insert_df['song_id'] == 284103908]
song_table_insert_df.song_id.duplicated().sum()
song_table_insert_df.loc[song_table_insert_df.song_id.duplicated(), :]

song_table_insert_df.drop_duplicates(subset=['song_id'], inplace=True)

song_table_insert_df


# In[420]:


artist_table_insert_df = artist_ref[['artist', 'artist_id']]

artist_table_insert_df.columns =['name', 'artist_id']

artist_table_insert_df


# In[439]:


song_play_insert_df = df_song_play1[['songplay_id', 'timestamp', 'level', 'userId', 'song_id', 'artist_id', 'sessionId','location', 'userAgent']]

song_play_insert_df.columns =['songplay_id', 'start_time', 'level', 'user_id', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent']
song_play_insert_df


# In[453]:


user_df = df_song_play1[['userId', 'firstName', 'lastName', 'gender','level']]

user_df.drop_duplicates(subset=['userId', 'firstName', 'lastName', 'gender'], inplace=True)
user_df

user_df.columns = ['user_id','first_name', 'last_name', 'gender', 'level']

user_df


# # LOAD DATA TO DATABASE

# In[ ]:


pip install SQLAlchemy


# In[373]:


from sqlalchemy import create_engine


# In[374]:


engine = create_engine('postgresql://postgres:1111@localhost:5432/Music_DB')


# In[412]:


song_table_insert_df.to_sql('song', con = engine, if_exists = 'append', chunksize = 10000, index=False)


# In[421]:


artist_table_insert_df.to_sql('artist', con = engine, if_exists = 'append', chunksize = 100000, index=False)


# In[441]:


song_play_insert_df.to_sql('songplay', con = engine, if_exists = 'append', chunksize = 100000, index=False)


# In[454]:


user_df.to_sql('user_table', con = engine, if_exists = 'append', chunksize = 100000, index=False)


# In[466]:


timestamp_insert_df.to_sql('time', con = engine, if_exists = 'append', chunksize = 100000, index=False)


# In[ ]:




