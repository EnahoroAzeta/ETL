#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install psycopg2


# In[36]:


import psycopg2


# In[37]:


conn = psycopg2.connect("dbname=Music_DB user=postgres password=1111")


# In[38]:


cursor = conn.cursor()


# In[39]:


conn.autocommit = True


# In[64]:


songplay_table_create = (""" 
CREATE TABLE IF NOT EXISTS songplay (songplay_id bigint UNIQUE PRIMARY KEY,
    start_time timestamp, user_id bigint, level varchar, song_id varchar,
    artist_id int, session_id bigint, location varchar, user_agent varchar);
""")

cursor.execute(songplay_table_create)


# In[65]:


user_table_create = ("""
CREATE TABLE IF NOT EXISTS User_table (User_ID int UNIQUE PRIMARY KEY, First_name varchar, Last_name varchar, Gender varchar, Level varchar);
""")

cursor.execute(user_table_create)


# In[56]:


song_table_create = ("""
CREATE TABLE IF NOT EXISTS Song (Song_ID int UNIQUE PRIMARY KEY, Title varchar, Artist_ID int, Year int, Duration int);
""")

cursor.execute(song_table_create)


# In[60]:


artist_table_create = ("""
CREATE TABLE IF NOT EXISTS Artist (Artist_ID int UNIQUE PRIMARY KEY, Name varchar, Location varchar, Lattitude varchar, Longitude varchar);
""")

cursor.execute(artist_table_create)


# In[78]:


time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (time_id bigint UNIQUE PRIMARY KEY, timestamp timestamp, date date, time time
   );
""")

cursor.execute(time_table_create)


# In[77]:


# DROP TABLES

songplay_table_drop = "DROP TABLE songplay;"
user_table_drop = "DROP TABLE user_table;"
song_table_drop = "DROP TABLE Song;"
artist_table_drop = "DROP TABLE Artist;"
time_table_drop = "DROP TABLE time;"


cursor.execute(time_table_drop)


# In[34]:


conn.close ()


# In[ ]:




