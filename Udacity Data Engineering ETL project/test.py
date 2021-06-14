#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('load_ext', 'sql')


# In[2]:


pip install postgresql


# In[2]:


pip install ipython-sql


# In[5]:


get_ipython().run_line_magic('sql', 'postgresql://postgres:****@localhost:5432/Music_DB')


# In[6]:


get_ipython().run_line_magic('sql', 'SELECT * FROM songplay LIMIT 5;')


# In[7]:


get_ipython().run_line_magic('sql', 'SELECT * FROM user_table LIMIT 5;')


# In[8]:


get_ipython().run_line_magic('sql', 'SELECT * FROM song LIMIT 5;')


# In[9]:


get_ipython().run_line_magic('sql', 'SELECT * FROM artist LIMIT 5;')


# In[10]:


get_ipython().run_line_magic('sql', 'SELECT * FROM time LIMIT 5;')



# In[ ]:




