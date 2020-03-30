#!/usr/bin/env python
# coding: utf-8

# In[5]:


# %load 100_get_tweets.py
#!/usr/bin/env python2
"""
Created on Thu Nov 22 11:30:51 2018

@author: hduser
"""
import time
import arrow
today_dt = arrow.now().format('YYYYMMDD')


# 1 - The purpose of this assignment is to compare the popularity between Python and
# JavaScript. We will use 2 days’ worth of Twitter data for this.

# 2. The first step is to get access to Twitters API keys, and connect to this streaming API
# and download the data. Your instructor will show you how to do this.


import json
import pandas as pd
import matplotlib.pyplot as plt



#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Consumer API keys
# SJLRRqubA7nUjHIZ3KIJRROBZ (API key)
# UO4SYnvwLoy6YIaFtp1YqUUwKJ2u6kAiYplRjhPYbudXuBv6Sl (API secret key)

# Access token & access token secret
# 17236286-hU70T227xL6lKbUnnz0z6uwSvq0eO2bRFRrED8oFH (Access token)
# 6n7V6bXhrAcQsYTOj19Vkoq2wsm5Niuyh3YZc0m456bu4 (Access token secret)
# Read and write (Access level)


#Variables that contains the user credentials to access Twitter API 
access_token = "17236286-hU70T227xL6lKbUnnz0z6uwSvq0eO2bRFRrED8oFH"
access_token_secret = "6n7V6bXhrAcQsYTOj19Vkoq2wsm5Niuyh3YZc0m456bu4"
consumer_key = "SJLRRqubA7nUjHIZ3KIJRROBZ"
consumer_secret = "UO4SYnvwLoy6YIaFtp1YqUUwKJ2u6kAiYplRjhPYbudXuBv6Sl"

    


# In[ ]:





# In[6]:




#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        f = open("tweets." + today_dt + ".json", "a")
        f.write(data)
        f.close()

        return True

    def on_error(self, status):
        print(status)



# In[7]:




#This handles Twitter authetification and the connection to Twitter Streaming API
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'


# In[8]:


import time
import timeout_decorator

@timeout_decorator.timeout(60*1)
def read_tweets():
    stream.filter(track=['covid', 'coronavirus','covid19','corona virus'])
    print("%d seconds have passed" % i)

read_tweets()    


# In[ ]:




