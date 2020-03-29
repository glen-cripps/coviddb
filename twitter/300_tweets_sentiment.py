#!/usr/bin/env python
# coding: utf-8

# In[1]:


from textblob import TextBlob

text = '''
The titular threat of The Blob has always struck me as the ultimate movie
monster: an insatiably hungry, amoeba-like mass able to penetrate
virtually any safeguard, capable of--as a doomed doctor chillingly
describes it--"assimilating flesh on contact.
Snide comparisons to gelatin be damned, it's a concept with the most
devastating of potential consequences, not unlike the grey goo scenario
proposed by technological theorists fearful of
artificial intelligence run rampant.
'''

blob = TextBlob(text)
blob.tags           # [('The', 'DT'), ('titular', 'JJ'),
                    #  ('threat', 'NN'), ('of', 'IN'), ...]

blob.noun_phrases   # WordList(['titular threat', 'blob',
                    #            'ultimate movie monster',
                    #            'amoeba-like mass', ...])

for sentence in blob.sentences:
    print(sentence.sentiment.polarity)

    


# In[2]:


import time
import arrow
today_dt = arrow.now().format('YYYYMMDD')


# In[50]:


import pandas as pd
tweets = pd.read_parquet("tweets_all."+today_dt+".parquet")


# In[6]:


tweets['text']


# In[52]:


def sentiment_calc(text):
    try:
        return TextBlob(text).sentiment
    except:
        return (0,0)

tweets['sentiment'] = tweets['text'].apply(lambda tweet: sentiment_calc(tweet))


# In[67]:


tweets[['sentiment_polarity', 'sentiment_subjectivity']] = pd.DataFrame(tweets['sentiment'].tolist(), index=tweets.index) 

tweets


# In[68]:


wtf = tweets.drop(['sentiment'], axis=1)
wtf.to_parquet("tweets_sentiment." + today_dt + ".parquet", compression='GZIP')



# In[69]:


wtf

