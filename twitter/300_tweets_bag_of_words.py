#!/usr/bin/env python
# coding: utf-8

# In[2]:


import re
import time
import json
import os
import pandas as pd
import arrow
today_dt = arrow.now().format('YYYYMMDD')


# In[3]:


import pandas as pd


# In[4]:


from platform import python_version

print(python_version())


# In[22]:


today_dt = arrow.now().format('YYYYMMDD')


# In[ ]:





# In[ ]:





# In[6]:


tweets = pd.read_parquet("tweets_all." + today_dt + ".parquet")


# In[7]:


tweets.head()


# In[8]:


def strip_links(text):
    link_regex    = re.compile('((http?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)', re.DOTALL)
    links         = re.findall(link_regex, text)
    for link in links:
        text = text.replace(link[0], ', ')    
    return text

def strip_all_entities(text):
    import string
    entity_prefixes = ['@','#']
    for separator in  string.punctuation:
        if separator not in entity_prefixes :
            if separator == "'":
                text = text.replace(separator,'')
            else:
                text = text.replace(separator,' ')
    words = []
    for word in text.split():
        word = word.strip()
        if word:
            if (word[0] not in entity_prefixes) and (word != 'RT'):
                words.append(word)    
    return ' '.join(words)

def strip_numbers(text):
    text = re.sub('[0-9]+', '', text)
    return text

def strip_junk(text):
    text = re.sub(r'\W+', ' ', text)
    return text

from nltk.stem import PorterStemmer
ps = PorterStemmer()
# 7. Use stemming technique to return words backs to its root form (avoid using aggressive
# stemming)
def stem_sentences(sentence):
    tokens = sentence.split()
    stemmed_tokens = [ps.stem(token) for token in tokens]
    return ' '.join(stemmed_tokens)


def preprocess(text_data):
    preprocessed_text=[]
    for text in text_data:
        #regext to remove RT retweets
        text=re.sub('RT'," ",text)
        #regex to remove punctuations,hashtags,@usermentions,URLs and convert to lowercase
        text=re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",text).lower()
        #regex to remove numbers
        text=re.sub("\d+", " ", text)
        preprocessed_text.append(text)
    return preprocessed_text


# In[9]:


tweets['text_uni'] = tweets['text'].astype('unicode')


# In[10]:


#tweets['text_str']= tweets['text_uni'].astype(str)
tweets['text_str'] = tweets['text_uni'].str.encode('ascii', 'ignore').str.decode('ascii')


# In[11]:


def make_lower(text):
    text = text.lower()
    return text

tweets['text_clean'] = tweets['text_str'].apply(strip_links).apply(strip_all_entities)         .apply(strip_numbers).apply(make_lower)         .apply(strip_junk).apply(stem_sentences)


# In[12]:


tweets


# In[13]:


tweets.columns = tweets.columns.str.lower()
tweets['word_list'] = tweets['text_clean'].str.split()


# In[14]:


# Vocabulary time
vocab_dict = {}
for word_list in tweets['word_list']:
    for word in word_list:
        vocab_dict[word] = vocab_dict.get(word, 0) + 1
# K now clean up that list by eliminating the stopwords


# In[15]:


# 8. Remove stopwords by using from nltk.corpus import stopwords,
# stopwords are dumb words like "the" and "a" 
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords


# In[16]:


stopwords=set(stopwords.words('english'))
vocab_clean_dict = {k:v for k,v in vocab_dict.items() if v>1 and k not in stopwords}


# In[17]:


# The bag of words is too dam big and crashes the computer
# So let's only allow words with 10 or more instances 
vocab_toosmall_dict = {k:v for k,v in vocab_clean_dict.items() if v<10}
vocab_trimmed_dict = {k:v for k,v in vocab_clean_dict.items() if v>=10}


# In[18]:


pat = r'\b(?:{})\b'.format('|'.join(vocab_toosmall_dict.keys()))
tweets['text_trim'] = tweets['text_clean'].str.replace(pat, '')


# In[19]:


pd.options.display.max_colwidth = 240

tweets[['text','text_trim']]



# In[20]:


#  10 - Now, use the scikit-learn function CountVectorizer to create bag-of-words features        
import scipy.sparse
from sklearn.feature_extraction.text import CountVectorizer


# In[21]:


vectorizer=CountVectorizer()
vectorizer.fit(vocab_trimmed_dict)

tweets['bag_of_words'] = list(vectorizer.transform(tweets['text_trim']).todense())



# In[26]:


import nltk
from nltk.corpus import stopwords

stop = stopwords.words('english')


def tokenizeit(text_data):
    tokenized_text=[]
    for text in text_data:
        #tokenize the text
        tokenized=nltk.word_tokenize(text)
        #remove stop words
        tokenized_text.append(" ".join(list(x for x in tokenized if x not in stop)))
    return tokenized_text


tweets['text_tokenized']=tokenizeit(tweets['text_trim'])


# In[ ]:





# In[ ]:





# In[57]:


for num_words in [100,200,400,800]: 
    vectorizer = CountVectorizer(max_features = num_words, stop_words='english')
    bag_of_words = vectorizer.fit_transform(tweets['text_tokenized']).toarray()

    sum_of_words=bag_of_words.sum(axis=0)

    df=pd.DataFrame(bag_of_words,columns=vectorizer.get_feature_names())
    
    df.columns = ['bow_' + str(col) for col in df.columns]

    #column names need to be str for parquet (apparently!)
    df.columns = df.columns.map(str)

#    df = tweets.append(df, ignore_index=True)
    
    df = pd.concat([tweets, df],axis=1)
    
    pretty_front = df[['text','text_tokenized']]
    pretty_back = df.drop(['text','text_tokenized'], axis=1)
    pretty_full = pd.concat([pretty_front,pretty_back], axis=1)

    #write as parquet
    pretty_full.drop(columns=['bag_of_words']).to_parquet("tweets_bow_t" + str(num_words) + "." + str(today_dt) + ".parquet")

    print(pretty_full.shape)


# In[52]:




