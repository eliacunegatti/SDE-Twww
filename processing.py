
from datetime import datetime
from numpy.core.fromnumeric import sort
import pandas as pd
from urllib.request import Request, urlopen
import json
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from collections import Counter
import re
pd.options.mode.chained_assignment = None

STOPWORDS = set(stopwords.words('english'))
STOPWORDS_IT = set(stopwords.words('italian'))


def remove_emoji(string):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)

def remove_stopwords(text):
    return " ".join([word for word in str(text).split() if word not in STOPWORDS])

def remove_stopwords_it(text):
    return " ".join([word for word in str(text).split() if word not in STOPWORDS_IT])

def ridefine_date(data):
    new = data.split(".",1)
    boh= new[0]
    ok = boh.replace(r'T', ' ')
    datetime_object = datetime.strptime(ok, '%Y-%m-%d %H:%M:%S') 
    return datetime_object

def find_date(data):
    df_find_date = pd.DataFrame()
    df_find_date = data
    df_find_date['startedAt'] = pd.to_datetime(df_find_date['startedAt'], format='%Y-%m-%d %H:%M:%S')
    df_find_date['startedAt']=pd.to_datetime(df_find_date['startedAt'], format='%H')
    df_find_date['startedAt'] = df_find_date['startedAt'].dt.strftime('%H:00:00')
    df_find_date = df_find_date.groupby(['startedAt'], sort=True).size().reset_index(name='counts')    
    #df_find_date = df_find_date.sort_values(["c"], ascending=True)    

    return df_find_date

def find_game(data):
    df_game = pd.DataFrame()
    df_game = data
    
    df_game = df_game.groupby(['gameName']).size().reset_index(name='counts')    
 
    df_game = df_game.sort_values(["counts"], ascending=False)    

    return df_game 

def find_all_details(l,id):
    l_id = l
    id_twitch = id

    i = 0
    dflist = []
    while (i < len(l)):
        request=Request("https://alpha.mangolytica.tk/streamers/"+str(id_twitch)+"/streams/"+str(l_id[i])+"?key=%3Csecret_1%3E")
        response = urlopen(request)
        elevations = response.read()
        data = json.loads(elevations)
        print(data['streamId'])
        data_stream = pd.json_normalize(data['tunits'])
        dflist.append(data_stream) 
        del data_stream
        i = i+1

    dfx = pd.DataFrame()

    dfx = pd.concat(dflist, axis=0)  
    return dfx

def avarage_viewers_for_hour(data):
    df_hout = pd.DataFrame()
    df_hout = data
    df_hout['hour'] = pd.to_datetime(df_hout['createdAt'], format='%Y-%m-%d %H:%M:%S')
    df_hout['hour']= pd.to_datetime(df_hout['hour'], format='%H')
    df_hout['hour'] = df_hout['hour'].dt.strftime('%H:00:00')
    df_hout = df_hout[df_hout['viewers'] != 0]
    dfx = df_hout.groupby(['hour'])['viewers'].mean().apply(int).reset_index(name='viewers') 
    dfx = dfx.sort_values(["hour"], ascending=True) 
    return dfx

def avarage_viewers_for_date(data):    
    df_date = pd.DataFrame()
    df_date = data 
    df_date['date'] = pd.to_datetime(df_date['createdAt'], format='%Y-%m-%d %H:%M:%S')
    df_date['date']= pd.to_datetime(df_date['date'], format='%Y-%m-%d')
    df_date['date'] = df_date['date'].dt.strftime('%Y-%m-%d')
    dfx = df_date.groupby(['date'])['viewers'].mean().apply(int).reset_index(name='viewers') 
    dfx = dfx.sort_values(["date"], ascending=True) 
    return dfx  

def hour_sleep(data):
    df_sleep = pd.DataFrame()
    df_sleep = data    
    df_sleep['hour'] = pd.to_datetime(df_sleep['hour'], format='%H:%M:%S')
    df_sleep['hour']= pd.to_datetime(df_sleep['hour'], format='%H:%M:%S')
    df_sleep['hour'] = df_sleep['hour'].dt.strftime('%H') 

    del df_sleep['viewers']
    col_one_list = df_sleep['hour'].tolist()
    a = [ int(x) for x in col_one_list]   
    b = []
    for i in range(0,24):
        b.append(i)
     
    d = list(set(b) - set(a))
    start = datetime.strptime(str(d[0]), '%H')
    end = datetime.strptime(str(d[len(d)-1]), '%H')
    start = str(start)
    end = str(end)
    start = start.replace('1900-01-01 ','')
    end = end.replace('1900-01-01 ','')
    r = start + "/" + end
    return r


def get_frequent_words(data):
    df = pd.DataFrame()
    df = data
    df['msg'] = df['msg'].apply(str)

    #remove emoji
    df['msg'] = df['msg'].apply(remove_emoji)

    #remove links, urls, twitter.com and html
    df['msg'] = df['msg'].apply(lambda x: re.split(r'https:\/\/.*', str(x))[0])
    df['msg'] = df['msg'].apply(lambda x: re.split(r'http:\/\/.*', str(x))[0])
    df['msg'] = df['msg'].apply(lambda x: re.split(r'www:\/\/.*', str(x))[0])
    df['msg'] = df['msg'].apply(lambda x: re.split(r'html:\/\/.*', str(x))[0])

    #remove everthing but characther
    df['msg'] = df['msg'].str.replace('[^a-zA-Z]', ' ')

    #remove words with only one characther and blank space
    df['msg']= df['msg'].str.replace(r'\b\w\b','').str.replace(r'\s+', ' ')

    #everthing on lowercase
    df['msg']  = df['msg'].str.lower()

    #remove stopwords
    df['msg'] = df['msg'].apply(remove_stopwords)
    df['msg'] = df['msg'].apply(remove_stopwords_it)
    cnt = Counter()
    for text in df['msg'].values:
        for word in text.split():
            cnt[word] += 1
    cnt.most_common(10) 
    freq = set([w for (w, wc) in cnt.most_common(10)])
    return freq    

def get_hour_game(data, list_game):
    df = pd.DataFrame()
    df = data
    l_game = list_game

    print('l_game**************************')
    print(l_game)
    print('**********************')

    dflist = []
    for item in range(0,len(l_game)):
        appo = df.index[df['gameName'] == str(l_game[item])].tolist()
        dflist.append(appo)

    dflist2 = []
    for item in dflist:
        print('++++++++++++++++++\nChecking list')
        print(item)
        print('---------------------')
        a = int(item[0])
        b = int(item[len(item)-1])
        print('FIRST: ', a)
        print('LAST:  ', b)
        if (len(item) == (b-a+1)):
            print('the list is consecutive')
            dflist2.append(item)
        else:
            print('the list is not consecutive')
            k = 0
            check = False
            for i in range(0,len(item)):
                print('I: ', item[i])
                if(check==True):
                    k=i
                    check=False
                if(i != len(item)-1):
                    if (int(item[i])+1 != int(item[i+1])):
                        new_item = list(item[n] for n in range(k,i + 1))
                        dflist2.append(new_item)
                        check=True
                else :
                    new_item = list(item[n] for n in range(k,i + 1))
                    dflist2.append(new_item)
                    check=True  

    print('dflist2####################################')
    print(dflist2)
    print('####################################')

    game_l = []
    start_l = []
    end_l = []
    for item in dflist2:
        a = item[0]
        b = item[len(item)-1]
        game_name = df['gameName'].iloc[a]
        start = df['createdAt'].iloc[a]
        end = df['createdAt'].iloc[b]
        game_l.append(game_name)
        start_l.append(start)
        end_l.append(end)

    res = pd.DataFrame(list(zip(game_l, start_l, end_l)), columns =['gameName', 'start','end']) 
    res['start'] = pd.to_datetime(res['start'], format='%Y-%m-%d %H:%M:%S')
    res['start'] = res['start'].apply(str)
    res['end'] = pd.to_datetime(res['end'], format='%Y-%m-%d %H:%M:%S')
    res['end'] = res['end'].apply(str)    
    res = res.sort_values(["start"], ascending=True)

    print('res§§§§§§§§§§§§§§§§§§§§§§§§§§§§§')
    print(res)
    print('§§§§§§§§§§§§§§§§§§§§§§§§§§§§§')

    return res

def get_game_av_viewers(data):
    df = pd.DataFrame()
    df = data    
    df = df.groupby(['gameName'])['viewers'].mean().apply(int).reset_index(name='viewers')   
    df = df.sort_values(["viewers"], ascending=False) 
    return df

def avarage_sub_for_hour(data):
    df_hout = pd.DataFrame()
    df_hout = data
    df_hout['hour'] = pd.to_datetime(df_hout['createdAt'], format='%Y-%m-%d %H:%M:%S')
    df_hout['hour']= pd.to_datetime(df_hout['hour'], format='%H')
    df_hout['hour'] = df_hout['hour'].dt.strftime('%H:00:00')
    dfx = df_hout.groupby(['hour'], sort=False).size().reset_index(name='sub')  
    #dfx = dfx.sort_values(["hour"], ascending=True) 
    return dfx

def avarage_viewers_for_hour_stream(data):
    df_hout = pd.DataFrame()
    df_hout = data
    df_hout['hour'] = pd.to_datetime(df_hout['createdAt'], format='%Y-%m-%d %H:%M:%S')
    df_hout['hour']= pd.to_datetime(df_hout['hour'], format='%H')
    df_hout['hour'] = df_hout['hour'].dt.strftime('%H:00:00')
    df_hout = df_hout[df_hout['viewers'] != 0]
    dfx = df_hout.groupby(['hour'], sort=False)['viewers'].mean().apply(int).reset_index(name='viewers') 
    return dfx

def like_tweet(data):
    dfx = pd.DataFrame()
    dfx = data
    dfx['date'] = pd.to_datetime(dfx['date'], format='%Y-%m-%d %H:%M:%S')
    dfx['date']= pd.to_datetime(dfx['date'], format='%Y-%m-%d')
    dfx['date'] = dfx['date'].dt.strftime('%Y-%m-%d')
    dfx = dfx.groupby(['date'])['like'].mean().apply(int).reset_index(name='like') 
    dfx = dfx.sort_values(["date"], ascending=True)     
    return dfx

def retweet(data):
    dfx = pd.DataFrame()
    dfx = data
    dfx['date'] = pd.to_datetime(dfx['date'], format='%Y-%m-%d %H:%M:%S')
    dfx['date']= pd.to_datetime(dfx['date'], format='%Y-%m-%d')
    dfx['date'] = dfx['date'].dt.strftime('%Y-%m-%d')
    dfx = dfx.groupby(['date'])['retweet_count'].mean().apply(int).reset_index(name='retweet') 
    return dfx

def find_words(data):
    dfx = pd.DataFrame()
    dfx = data    
    dfx['date'] = pd.to_datetime(dfx['date'], format='%Y-%m-%d %H:%M:%S')
    dfx['date']= pd.to_datetime(dfx['date'], format='%Y-%m-%d')
    dfx['date'] = dfx['date'].dt.strftime('%Y-%m-%d')    
    dfx['text'] = dfx['text'].str.lower()
    s = ['twitch','stream','live']
    dfx = dfx[dfx['text'].str.contains('|'.join(s))]
    dfx = dfx.sort_values(["date"], ascending=True)     
    dfx = dfx['date']
    return dfx