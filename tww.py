from flask import Flask, request, Response, jsonify
from dotenv import load_dotenv
import os
from nltk import parse
import pandas as pd
pd.options.mode.chained_assignment = None
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin import auth
from urllib.request import Request, urlopen
from urllib import parse
import json
from datetime import datetime
from processing import find_words, ridefine_date,find_date,find_game,find_all_details,avarage_viewers_for_date,avarage_sub_for_hour,hour_sleep,get_frequent_words,get_hour_game,get_game_av_viewers,avarage_viewers_for_hour,avarage_viewers_for_hour_stream,like_tweet,retweet
import requests

# Use a service account


cred = credentials.Certificate('serviceAccount.json')
firebase_admin.initialize_app(cred)


db = firestore.client()

load_dotenv()
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DEBUG = os.getenv("DEBUG")


def get_id(username):
    id_twitter = username
    docs = db.collection(u'account').where(u'id_twitter', u'==', id_twitter).stream()      
    l = {}

    for doc in docs:
        l = doc.to_dict()
    return l

def check_fdb(token, id, tt):
    uid = str(token)
    id_twitter = str(id)
    id_twitch = str(tt)
    doc_ref = db.collection(u'user').where(u'uid',u'==',uid).stream()
    check = 0
    l = {}

    for item in doc_ref:
        check = check +1
        l = item.to_dict()
        print("esiste gia")
    if(check==0):
        data = {
            u'uid' : uid,
            u'favorites_twitter': [id_twitter],
            u'favorites_twitch': [id_twitch],

        }
        db.collection(u'user').document(uid).set(data)
        print("agguiutn")
    else:
        print(l)
        ll = l['favorites_twitter']
        ll1 = l['favorites_twitch']
        if id_twitter in ll:
            ll.remove(id_twitter)
            ll1.remove(id_twitch)
        else :
            ll.append(id_twitter)
            ll1.append(id_twitch)
        new = db.collection(u'user').document(uid)

        new.update({u'favorites_twitter': ll, u'favorites_twitch':ll1})       

        print(ll)

def get_favorites(token):
    uid = str(token)
    doc_ref = db.collection(u'user').where(u'uid',u'==',uid).stream()
    l = {}
    check = 0

    for item in doc_ref:
        check = check +1
        l = item.to_dict()
    if(check==0):
        ll = []
    else:
        ll = l['favorites_twitter']       

    return ll
def get_favorites_tt(token):
    uid = str(token)
    doc_ref = db.collection(u'user').where(u'uid',u'==',uid).stream()
    check = 0
    l = {}
    
    for item in doc_ref:
        check = check +1
        l = item.to_dict()
    if(check==0):
        ll = []
    else:
        ll = l['favorites_twitch']       

    return ll

def get_trending():
    doc_ref = db.collection(u'account').order_by(u'counts', direction=firestore.Query.DESCENDING).where(u'counts', u'>=', 10).limit(5).stream()
    l = {}
    list = []

    for doc in doc_ref:
        l = doc.to_dict()
        list.append(l)

    return list

app = Flask(__name__)
@app.route('/startmonitoring')
def check_db():
    id_twitch = request.args.get('id_twitch')
    id_twitter = request.args.get('id_twitter')
    twitch_name = request.args.get('twitch_name')
    check = False
    
    try:
        docs_1 = db.collection(u'account').where(u'id_twitch', u'==', id_twitch).stream()    
        docs_2 = db.collection(u'account').where(u'id_twitter', u'==', id_twitter).stream()    
    except:
        pass
    check1 = 2
    for doc1 in docs_1:
        for doc2 in docs_2:
            if (doc1.id == doc2.id):    
                check = True 
                docs = db.collection(u'account').where(u'id_twitter', u'==', id_twitter).stream()    
                l = {}

                for doc in docs:
                    l = doc.to_dict()
                count = int(l['counts']) +1
                id = l['id']
                new = db.collection(u'account').document(id)

                new.update({u'counts': count})              
                break
    
    if(check == False):
        docs = db.collection(u'account').where(u'id_twitch', u'==', id_twitch).stream()  

        for doc in docs:
            if(doc.id):
                check1 = check1 -1
        docs = db.collection(u'account').where(u'id_twitter', u'==', id_twitter).stream()  

        for doc in docs:
            if(doc.id):
                check1 = check1 -1
        if(check1 == 2):   
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")    
            doc_ref = db.collection(u'account').document()
            doc_ref.set({
                u'id_twitch': str(id_twitch),
                u'id_twitter': str(id_twitter),
                u'twitch_name': str(twitch_name),
                u'id': doc_ref.id,
                u'date' : dt_string,
                u'monitored' : False,
                u'counts' : str(1)
            })
            r = "New twitch and twitter account added to be followed" 
            res = {"SUCCESS":r}
            data = parse.urlencode({"streamerId":str(id_twitch)}).encode()
            req = Request("https://alpha.mangolytica.tk/streamers?key=%3Csecret_1%3E",data=data)
            re = urlopen(req)
        else :
            r = "I'm sorry but either the twitch or the twitter account is already monitored with someone else" 
            res = {"ERROR":r}

    if (check==True):
        r = "We are already monitoring this profile"
        res = {"SUCCESS":r}

    r = json.dumps(res)
    resp = Response(response=r, status=200, mimetype="application/json")
    return resp

@app.route('/<username_screen_name>/insights')
def method_name(username_screen_name):
    username = username_screen_name
    l = get_id(username)
    id_twitch = l['id_twitch']
    id_twitter = l['id_twitter']
    # monitored = l['monitored']
    # date_start = l['date']
    # count = l['counts']
    
    request_5=Request("https://alpha.mangolytica.tk/streamers/"+str(id_twitch)+"?key=%3Csecret_1%3E")
    response_5 = urlopen(request_5)
    elevations_5 = response_5.read()
    data_5 = json.loads(elevations_5)
    df = pd.json_normalize(data_5['streams'])
    #started hour
    df = find_date(df)
    date_start_stream = df.to_dict(orient="records")

    df = pd.json_normalize(data_5['streams'])
    l = []
    l = df['streamId']
    appo = []
    df1 = find_all_details(l,id_twitch)
    appo =df1['createdAt']
    df2= pd.DataFrame()
    df3 = pd.DataFrame()
    df4 = pd.DataFrame()

    df2 = df1
    df3 = df1
    df4 = df1

    df3 = find_game(df3)
    game = df3.to_dict(orient="records")

    df2 = avarage_viewers_for_hour(df2)      
    average_hour =  df2.to_dict(orient="records") 
    score1 = int(df2['viewers'].mean())

    sleep = hour_sleep(df2)

    df4['createdAt'] = appo
    df4 = avarage_viewers_for_date(df4)
    average_date = df4.to_dict(orient="records") 
    score2 = int(df4['viewers'].mean())

    
    request=Request("https://gamma.mangolytica.tk/"+str(id_twitter)+"/like")
    response = urlopen(request)
    elevations = response.read()
    data = json.loads(elevations)
    df = pd.json_normalize(data)
    score3 = int(df['retweet_count'].mean())
    score4 = int(df['like'].mean())    
    d_words = pd.DataFrame()
    d_words = df

    
    
    df_like = like_tweet(df)
    df_retweet = retweet(df)

    df = pd.merge(df_like,df_retweet,how="inner",on=['date'])
    
    like = df.to_dict(orient="records")
    
    

    d_words = find_words(d_words)
    d_words = d_words.tolist()

    request_1=Request("https://gamma.mangolytica.tk/"+str(id_twitter)+"/countsbyhour")
    response_1 = urlopen(request_1)
    elevations_1 = response_1.read()
    data_1 = json.loads(elevations_1)
    df1 = pd.json_normalize(data_1)
    counts_hour= df1.to_dict(orient="records")

    request_2=Request("https://gamma.mangolytica.tk/"+str(id_twitter)+"/countsbydate")
    response_2 = urlopen(request_2)
    elevations_2 = response_2.read()
    data_2 = json.loads(elevations_2)
    df2 = pd.json_normalize(data_2)      
    counts_date = df2.to_dict(orient="records")

   
    request_4=Request("https://gamma.mangolytica.tk/"+str(id_twitter)+"/fwords")
    response_4 = urlopen(request_4)
    elevations_4 = response_4.read()
    data_4 = json.loads(elevations_4)
    score = int((score1 + score2 + score3 + score4) / 4)
    res = {"start_stream": date_start_stream,"sleep":sleep, "favourite_games": game,"hour": average_hour,"date": average_date,"like" : like,"count_hour":counts_hour,"count_date":counts_date,"frequent_words_tweet":data_4,"score":score,"words_twitch":d_words}
    r = json.dumps(res)
    resp = Response(response=r, status=200, mimetype="application/json")
    return resp    



@app.route('/<username_screen_name>/<stream>/details')
def info_stream(username_screen_name, stream):
    id_stream = stream
    username = username_screen_name
    l = get_id(username)
    id_twitch = l['id_twitch']
    id_twitter = l['id_twitter']

    #called to find data of the streams
    request=Request("https://alpha.mangolytica.tk/streamers/"+str(id_twitch)+"/streams/"+str(id_stream)+"?key=%3Csecret_1%3E")
    response = urlopen(request)
    elevations = response.read()
    data = json.loads(elevations)

    #take information of tunits
    data_stream = pd.json_normalize(data['tunits'])
    #print(data_stream)

    #define start and end
    start = data['startedAt']
    start = ridefine_date(start)
    start = str(start)
    appo_data = data_stream.iloc[len(data_stream)-1]
    end = appo_data['createdAt']
    end = ridefine_date(end)
    end = str(end)

    #average viewers for hour
  
    #print(data_stream)
    list_game = data_stream['gameName']
    s = []
    for i in list_game:
        if i not in s:
            s.append(i)    
    list_game = s
    del s
    #print(list_game)

    list_title = data_stream['title']
    s = []
    for i in list_title:
        if i not in s:
            s.append(i)    
    list_title = s
    del s
    #print(list_title)

    df1 = pd.DataFrame()
    df1['title'] = data_stream['title']
    df1['gameName'] = data_stream['gameName']

    df1 = df1.drop_duplicates(['title','gameName'], keep= 'last')
    game_end_title= df1.to_dict(orient="records")


    del df1
    df1 = pd.DataFrame() 
    df1= data_stream 
    df1 = get_game_av_viewers(df1)
    av_game = df1.to_dict(orient="records")
    
    del df1
    df1 = pd.DataFrame() 
    df1= data_stream 
    print('getting game timestamps @@@@@@@@@@@@@@@@@@@@@@@@@@@')
    print(df1)
    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    df1 = get_hour_game(df1,list_game)
    dur = df1.to_dict(orient="records")

    del df1
    df1 = pd.DataFrame() 
    df1= data_stream 
    df1 = df1[df1['viewers'] != 0]

    mean = int(df1['viewers'].mean())

    request_1=Request("https://alpha.mangolytica.tk/streamers/"+str(id_twitch)+"/streams/"+str(id_stream)+"/events?key=%3Csecret_1%3E")
    response_1 = urlopen(request_1)
    elevations_1 = response_1.read()
    data_1 = json.loads(elevations_1)
    
    data_sub = pd.json_normalize(data_1['subscriptions'])
    fre = []
    fre = list(get_frequent_words(data_sub))    
    total_sub = int(len(data_sub))
    mean_sub = int(data_sub['months'].mean())

    del df1
    df1 = pd.DataFrame() 
    df1= data_sub
    df1 = avarage_sub_for_hour(df1)  
    sub_hour = df1.to_dict(orient="records")
    

    #print(id_twitter)
    score_like = 0
    score_r =0
    response_before = requests.get("https://gamma.mangolytica.tk/"+str(id_twitter)+"/tweetstream/before/date",params={'from': start})
    json_response_before = response_before.json()
    t_b = pd.json_normalize(json_response_before)
    t_b = t_b[0:5]
    #print(t_b)
    try:
        score3_r = int(t_b['retweet_count'].mean())
        score3_l = int(t_b['like'].mean())
        t_b = t_b.to_dict(orient='records')
    except:
        pass
    response_between = requests.get("https://gamma.mangolytica.tk/"+str(id_twitter)+"/tweetstream/between/date",params={'from': start,'to':end})
    json_response_between = response_between.json()    
    t_bt = pd.json_normalize(json_response_between)
    #print(t_bt)
    try:
        scorebt_r = int(t_bt['retweet_count'].mean())
        scorebt_l = int(t_bt['like'].mean())  
    except:
        pass
    t_bt = t_bt.to_dict(orient='records')

    response_after = requests.get("https://gamma.mangolytica.tk/"+str(id_twitter)+"/tweetstream/after/date",params={'from': end})
    json_response_after = response_after.json()    
    t_a = pd.json_normalize(json_response_after)
    t_a = t_a[0:5]    
    #print(t_a)
    try:
        score4_r = int(t_a['retweet_count'].mean())
        score4_l = int(t_a['like'].mean())   
    except:
        pass

    t_a = t_a.to_dict(orient='records')
    try:
        score_like = int((score3_l+score4_l+scorebt_l)/3)
        score_r = int((score3_r+score4_r+scorebt_r)/3)
    except:
        pass
    score_like = score_like + score_r
    score = int(((int(mean) +(int(total_sub) * int(mean_sub)) + score_like))/3)


    res = {"stream":data,"stream_events":data_1,"game_with_title": game_end_title,"average_viewers_game":av_game,"dur_game":dur,"average_viewers" : mean, "frequent_words_sub": fre, "total_sub":total_sub,"mean_month_sub":mean_sub,"sub_hour":sub_hour, "tweet_before":t_b,"tweet_between":t_bt,"tweet_after":t_a,"score":score}
    r = json.dumps(res)
    resp = Response(response=r, status=200, mimetype="application/json")
    return resp

@app.route('/search')
def search_user():
    username = request.args.get('username')
    request_1=Request("https://gamma.mangolytica.tk/searchuser?username="+parse.quote(username))
    response_1 = urlopen(request_1)
    elevations_1 = response_1.read()
    data_1 = json.loads(elevations_1)
 
    request_2=Request("https://alpha.mangolytica.tk/search?key=%3Csecret_1%3E&query="+parse.quote(username))
    response_2 = urlopen(request_2)
    elevations_2 = response_2.read()   
    data_2 = json.loads(elevations_2)
    docs = db.collection(u'account').where(u'id_twitter', u'==', username).stream()    
    l = {}
    list = []

    for doc in docs:
        l = doc.to_dict()
        list.append(l)
    
    docs = db.collection(u'account').where(u'twitch_name', u'==', username).stream()    

    for doc in docs:
        l = doc.to_dict()
        x = False
        for streamer in list:
            if(streamer['id'] == l['id']):
                x = True

        if(x == False):
            list.append(l)



    res = {"monitored":list,"twitch_results":data_2,"twitter_results":data_1}
    
    r = json.dumps(res)
    resp = Response(response=r, status=200, mimetype="application/json")
    return resp


@app.route('/<username_screen_name>/info')
def info_user(username_screen_name):
    username = username_screen_name
    l = get_id(username)
    id_twitch = l['id_twitch']
    id_twitter = l['id_twitter']
    docs = db.collection(u'account').where(u'id_twitter', u'==', id_twitter).stream()    
    l = {}

    for doc in docs:
        l = doc.to_dict()
    count = int(l['counts']) +1
    id = l['id']
    new = db.collection(u'account').document(id)

    # Set the capital field
    new.update({u'counts': count})      
    request_1=Request("https://gamma.mangolytica.tk/"+str(id_twitter)+"/getweets")
    response_1 = urlopen(request_1)
    elevations_1 = response_1.read()
    data_1 = json.loads(elevations_1)

    request_2=Request("https://gamma.mangolytica.tk/"+str(id_twitter)+"/infouser")
    response_2 = urlopen(request_2)
    elevations_2 = response_2.read()
    data_2 = json.loads(elevations_2)

    request_3=Request("https://alpha.mangolytica.tk/streamers/"+str(id_twitch)+"?key=%3Csecret_1%3E")
    response_3 = urlopen(request_3)
    elevations_3 = response_3.read()
    data_3 = json.loads(elevations_3)

    
    res = {"twitch_info":data_3,"twitter_info":data_2,"tweets":data_1}
    r = json.dumps(res)
    resp = Response(response=r, status=200, mimetype="application/json")
    return resp


@app.route('/favorites',methods=['GET','POST'])
def get_token():

    if request.method == 'POST':
        content = request.json
        token = content['token']
        id_twitter = content['id_twitter']
        id_twitch = content['id_twitch']

        decoded_token = auth.verify_id_token(token)
        uid = decoded_token['uid']
        check_fdb(uid,id_twitter,id_twitch)
        
        r = "ok"
    else:
        token = request.args.get("token")
        decoded_token = auth.verify_id_token(token)
        uid = decoded_token['uid']
        r0 = get_favorites(uid)
        r1 = get_favorites_tt(uid) 
        r = {"twitch_favorites":r1,"twitter_favorites":r0}

    return jsonify(r)

@app.route('/trending')
def get_fav():
    r0 = get_trending()
    print('=======\n', r0)
    r = {"trending":r0}

    return jsonify(r)


if __name__ == "__main__":
    app.run(HOST,PORT,DEBUG)
