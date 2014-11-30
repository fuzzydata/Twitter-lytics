__author__ = 'prog-RAM-zing'

#!/usr/bin/env python

'''
A General Twitter Streamer script using Python's Tweepy module (pip install tweepy)
'''

import tweepy,threading,datetime,unidecode,time,sys
import re,subprocess,shlex
import db_class,sqlite3
from Queue import Queue

#Enter credentials from Twitter App
consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''

auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_secret)

api = tweepy.API(auth)


#Creating a custom STwitter Stream Listener
class customStreamListener(tweepy.StreamListener):

    def __init__(self,thr,db_name,store):
        super(customStreamListener,self).__init__()
        self.store = store
        self.thr = thr
        self.tweet_collection = []
        self.tweet_to_store = []
        self.q = Queue(maxsize=1)
        self.t1 = threading.Thread(target=self.tweet_check)	#Concurrency via threading not to mis out on tweets from listener. 
        self.t1.daemon = True
        self.t1.start()
        self.db_name = db_name

    def on_disconnect(self):
        self.tweet_to_store = (self.tweet_collection[:self.thr])
        self.q.put('go')

        print time.strftime('%Y-%m-%d @ %H:%M:%S'),'\t Stream disconected'
        return True

#On receiving the tweet object returned by stream
    def on_status(self, tweet):
        self.tweet_collection.append(tweet)
        if  self.tweet_collection.__len__() > self.thr:
            self.tweet_to_store = (self.tweet_collection[:self.thr])
            self.tweet_collection = self.tweet_collection[self.thr+1:]
            self.q.put('go')

    def on_error(self, status_code):
        print 'Error: ' + repr(status_code)
        return False

#Insert into DB
    def db_store(self):
        conn = sqlite3.connect('path/to/Database/'+self.db_name +'.db')
        Mgr = db_class.DBManager(conn)
        Mgr.insert_record(self.tweet_to_store,'tweet_table',False)
        conn.close()

    def tweet_check(self):          ###########start this on thread!!
        while True:
                self.q.get()
                for item in self.tweet_to_store:

                    try:
                        if  'http://' in item.text:
                            self.tweet_to_store.remove(item)
                        elif item.entities['urls']:
                            self.tweet_to_store.remove(item)
                        elif item.entities['media']:
                            self.tweet_to_store.remove(item)
                    except KeyError:
                            continue

                if self.store:
                    self.db_store()


#Stanfords OpenNLP sentiment Tool running on Java. Calling it as a subprocess. Optional
def SentimentParse(self,strings,res):
        str = 'java -cp "*" -mx1g edu.stanford.nlp.sentiment.SentimentPipeline -stdin'
        cw_dir = 'path/to/stanford-corenlp-full-2014-01-04'
        result = []
        p = subprocess.Popen(shlex.split(str),stdout = subprocess.PIPE,stdin = subprocess.PIPE,cwd=cw_dir,bufsize = 1)
        for string in strings:
            try:
                p.stdin.write(string+'\n')
                op = p.stdout.readline()
                if 'Positive' in op or 'positive' in op:
                    result.append('Positive')
                elif 'Negative' in op or 'negative' in op:
                    result.append('negative')
                elif 'Neutral' in op:
                    result.append('Neutral')
                else: print colored(op,'green')
            except UnicodeEncodeError:
                print string
                exit(1)



def analyze_record(res):
    tweets_without_meta = []
    for ind,item in enumerate(res):

        item = unidecode.unidecode(item)
        item = item.strip()
        item = item.replace('\n','.')
        if 'RT ' in item:				#Replace "RT/@RT/#hashtags in the tweet text"
            item = item.replace('RT ','')
        if '@' in item:
            item = re.sub(r'[@]\S*','',item)

        if '#' in item:
            item = re.sub(r'[#]\S*','',item)
        item = item.strip()

        tweets_without_meta.append(item)
    #print colored('Tweets Analyzed....','blue')
    return tweets_without_meta

if __name__ == '__main__':
    db_name = str(sys.argv[1])

#Keyword list for the stream to listen to
    keywords = ['WC14','#worldcup','#Brazil2014','FIFA WC','Neymar','Ronaldo','Messi','CR7','LM10','Rooney','Lampard','Gerrard','Iniesta','Xavi','Eden Hazard','Suarez','Benzema','Paul Pogba','di Maria','Miroslav Klose','Sergio Aguero','Diego Costa','Andrea Pirlo','van Persie','Arjen Robben','Mesut Ozil','Balotelli'
                '#Brazil2014','Football AND England','Football AND Brazil','Football AND Argentina','Football AND Portugal','Football AND France',\
            'Football AND Germany','Football AND Belgium','Football AND Spain','Football AND Italy','Football AND Netherlands','Azzurri','Azzuri','Furia Roja','Les Blues',\
            'WC AND England','WC AND Brazil','WC AND Argentina','WC AND Portugal','WC AND France','WC AND Germany','WC AND Belgium','WC AND Spain','WC AND Italy','WC AND Netherlands'\
            'Roy Hodgson','Felipe Scolari','del Bosque','Didier Deschamps','Joachim Loew','Paulo Bento','Cesare Prandelli','#ESP','#FRA','#GER','#BRA','#ENG','#NED','#URU','#BEL','#ITA','#ARG','#POR','#MEX' ]

#Optional latLng list. Either keyoword or latLng.
    latlng_box = {'bangalore':[77.350788,12.742450,77.946796,13.242910],'delhi':[76.850191,28.427393,77.476412,28.875701],'chennai':[80.016067,12.832517, 80.388915,13.273992],'mumbai': [72.758981,18.877717,73.008234,19.292365],'kolkata':[88.108504,22.335938, 88.591902,23.017647],'hyderabad':[78.158107,17.217001,78.657985,17.591780]}
    l = customStreamListener(500,db_name,store=True)
    streamer = tweepy.Stream(auth,listener=l)
    try:
        streamer.filter(languages=['en'],track=keywords,async=True)
        print time.strftime('%Y-%lm-%d @ %H:%M:%S'), '\t Stream Connected'

#Ubuntu Notification. Useful when running as cronjob
        subprocess.Popen(['notify-send','Tweet Scrapper is now running'])

#Stream to run for how long. Set up as cron job and run periodically
        time.sleep(15*60)
        streamer.disconnect()
        time.sleep(5)
        subprocess.Popen(['notify-send','Tweet Scrapper has finished'])
        sys.exit(0)
    except KeyboardInterrupt:
        streamer.disconnect()
        subprocess.Popen(['notify-send','Tweet Scrapper has finished'])
        sys.exit(0)




