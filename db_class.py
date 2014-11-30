__author__ = 'prog-RAM-zing'

'''
An Sqlite3 Databse manager for the Tweet Streamer nly.
'''

import sqlite3,subprocess,shlex,re,unidecode,datetime
from time import sleep
from sys import exit
class DBManager(object):

    def __init__(self,conn):
        self.conn = conn
        self.cursor = self.conn.cursor()

    def create_table(self,table_name):

        if self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" %(table_name)).fetchone():
            print 'Table "%s" already exists in database' %table_name
            return False
        else:
            self.conn.execute('''CREATE TABLE IF NOT EXISTS %s \
            (
            Author          TEXT   , \
            Tweet           TEXT        NOT NULL, \
            Tweet_id        INTEGER     UNIQUE NOT NULL, \
            Time            TEXT        NOT NULL,  \
            Favorite_count  INTEGER,            \
            Retweet_count   INTEGER , \
            Hashtags        Text)
            ''' %table_name)
            print 'Table "%s" has been created' %table_name
            return

    def insert_record(self,tweets,table_name,politician):
        if not self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" %(table_name)).fetchone():
            print 'Table "%s" doesnt exist' %table_name
            return False

        success_cnt = 0
        if politician:
            for tweet in tweets:
                #if tweet.retweeted ==True or 'RT @' in tweet.text:
                    #mentions =', '.join([z['screen_name'] for z in tweet.entities['user_mentions']])
                    hashtags = ', '.join([item['text'] for item in tweet.entities['hashtags']])
                    timestamp = (tweet.created_at + datetime.timedelta(hours=5.5)).strftime('%Y-%m-%d : %H:%M:%S')
                    self.cursor.execute('INSERT INTO '+ table_name +''' (Author,Tweet,Tweet_id,Time,Favorite_count,Retweet_count,Hashtags) VALUES (?,?,?,?,?,?,?)''' ,
                    (tweet.user.name,tweet.text,tweet.id,timestamp,tweet.favorite_count,tweet.retweet_count,hashtags));

                    success_cnt += 1
        else:
            for tweet in tweets:
                try:
                    #if tweet.retweeted ==True or 'RT @' in tweet.text:
                        #mentions =','.join([z['screen_name'] for z in tweet.entities['user_mentions']])
                        hashtags = ', '.join([item['text'] for item in tweet.entities['hashtags']])
                        timestamp = (tweet.created_at + datetime.timedelta(hours=5.5)).strftime('%Y-%m-%d : %H:%M:%S')

                        self.cursor.execute('INSERT INTO '+ table_name +''' (Author,Tweet,Tweet_id,Time,Favorite_count,Retweet_count,Hashtags) VALUES (?,?,?,?,?,?,?)''' ,
                        (tweet.user.name,tweet.text,tweet.id,timestamp,tweet.favorite_count,tweet.retweet_count,hashtags));

                        success_cnt += 1

                except sqlite3.IntegrityError:
                    print 'Tweet already exists in database'
                except sqlite3.OperationalError as e:
                    print str(e)
                    #print '''%s,%s,%d,%s,%d,%d)'''%(tweet.user.name,tweet.text,tweet.id,tweet.created_at.strftime('%Y-%m-%d : %H:%M:%S'),tweet.favorite_count,tweet.retweet_count)

        if success_cnt:
            subprocess.Popen(['notify-send','%d records added till ID = %d' %(success_cnt,self.cursor.lastrowid)])
            self.conn.commit()

        return

    def get_tables_list(self):
        table_list = []
        print 'Fetching existing tables from sqlite_master...'
        for table in self.conn.execute("select name from sqlite_master where type = 'table'").fetchall():
            table_list.append(table[0])
        try:
            table_list.remove('sqlite_sequence')
        except:
            return table_list
        return table_list


    def drop_table(self,table_name):
        if  table_name == 'sqlite_sequence':
            print 'Cannot drop table "sqlite_sequence"'
            return False

        if self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" %(table_name)).fetchone():
            self.conn.execute('DROP TABLE %s' %(table_name))
            print 'Table "%s" has been deleted' %(table_name)
            return
        else:
            print 'Table "%s" doesnt exist' %(table_name)
            return False


    def pull_record(self,table_name,count=10000,start_pos=0):

        cursor = self.conn.cursor()

        try:
            lim = self.last_row_id(table_name)
            if start_pos != 'start' and start_pos==0:
                with open('db_rowid_log.txt','r') as fp:
                    start_pos = fp.readline().rstrip()
                    if  start_pos !='':
                        start_pos = int(start_pos)
            elif start_pos=='start':
                    start_pos=0

            if start_pos!=0:
                if start_pos+count > lim and start_pos<lim:
                    print 'The number of tweets specified is geater than total record number. Readjusting to maximum number of records available.'
                    res_cur = cursor.execute('select Tweet from '+ str(table_name)+' where rowid >= ? AND rowid < ?',(start_pos,lim,)).fetchall()
                    raw_str = [string[0] for string in res_cur ]
                    res = self.analyze_record(raw_str)
                    if  res:
                        start_pos +=count
                        with open('/path/to/db/db_rowid_log.txt','w') as fp:
                            fp.write(str(start_pos))
                elif start_pos>=lim:
                    return False


                else:

                    res_cur = cursor.execute('select Tweet from '+ str(table_name)+' where rowid >= ? AND rowid < ?',(start_pos,start_pos+count)).fetchall()
                    raw_str = [string[0] for string in res_cur ]
                    res = self.analyze_record(raw_str)
                    if res:
                        start_pos += count
                        with open('/path/to/db/db_rowid_log.txt','w') as fp:
                            fp.write(str(start_pos))

            else:
                res_cur = cursor.execute('select Tweet from '+ str(table_name) + ' where rowid < ?',(count,)).fetchall()
                raw_str = [string[0] for string in res_cur ]
                res = self.analyze_record(raw_str)
                if res:
                    start_pos = count
                    print start_pos
                    with open('/path/to/db/db_rowid_log.txt','w') as fp:
                        fp.write(str(start_pos))

        except KeyboardInterrupt:
            with open('/path/to/db/db_rowid_log.txt','w') as fp:
                fp.write(str(start_pos))
        
        return res




    def analyze_record(self,res):
        tweets_without_meta = []
        for item in res:
            item = list(item)
            item = unidecode.unidecode(item)
            item = item.strip()
            item = item.replace('\n','.')
            if 'RT ' in item:
                item = item.replace('RT ','')
            if '@' in item:
                item = re.sub(r'[@]\S*','',item)

                #print colored(mentions,'blue'), colored(item[0],'blue')
            if '#' in item:
                item = re.sub(r'[#]\S*','',item)
            item = item.strip()

    #keywords = ['Narendra','Modi','NaMo','Rahul Gandhi','Congress','UPA','Pappu','Papu','NDA','BJP','AAP','Arvindh Kejriwal','Arvind Kejriwal','Arvind Kejiriwal']
            tweets_without_meta.append(item)

        return tweets_without_meta


    def last_row_id(self,table_name):
        return  self.cursor.execute('SELECT max(rowid) FROM ' + table_name).fetchone()[0]


    def get_one_time_chunk(self,from_time,to_time):

        '''
        append "No tweets" string when none available. list of lists is returned. WITH TIMESTAMP AND TWEET IN A TUPLE
        '''
        from_date = datetime.datetime.strptime('2014-'+str(from_time[1])+'-'+str(from_time[0]),'%Y-%m-%d')
        to_date = datetime.datetime.strptime('2014-'+str(to_time[1])+'-'+str(to_time[0]),'%Y-%m-%d')
        time_delta = to_date - from_date

        curr_date = from_date
        res = []
        if time_delta.days ==0:
            res_cur = self.cursor.execute('select Tweet,Time from test1 where Time LIKE ?',(curr_date.strftime('%Y-%m-%d')+'%',)).fetchall()
            print curr_date
            if not res_cur:
                    res.append(curr_date.strftime('No Tweets for %d %b %Y'))
            else:
                res = (res_cur)

        else:
            for incr in range(1,time_delta.days+1):

                res_cur = self.cursor.execute('select Tweet,Time from test1 where Time LIKE ?',(curr_date.strftime('%Y-%m-%d')+'%',)).fetchall()
                if not res_cur:
                    res.append(curr_date.strftime('No Tweets for %d %b %Y'))
                else:
                    res.append(res_cur)
                curr_date = from_date + datetime.timedelta(days=incr)
        return res





    def SentimentParse(self,strings,res):
        str = 'java -cp "*" -mx1g edu.stanford.nlp.sentiment.SentimentPipeline -stdin'
        cw_dir = '/path/to/stanford-corenlp-full-2014-01-04'
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

#        for ind,item in enumerate(res):
#            print colored(item,'green') ,':' , colored(result[ind],'blue')
#            sleep(1)
#        return

    #pull_record('test1',count=1001)
