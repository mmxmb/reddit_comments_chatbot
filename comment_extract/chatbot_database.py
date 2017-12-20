import sqlite3
import json
from datetime import datetime
import time
import os
import bz2
import concurrent.futures

def create_table(c):
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY,comment_id TEXT UNIQUE,parent TEXT,comment TEXT,subreddit TEXT,unix INT,score INT)")

def format_data(data):
    data = data.replace('\n',' newlinechar ').replace('\r',' newlinechar ').replace('"',"'")
    return data

def transaction_bldr(c,connection,sql_transaction,sql):
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []

def sql_insert_replace_comment(c,connection,sql_transaction,commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?,comment_id = ?,parent = ?,comment = ?,subreddit = ?,unix = ?,score = ? WHERE parent_id =?;""".format(parentid,commentid,parent,comment,subreddit,int(time),score,parentid)
        transaction_bldr(c,connection,sql_transaction,sql)
    except Exception as e:
        print('s0 insertion 33',str(e))

def sql_insert_has_parent(c,connection,sql_transaction,commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id,comment_id,parent,comment,subreddit,unix,score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid,commentid,parent,comment,subreddit,int(time),score)
        transaction_bldr(c,connection,sql_transaction,sql)
    except Exception as e:
        print('s0 insertion 40',str(e))

def sql_insert_no_parent(c,connection,sql_transaction,commentid,parentid,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id,comment_id,comment,subreddit,unix,score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid,commentid,comment,subreddit,int(time),score)
        transaction_bldr(c,connection,sql_transaction,sql)
    except Exception as e:
        print('s0 insertion 47',str(e))

def acceptable(data):
    if len(data.split(' ')) > 1000 or len(data) < 1:
        return False
    elif len(data) > 32000:
        return False
    elif data == '[deleted]':
        return False
    elif data == '[removed]':
        return False
    else:
        return True

def find_parent(c,pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else: return False
    except Exception as e:
        #print(str(e))
        return False

def find_existing_score(c,pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else: return False
    except Exception as e:
        #print(str(e))
        return False

def get_subreddits(subreddits_file):
    with open(subreddits_file) as f:
        subreddits = f.read().lower().splitlines()
        return subreddits

def create_and_fill_db(timeframe):
    
    connection = sqlite3.connect(os.path.join(data_dir,'dbs','{}.db'.format(timeframe)))
    c = connection.cursor()
    create_table(c)

    sql_transaction = []
    row_counter = 0
    paired_rows = 0

    with bz2.BZ2File(os.path.join(data_dir,'{}'.format(timeframe.split('-')[0]),'RC_{}.bz2'.format(timeframe)),buffering=1000) as f:
        for row in f:
            row_counter += 1
            if row_counter % 100000 == 0:
                print('Timeframe: [{}],Total Rows Read: {},Paired Rows: {},Time: {}'.format(timeframe,row_counter,paired_rows,str(datetime.now())))

            if row_counter > start_row:
                try:
                    row = json.loads(row)
                    
                    if subreddit in crypto_subreddits:
                        pass
                    else:
                        continue
                    parent_id = row['parent_id'].split('_')[1]
                    body = format_data(row['body'])
                    created_utc = row['created_utc']
                    score = row['score']
                    
                    comment_id = row['id']
                    
                    subreddit = row['subreddit'].lower()
                        
                    parent_data = find_parent(c,parent_id)
                    
                    existing_comment_score = find_existing_score(c,parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            if acceptable(body):
                                sql_insert_replace_comment(c,connection,sql_transaction,comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                                
                    else:
                        if acceptable(body):
                            if parent_data:
                                if score >= 1:
                                    sql_insert_has_parent(c,connection,sql_transaction,comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                                    paired_rows += 1
                            else:
                                sql_insert_no_parent(c,connection,sql_transaction,comment_id,parent_id,body,subreddit,created_utc,score)
                except Exception as e:
                    print(str(e))
                            
            
            if row_counter > start_row:
                if row_counter % cleanup == 0:
                    print("Cleanin up!")
                    sql = "DELETE FROM parent_reply WHERE parent IS NULL"
                    c.execute(sql)
                    connection.commit()
                    c.execute("VACUUM")
                    connection.commit()
                    
# Need to define these vars at module level for Windows multiprocessing compatability
# See: https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
timeframes = ['2017-06','2017-07']
start_row = 0
cleanup = 1000000
data_dir = os.path.abspath('/Users/lawnboymax/data/reddit_comments')
crypto_subreddits_file = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','crypto_subreddits'))
crypto_subreddits = get_subreddits(crypto_subreddits_file)
    
if __name__ == '__main__':

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for timeframe,_ in zip(timeframes,executor.map(create_and_fill_db,timeframes)):
            pass    
