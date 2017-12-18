import sqlite3
import json
from datetime import datetime
import bz2
import os

timeframe = '2017-11'
sql_transaction = []
#data_dir = os.path.abspath('/home/lawnboymax/projects/chatbot_data')
data_dir = os.path.abspath('/Users/lawnboymax/data/reddit_comments')
crypto_subreddits = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'crypto_subreddits'))

connection = sqlite3.connect(os.path.join(data_dir, 'dbs', '{}.db'.format(timeframe)))
c = connection.cursor()


def create_table():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply 
    (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE,
    parent TEXT, comment TEXT, subreddit TEXT, unix INT, 
    score INT)""")

def format_data(data):
    data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
    return data

def find_parent(pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        print('find_parent', str(e))
        return False

def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        print('find_existing_score', str(e))
        return False

def acceptable(data):
    if data == None:
        return False
    if len(data.split(' ')) > 50 or len(data) < 1: # interested in short comments (word count)
        return False
    elif len(data) > 1000: # interested in short comments (character count)
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True

def sql_insert_replace_comment(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s-UPDATE',str(e))

def sql_insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-PARENT',str(e))


def sql_insert_no_parent(commentid,parentid,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-NO_PARENT',str(e))

def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 10000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []

def get_subreddits(subreddits_file):
    with open(subreddits_file) as f:
        subreddits = f.read().lower().splitlines()
        return subreddits
        

if __name__=="__main__":
    create_table()
    row_counter = 0
    paired_rows = 0
    subreddits = get_subreddits(crypto_subreddits)

    with open(os.path.join(data_dir, '{}'.format(timeframe.split('-')[0]), 'RC_{}'.format(timeframe)), buffering=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)
            try:
                comment_id = row['name']
            except KeyError:
                comment_id = row['author']
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            if row['subreddit'] not in subreddits:
                continue
            else:
                subreddit = row['subreddit']

            parent_data = find_parent(parent_id)

            if score >= 2: #threshold to filter useless comments (low score)
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            if acceptable(body):
                                sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                    else:
                        if acceptable(body):
                            if parent_data:
                                sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                                paired_rows += 1
                            else:
                                sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)
                                
            if row_counter % 100000 == 0:
                print("Total rows read: {}, Paired rows: {}, Time: {}".format(row_counter, paired_rows, str(datetime.now())))