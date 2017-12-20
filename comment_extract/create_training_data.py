import sqlite3
import pandas as pd
import os

timeframes = ['2017-11']
data_dir = os.path.abspath('/Users/lawnboymax/data/reddit_comments/')
train_dir = os.path.join(data_dir, 'model_data', 'train')
test_dir = os.path.join(data_dir, 'model_data', 'test')

for timeframe in timeframes:
    connection = sqlite3.connect(os.path.join(data_dir, 'dbs', '{}.db'.format(timeframe)))
    c = connection.cursor()
    limit = 5000
    last_unix = 0
    cur_length = limit
    counter = 0
    test_done = False

    while cur_length == limit:
        df = pd.read_sql('SELECT * FROM parent_reply WHERE unix > {} AND parent NOT NULL AND score > 0 ORDER BY unix ASC LIMIT {}'.format(last_unix, limit), connection)
        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df)
        """
     	if not test_done:
            with open(os.path.join(test_dir, 'test.from'), 'a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content + '\n')
            with open(os.path.join(test_dir, 'test.to'), 'a', encoding='utf8') as f:
                for content in df['comment'].values:
                    f.write(content + '\n')
            test_done = True
        else:
	"""
        with open(os.path.join(train_dir, '[{}]train.from'.format(timeframe)), 'a', encoding='utf8') as f:
            for content in df['parent'].values:
                f.write(content + '\n')
        with open(os.path.join(train_dir, '[{}]train.to'.format(timeframe)), 'a', encoding='utf8') as f:
            for content in df['comment'].values:
                f.write(content + '\n')

        counter += 1
        if counter % 20 == 0:
            print(counter*limit, 'rows completed')
