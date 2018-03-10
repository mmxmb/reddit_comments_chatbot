import sqlite3
import pandas as pd
import os
import datetime

def create_training_data(timeframe):

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
            
data_dir = os.path.abspath('/home/ubuntu/')
train_dir = os.path.join(data_dir, 'model_data', 'train')
test_dir = os.path.join(data_dir, 'model_data', 'test')
timeframes = ['2017-01', '2017-02', '2017-03', '2017-04', '2017-05', '2017-06', '2017-07', '2017-08', '2017-09', '2017-10', '2017-11', 
              '2016-01', '2016-02', '2016-03', '2016-04', '2016-05', '2016-06', '2016-07', '2016-08', '2016-09', '2016-10', '2016-11', '2016-12',
              '2015-01', '2015-02', '2015-03', '2015-04', '2015-05', '2015-06', '2015-07', '2015-08', '2015-09', '2015-10', '2015-11', '2015-12',
              '2014-01', '2014-02', '2014-03', '2014-04', '2014-05', '2014-06', '2014-07', '2014-08', '2014-09', '2014-10', '2014-11', '2014-12']

if __name__ == '__main__':
    startTime = datetime.now()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for timeframe in executor.map(create_training_data,timeframes):
            pass    
    print(datetime.now() - startTime)
