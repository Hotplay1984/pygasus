import requests, json, asyncio
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
import datetime as dt
import os, traceback
from time import sleep
from dateutil.relativedelta import relativedelta
from send_email import *

engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}
proxies = {
    'http':'http://119.28.222.122:8888',
    'https':'https://119.28.222.122:8888'
}

def get_res(url, max_retry=3):
    try_counter = 0
    res_text = None
    while try_counter <= max_retry:
        if res_text is None:
            with requests.Session() as session:
                retries = Retry(total=10,
                        backoff_factor=0.1,
                        status_forcelist=[ 404, 500, 502, 503, 504 ])
                session.mount('http://',HTTPAdapter(max_retries=retries))
                # res = session.get(url, headers=headers,timeout = 40)
                res = session.get(url, headers=headers,timeout = 40, proxies=proxies)
            res_text = res.text 
            if res_text is not None:
                return res_text
            else:
                try_counter += 1
                
def get_json(datadate_str):
    url = '''
        http://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/Settlements/1/
        FUT?strategy=DEFAULT&tradeDate=@@datadate
        '''
    datadate_dt = dt.datetime.strptime(str(datadate_str), '%Y%m%d')
    datadate = datadate_dt.strftime('%m/%d/%Y')
    url = url.replace('\n', '')
    url = url.replace('\t', '')
    url = url.replace(' ', '')
    url = url.replace('@@datadate', datadate)
    res = json.loads(get_res(url))
    
    return res['settlements']

def get_df(datadate):
    records = get_json(datadate)

    columns = ['month', 'open', 'high', 'low', 'last', 'change',
              'settle', 'volume', 'openInterest']
    values = []
    for record in records:
        values.append([record[c] for c in columns])
    df = pd.DataFrame(values, columns=columns)
    df['datadate'] = [datadate for x in range(len(df))]
    df['time_stp'] = [dt.datetime.now() for x in range(len(df))]
    
    return df


def send_data(df):
	local_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\'
	file_name = local_path + 'daily_update_cme_eurodollar_futures.xls'
	df.to_excel(file_name)
	df.to_sql(
		'cme_eurodollar_futures',
		engine,
		schema='money_market',
		if_exists = 'append',
		index = False
	)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_cme_eurodollar_futures.xls')


if __name__ == '__main__':
	date_dt = dt.datetime.now() - relativedelta(days=1)
	date = date_dt.strftime('%Y%m%d')

	try:
		print('Update CME EuroDollar Futures Closing Data...')
		send_data(get_df(date))
		print('Finished!')
	except:
		traceback.print_exc()
	os.system('pause')