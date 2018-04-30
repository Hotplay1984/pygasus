import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
import datetime as dt
from send_email import *
import traceback
import os

engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}

def get_repo_df(res):
	bs = BeautifulSoup(res.text, 'lxml')
	val_list = [x.text for x in bs.findAll('td')]
	col_list = ['repo_code', 'repo_name', 'latest_rate', 'change', 'volume_10k', 'avg_rate']
	val_rows = [[] for x in range(int(len(val_list)/len(col_list)))]
	n = 0
	for row_n in range(len(val_rows)):
		val_rows[row_n] = val_list[n: n+len(col_list)]
		n += len(col_list)
	df = pd.DataFrame(val_rows, columns = col_list)
	datatime, datadate = get_datadate()
	df['datadate'], df['time_stp'] = [datadate for x in range(len(df))], [datatime for x in range(len(df))]
	return df

def get_datadate():
	url_date = 'http://quote.stockstar.com/bond/bankrepurchase.shtml'
	with requests.Session() as session:
			retries = Retry(total=5,
					backoff_factor=0.1,
					status_forcelist=[ 500, 502, 503, 504 ])
			session.mount('http://',HTTPAdapter(max_retries=retries))
			res = session.get(url_date, headers=headers,timeout = 40)
	bs = BeautifulSoup(res.text, 'lxml')
	date_html = bs.find('span', {'id': 'datatime'}).text
	date_str = re.findall("\d{4}-\d+-\d+ \d{2}:\d{2}:\d{2}", date_html)[0]
	datatime = dt.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
	datadate = datatime.strftime('%Y%m%d')
	
	return datatime, datadate

def download_data():
	url_p = 'http://quote.stockstar.com/webhandler/bond_interbank.ashx?type=2&sortfield=1&direction=0&_'
	url_o = 'http://quote.stockstar.com/webhandler/bond_interbank.ashx?type=3&sortfield=1&direction=0&_'
	res_ = []
	for url in [url_p, url_o]:
		with requests.Session() as session:
				retries = Retry(total=5,
						backoff_factor=0.1,
						status_forcelist=[ 500, 502, 503, 504 ])
				session.mount('http://',HTTPAdapter(max_retries=retries))
				res = session.get(url, headers=headers,timeout = 40)
				res_.append(res)
	df_p, df_o = get_repo_df(res_[0]), get_repo_df(res_[1])
	dfs = [df_p, df_o]
	
	return df_p, df_o

def save_data():
	dfs = download_data()
	file_names = ['repo_p', 'repo_o']
	for df, file_name in zip(dfs, file_names):
		file_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\daily_update_%s.xls' % file_name
		df.to_excel(file_path)
		df.to_sql(
			file_name,
			engine,
			schema='money_market',
			if_exists = 'append',
			index = False
		)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'repos')

if __name__ == '__main__':
	try:
		save_data()
	except:
		traceback.print_exc()
		os.system('pause')
	print('REPO Update Finished!')
	os.system('pause')