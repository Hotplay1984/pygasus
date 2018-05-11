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

def get_open_close(url):
	res = get_res(url)
	bs = BeautifulSoup(res, 'lxml')
	values_str = [x.text for x in bs.find('div', {'id':'elem946924'}).findAll('td')]
	values = [values_str[1].replace('%', ''), values_str[3].replace('%', '')]
	update_time = bs.find('div', {'id':'elem946915'}).find('tr').find('th').text
	values.append(update_time)
	return values

def get_usd_irs_df():
	url_intraday = 'https://produkte.erstegroup.com/CorporateClients/en/MarketsAndTrends/Fixed_Income/Capital_markets_derivatives/index.phtml?elem999058_index=Table_SwapRates_World_US_USD&elem999058_durationTimes=0b'
	res = get_res(url_intraday)
	bs = BeautifulSoup(res, 'lxml')
	replaces = ['\xa0', ' ', '\r', '\t']
	txts, urls, open_close_values = [], [], []
	for tr in bs.find('div', {'id':'elem999058'}).find('table').findAll('tr')[1:]:
		txt = tr.text
		for r in replaces:
			if r in txt:
				txt = tr.text.replace(r, '')
		if len(txt) > 0:
			txts.append(txt)
		url = tr.find('a')['href']
		urls.append(url)
		open_close_values.append(get_open_close(url))
		print(url)
	values_current = []
	for txt in txts:
		value_single_row = [x for x in txt.split('\n') if x != '']
		values_current.append(value_single_row)
	values = [x+y for x, y in zip(values_current, open_close_values)]
	df = pd.DataFrame(values, columns=['code', 'current', 'change', 'open', 'prev_close', 'update_time'])
	df['datadate'] = [dt.datetime.now().strftime('%Y%m%d')
					 for x in df['update_time']]
	df['url'] = urls
	df['time_stp'] = [dt.datetime.now() for x in range(len(df))]
	return df

def send_data(df):
	local_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\'
	file_name = local_path + 'daily_update_usd_irs_a_3m_current.xls'
	df.to_excel(file_name)
	df.to_sql(
		'usd_irs_a_3m_current',
		engine,
		schema='money_market',
		if_exists = 'append',
		index = False
	)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_usd_irs_a_3m_current.xls')

if __name__ == '__main__':
	try:
		print('Updating USD IRS A-3M Current...')
		(send_data(get_usd_irs_df()))
		print('Finished!')
	except:
		traceback.print_exc()
	os.system('pause')
