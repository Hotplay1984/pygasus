import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import json, time
from dateutil.relativedelta import relativedelta
import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
import datetime as dt
from send_email import *
import traceback

engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}
proxies = {
	'http':'http://119.28.222.122:8888',
	'https':'https://119.28.222.122:8888'
}

term_dict = {
	'1Y':'15237309',
	'2Y':'15237320',
	'3Y':'15237321',
	'4Y':'15237322',
	'5Y':'15237323',
	'6Y':'15237324',
	'7Y':'15237325',
	'8Y':'15237326',
	'9Y':'15237327',
	'10Y':'15237307',
	'12Y':'15237310',
	'15Y':'15237313',
	'20Y':'15237314',
	'30Y':'15237315'
}
time_buckets = ['1Y', '2Y', '3Y', '4Y', '5Y',
			   '6Y', '7Y', '8Y', '9Y', '10Y',
			   '12Y', '15Y', '20Y', '30Y']

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


def get_usd_irs_dict():
	url_head = 'https://produkte.erstegroup.com/modules/res/gethighstockdata_ajax.php?'
	para_dict = {'resolution' : '1D',
				 'check' : 'false',
				 'timerange':'1Y',
				 'prodid' : '@@prodid',
				 'exchange':'%24%24%24%24',
				 'type':'price',
				 'market':'at',
				 'assettype':'interest',
				 'lan' : 'en',
				 'lastprice' : '1',
				 'startlimitdate' : '',
				 'enddate' : ''}
	para_str = ''
	for key, value in para_dict.items():
		para_str += '&' + key + '=' + value
	url_0 = url_head + para_str.strip('&')
	df_dict = {}
	for time_bucket in time_buckets:
		df = pd.DataFrame()
		prodid = term_dict[time_bucket]
		url = url_0.replace('@@prodid', prodid)
		res = get_res(url)
		js = json.loads(res)
		df_tmp = pd.DataFrame(js, columns = ['time_stp', time_bucket])
		df['datadate_%s' % time_bucket] = [dt.datetime.fromtimestamp(x/1000).strftime('%Y%m%d')
									  for x in df_tmp['time_stp']]
		df[time_bucket] = df_tmp[time_bucket].tolist()
		df_dict[time_bucket] = df
	return df_dict


def get_usd_irs_df():
	df_dict = get_usd_irs_dict()
	df = df_dict['1Y']
	df.columns = ['datadate', '1Y']
	datadates = df['datadate'].tolist()
	for time_bucket in time_buckets[1:]:
		values = []
		df_tmp = df_dict[time_bucket]
		for date in datadates:
			if date in df_tmp['datadate_%s' % time_bucket].tolist():
				values.append(df_tmp[df_tmp['datadate_%s' % time_bucket] == date][time_bucket].values[0])
			else:
				values.append(None)
		df[time_bucket] = values
	df['time_stp'] = [dt.datetime.now() for x in range(len(df))]
	return df


def date_check(new_df):
	sql_str = '''
	select distinct datadate from money_market.usd_irs_a_3m_history
	order by datadate desc 
	'''
	df_dates = pd.read_sql_query(sql_str, engine)
	sql_dates = df_dates['datadate'].tolist()
	values = []
	for ix in new_df.index:
		date = new_df.at[ix, 'datadate']
		if date not in sql_dates:
			values.append(new_df.iloc[ix].tolist())
	return pd.DataFrame(values, columns=new_df.columns.tolist())

def send_data(df):
	local_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\'
	file_name = local_path + 'daily_update_usd_irs_a_3m_history.xls'
	df.to_excel(file_name)
	df.to_sql(
		'usd_irs_a_3m_history',
		engine,
		schema='money_market',
		if_exists = 'append',
		index = False
	)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_usd_irs_a_3m_history.xls')

if __name__ == '__main__':
	try:
		print('Update USD IRS A 3M History...')
		send_data(date_check(get_usd_irs_df()))
		print('Finished!')
	except:
		traceback.print_exc()
	os.system('pause')

