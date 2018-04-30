import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import json
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
# url = 'http://www.chinamoney.com.cn/dqs/rest/dqs-u-fx-bk/FoivCurvHisData?'
url = 'http://www.chinamoney.com.cn/ags/ms/cm-u-bk-fx/FoivCurvHisData?'
def get_json(url, data):
	with requests.Session() as session:
			retries = Retry(total=5,
					backoff_factor=0.1,
					status_forcelist=[ 404, 500, 502, 503, 504 ])
			session.mount('http://',HTTPAdapter(max_retries=retries))
			res = session.get(url, params = data,headers=headers,timeout = 40)
	print(url)
	j_res = res.json()
	
	return j_res

def get_row_count(page_size, today):
	# url = 'http://www.chinamoney.com.cn/dqs/rest/dqs-u-fx-bk/FoivCurvHisData?'
	data = {
		'pageSize':page_size,
		'lang':'CH',
		'pageNum':1,
		'tradeTime':'16:00',
		'startDate':today,
		'endDate':today
	}
	j_res = get_json(url, data)
	total_count = j_res['data']['totalCount']
	return total_count

def download_volatility(datadate):
	date_dt = dt.datetime.strptime(datadate, '%Y%m%d')
	today = date_dt.strftime('%Y-%m-%d')
	total_count = get_row_count(1, today)
	data = {
		'pageSize':total_count,
		'lang':'CH',
		'pageNum':1,
		'tradeTime':'16:00',
		'startDate':today,
		'endDate':today
	}
	j_res = get_json(url, data)

	columns_web = ['tradeDate', 'tradeTime', 'ccyPair', 'volatilityTypeEN', 'tenor',
			  'askVolatilityStr', 'bidVolatilityStr', 'midVolatilityStr']
	columns = ['tradeDate', 'tradeTime', 'ccyPair', 'volatilityType', 'tenor',
	'askVolatility', 'bidVolatility', 'midVolatility']
	value_lists = [[] for x in j_res['records']]
	for row_n in range(len(j_res['records'])):
		for col in columns_web:
			value_lists[row_n].append(j_res['records'][row_n][col])
	df = pd.DataFrame(value_lists, columns = columns)
	df['datadate'] = [dt.datetime.now().strftime('%Y%m%d') for x in range(len(df))]
	df['time_stp'] = [dt.datetime.now() for x in range(len(df))]
	
	return df


def send_data(df):
	local_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\'
	file_name = local_path + 'daily_update_cfets_fxoption_implied_volatility.xls'
	df.to_excel(file_name)
	df.to_sql(
		'cfets_fxoption_implied_volatility',
		engine,
		schema='cfets',
		if_exists = 'append',
		index = False
	)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_cfets_fxoption_implied_volatility.xls')
	
def main():
	print('Start downloading Cfets FX Option Implied Volatility......')
	today_dt = dt.datetime.now()
	today_str = today_dt.strftime('%Y%m%d')
	try:
		df = download_volatility(today_str)
		send_data(df)
		print('finished!')
		os.system('pause')
	except:
		traceback.print_exc()
		os.system('pause')

if __name__ == '__main__':

	main()
