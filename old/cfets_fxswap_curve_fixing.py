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
engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}

def get_soup(url):
	with requests.Session() as session:
			retries = Retry(total=5,
					backoff_factor=0.1,
					status_forcelist=[ 500, 502, 503, 504 ])
			session.mount('http://',HTTPAdapter(max_retries=retries))
			res = session.get(url, headers=headers,timeout = 40)
	bs = BeautifulSoup(res.text, 'lxml')
	return bs



def download_swap_curve(datadate):
	date_dt = dt.datetime.strptime(datadate, '%Y%m%d')
	today = date_dt.strftime('%Y-%m-%d')
	url_0 = 'http://www.chinamoney.com.cn/fe-c/fxCSwapHistoryAction.do?startDate=@@date&endDate=@@date&message='
	url = url_0.replace('@@date', today)

	bs = get_soup(url)
	trs = bs.findAll('table')[4].findAll('tr')[1:]
	value_lists = [[] for x in trs]
	for row_n in range(len(trs)):
		for val_html in trs[row_n].findAll('td'):
			val = val_html.text
			value_lists[row_n].append(val)
	columns = ['curve_date', 'time_bucket', 'swap_point', 'change_pips', 'data_type', 'fx_rate', 'fx_rate_change']
	df = pd.DataFrame(value_lists, columns = columns)
	df['datadate'] = [dt.datetime.now().strftime('%Y%m%d') for x in range(len(df))]
	df['time_stp'] = [dt.datetime.now() for x in range(len(df))]

	return df


def send_data(df):
	local_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\'
	file_name = local_path + 'daily_update_cfets_fxswap_curve.xls'
	df.to_excel(file_name)
	df.to_sql(
		'cfets_fxswap_curve',
		engine,
		schema='cfets',
		if_exists = 'append',
		index = False
	)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_cfets_fxswap_curve.xls')
	
def main():
	print('Start downloading Cfets FX Swap Curve......')
	today_dt = dt.datetime.now()
	today_str = today_dt.strftime('%Y%m%d')
	# today_str = '20171108'
	try:
		df = download_swap_curve(today_str)
		send_data(df)
		print('finished!')
		os.system('pause')
	except:
		traceback.print_exc()
		os.system('pause')

if __name__ == '__main__':

	main()