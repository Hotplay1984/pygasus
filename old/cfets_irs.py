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

def make_df(url):
	with requests.Session() as session:
			retries = Retry(total=5,
					backoff_factor=0.1,
					status_forcelist=[ 500, 502, 503, 504 ])
			session.mount('http://',HTTPAdapter(max_retries=retries))
			res = session.get(url, headers=headers,timeout = 40)
	bs = BeautifulSoup(res.text, 'lxml')

	tables = bs.findAll('table')
	table = tables[5]
	table_rows = table.findAll('tr')
	value_lists = [[] for x in table_rows]
	for row_n in range(len(table_rows)):
		for value in table_rows[row_n].findAll('td'):
			value_lists[row_n].append(value.text)

	columns = value_lists[0]
	new_columns = []
	for col in columns:
		if col == '日期':
			col = 'curve_date'
		elif col == '曲线名称':
			col = 'curve_name'
		elif col == '价格类型':
			col = 'number_type'
		new_columns.append(col)

	row_0 = value_lists[1]
	row_1 = value_lists[2]
	df = pd.DataFrame([row_0, row_1], columns = new_columns)
	
	return df

def download_irs_data(bng_date, end_date):
	
	bng_date_dt = dt.datetime.strptime(bng_date, '%Y%m%d')
	end_date_dt = dt.datetime.strptime(end_date, '%Y%m%d')
	bng_str = bng_date_dt.strftime('%Y-%m-%d')
	end_str = end_date_dt.strftime('%Y-%m-%d')
	df_dict = {}
	url_0 = 'http://www.chinamoney.com.cn/fe-c/interestRateSwapCurve3MHistoryAction.do?lan=cn&startDate=@@bng_str&endDate=@@end_str&bidAskType=avg&bigthType=@@curve_type&interestRateType=&message='
	curve_types = ['Shibor3M', 'FR007', 'FDR007', 'ShiborON', 'Shibor1W', 'Deposit1Y']
	
	for i in range(len(curve_types)):
		curve_type = curve_types[i]
		print(curve_type)
		url_1 = url_0.replace('@@bng_str', bng_str)
		url_1 = url_1.replace('@@end_str', end_str)
		url_1 = url_1.replace('@@curve_type', curve_type)
		print(url_1)
		df_temp = make_df(url_1)
		df_temp['datadate'] = [dt.datetime.now().strftime('%Y%m%d') for x in range(len(df_temp))]
		df_temp['time_stp'] = [dt.datetime.now() for x in range(len(df_temp))]
		df_dict[curve_types[i]] = df_temp
	
	return df_dict

def send_data(df_dict):
	local_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\'
	for curve_type, curve_df in df_dict.items():
		file_name = local_path + 'daily_update_'+ curve_type + '.xls'
		curve_df.to_excel(file_name)
		curve_df.to_sql(
			'irs_%s' % curve_type,
			engine,
			schema='cfets',
			if_exists = 'append',
			index = False
		)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'irs_curves')

def main():
	print('Start downloading IRS Curves......')
	today_dt = dt.datetime.now()
	today_str = today_dt.strftime('%Y%m%d')
	# today_str = '20171031'
	bng_date = today_str
	end_date = today_str

	try:
		df_dict = download_irs_data(bng_date, end_date)
		send_data(df_dict)
		print('IRS Curves transfer finished!')
		os.system('pause')
	except:
		print('There is a problem, pls check...')
		traceback.print_exc()
		os.system('pause')


if __name__ == '__main__':
	main()