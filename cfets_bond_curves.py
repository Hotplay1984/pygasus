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

def get_curve_codes():
	print('downloading curve codes...')
	url_curve_code = 'http://www.chinamoney.com.cn/fe-c/closedYieldCurveHistoryQueryAction.do?'
	bs = get_soup(url_curve_code)
	curve_code_dict = {}
	for val_html in bs.find('select', {'name':'bondTypeTemp'}).findAll('option'):
		curve_code_dict[val_html.text] = val_html.attrs['value']
	print('curve codes downloaded...')

	return curve_code_dict

def download_curves(startDate, endDate):
	url_0 = 'http://www.chinamoney.com.cn/fe-c/closedYieldCurveHistoryQueryAction.do?startDateTool=@@startDate&endDateTool=@@endDate&showKey=1%2C2%2C3%2C&termId=0.1&bondType=@@curveCode&start=@@startDate&end=@@endDate&bondTypeTemp=@@curveCode&reference=1&reference=2&reference=3&termIdTemp=0.1'
	url_0 = url_0.replace('@@startDate', startDate)
	url_0 = url_0.replace('@@endDate', endDate)
	curve_code_dict = get_curve_codes()
	df_dict = {}
	for curve_name, curve_code in curve_code_dict.items():
		print(curve_name)
		url = url_0.replace('@@curveCode', curve_code)
		bs = get_soup(url)
		try:
			trs = bs.find('table', {'class':'rmb-cnt'}).findAll('tr')[3:]
		except:
			print('Error on %s' % curve_name)
			continue
		columns = ['curve_date', 'time_bucket', 'ytm', 'spot', 'forward']
		value_lists = [[] for x in range(len(trs))]
		for row_n in range(len(trs)):
			for val_html in trs[row_n].findAll('td')[:]:
				val = val_html.text.replace('\xa0', '')
				val = val.replace('---', '0.0000')
				value_lists[row_n].append(val)
		df = pd.DataFrame(value_lists, columns = columns).dropna()
		df['curve_name'] = [curve_name for x in range(len(df))]
		df['curve_code'] = [curve_code for x in range(len(df))]
		df_dict[curve_code] = df
	columns = ['curve_date', 'time_bucket', 'ytm', 'spot', 'forward', 'curve_name', 'curve_code']
	df = pd.DataFrame([[None for x in range(len(columns))]], columns = columns)
	for df_temp in df_dict.values():
		df = pd.concat([df, df_temp])
	df = df.dropna().reset_index()[columns]
	df['datadate'] = [dt.datetime.now().strftime('%Y%m%d') for x in range(len(df))]
	df['time_stp'] = [dt.datetime.now() for x in range(len(df))]

	return df

def send_data(df):
	local_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\'
	file_name = local_path + 'daily_update_cfets_bond_curves.xls'
	df.to_excel(file_name)
	df.to_sql(
		'cfets_bond_curves',
		engine,
		schema='cfets',
		if_exists = 'append',
		index = False
	)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_cfets_bond_curves.xls')
	
def main():
	print('Start downloading Cfets bond curves......')
	today_dt = dt.datetime.now()
	today_str = today_dt.strftime('%Y-%m-%d')
	# today_str = '2017-10-30'
	try:
		df = download_curves(today_str, today_str)
		send_data(df)
		print('finished!')
		os.system('pause')
	except:
		traceback.print_exc()
		os.system('pause')

if __name__ == '__main__':

	main()
