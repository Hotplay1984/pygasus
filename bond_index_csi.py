import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
from send_email import *
import os, traceback
import datetime as dt
from pandas.tseries.offsets import Day
import requests

local_path = "C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\bond_index_csi\\"
engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)
url_0 = "http://www.csindex.com.cn/zh-CN/bond-valuation/stock-market-index?date1=@@bng_date&date2=@@end_date"

def convert_date(datadate):
	datadate_str = datadate
	datadate_dt = dt.datetime.strptime(datadate_str, '%Y%m%d')
	datadate = datadate_dt.strftime('%Y-%m-%d')
	return datadate

def download_bond_index(bng_date, end_date):
	bng_date = convert_date(bng_date)
	end_date = convert_date(end_date)
	url = url_0.replace('@@bng_date', bng_date)
	url = url.replace('@@end_date', end_date)
	print('start downloading bond index')
	r = requests.get(url)
	with open(local_path +'bond_index_csi.xls', 'wb') as output:
		output.write(r.content)
	print('finished!')
def read_df():
	df = pd.read_excel(local_path+'bond_index_csi.xls')
	df['time_stp'] = [dt.datetime.now() for x in range(len(df))]
	columns = ['index_code', 'index_name', 'datadate', 'index_value', 'change', 'volume_1', 'volume_2', 'modified_duration', 'convexity', 'ytm', 'coupon_rate', 'time_stp']
	df.columns = columns
	
	for col in ['modified_duration', 'convexity', 'ytm', 'coupon_rate']:
		df[col] = df[col].replace('--', 0)
	return df
def send_data(df):
	df.to_sql(
		'bond_index_csi',
		engine,
		schema='bond',
		if_exists = 'append',
		index = False
	)
	file_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\daily_update_bond_index_csi.xls'
	df.to_excel(file_path)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_bond_index_csi.xls')

def main():
	today_dt = dt.datetime.now()
	today_str = today_dt.strftime('%Y%m%d')
	# today_str = '20171204'
	bng_date = today_str
	end_date = today_str
	download_bond_index(bng_date, end_date)
	df = read_df()
	send_data(df)

if __name__ == '__main__':
	try:
		main()
	except:
		traceback.print_exc()
	os.system('pause')

