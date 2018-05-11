import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
from send_email import *
import os, traceback
import datetime as dt
from pandas.tseries.offsets import Day
import requests

local_path = "C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\bond_curve_csi\\"
engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)

def url_maker(curve_ids, str_date):
	url_list = []
	url_str = ["http://www.csindex.com.cn/zh-CN/bond-valuation/bond-yield-curve?",
		  "type=","&line_id=","&line_date=","&line_type=","&start_date=",
		  "&end_date=", "&download="]
	url_type = '1'
	url_line_date = '1'
	url_line_type = '1'
	url_start_date = str_date
	url_end_date = ''
	url_download = '1'
	for curve_id in curve_ids:
		url_line_id = curve_id
		url_option = [url_type, url_line_id, url_line_date, url_line_type, 
					 url_start_date, url_end_date, url_download]

		url = url_str[0]
		for option, option_str in zip(url_option, url_str[1:]):
			url += option_str+option
		url_list.append(url)
	return url_list

def download_single_curve(url_pkg):
	name = url_pkg[0]
	url = url_pkg[1]

	r = requests.get(url)
	print('start downloading %s' % name)
	with open(local_path + name +'.xls', 'wb') as output:
		output.write(r.content)
	print('%s finished!' % name)

def read_single_file(id_):
	file_name = local_path + id_ +  '.xls'
	df = pd.read_excel(file_name)
	df['time_stp'] = [dt.datetime.now() for x in range(len(df))]
	df.columns = ['curve_type', 'datadate', 'time_bucket', 'spot', 'ytm', 'forward', 'time_stp']
	for col in ['spot', 'ytm', 'forward']:
		df[col] = df[col].replace('null', None)
	return df

def form_df(curve_ids):
	df = pd.DataFrame()
	for id_ in curve_ids:
		df_ = read_single_file(id_)
		df = pd.concat([df, df_]).reset_index()[df_.columns.tolist()]
	return df

def send_data(df):
	df.to_sql(
		'bond_curve_csi',
		engine,
		schema='bond',
		if_exists = 'append',
		index = False
	)
	file_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\daily_update_bond_curve_csi.xls'
	df.to_excel(file_path)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_bond_curve_csi.xls')

if __name__ == '__main__':
	today = dt.datetime.now()
	today = today.strftime('%Y-%m-%d')
	curve_ids = [str(i) for i in range(1, 38)]
	url_list = url_maker(curve_ids, today)
	url_pkgs = [[id_, url] for id_, url in zip(curve_ids, url_list)]

	for url_pkg in url_pkgs:
		download_single_curve(url_pkg)

	df = form_df(curve_ids)
	send_data(df)
	print('%s bond_curve_csi update finished.' % today)
	os.system('pause')