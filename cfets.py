import requests
import json
import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
from send_email import *
import os, traceback
import datetime as dt

engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)

url_lend = 'http://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/iblr-md.json'
url_repo_p = 'http://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-md.json'
url_repo_o = 'http://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/orr-md.json'
url_bond = 'http://www.chinamoney.com.cn/dqs/rest/dqs-u-bond/CbtPri?lang=cn&flag=1'
url_cny_spot = 'http://www.chinamoney.com.cn/webdata/fe/rmb_fx_spot.json'
url_cny_swap = 'http://www.chinamoney.com.cn/webdata/fe/rmb_fx_swap.json'
url_g7_spot = 'http://www.chinamoney.com.cn/webdata/fe/g7_spot.json'

header_lend = [ 'productCode','weightedRate', 'latestRate', 'avgPrd', ]
header_repo_p = [ 'productCode',  'weightedRate', 'latestRate', 'avgPrd', ]
header_repo_o = ['productCode', 'weightedRate', 'latestRate', ]
header_bond = ['abdAssetEncdShrtDesc', 'bondcode','bpNum', 'dmiLatestContraRate', 
			   'dmiWghtdContraRate', 'dmiTtlTradedAmnt']
header_cny_spot = ['askPrc', 'bidPrc', 'ccyPair', 'midprice', 'time']
header_cny_swap = ['ccpair', 'label_1M', 'label_1W', 
				   'label_1Y', 'label_3M', 'label_6M', 'label_9M']
header_g7_spot = ['askPrc', 'bidPrc', 'ccyPair', 'midprice', 'time']
craw_dict = {
	'interbank_lending':[url_lend, header_lend],
	'repo_p':[url_repo_p, header_repo_p],
	'repo_o':[url_repo_o, header_repo_o],
	'bond': [url_bond, header_bond],
	'cny_fx_spot': [url_cny_spot, header_cny_spot],
	'cny_fx_swap': [url_cny_swap, header_cny_swap],
	'g7_spot': [url_g7_spot, header_g7_spot],
}

def get_data_dict():

	df_dict = {}
	for key, value in craw_dict.items():
		print(key)
		url, header = value[0], value[1]
		session = requests.Session()
		res = session.get(url)
		j_res = res.json()
		raw_data = j_res['records']

		data_col = [[] for x in header]
		df = pd.DataFrame()
		for col in range(len(header)):
			for row in raw_data:
				try:
					val = row[header[col]]
				except:
					val = ''
				data_col[col].append(val)
		for col in range(len(header)):
			df[header[col]] = data_col[col]
		df_dict[key] = df

	df = df_dict['cny_fx_swap']
	col_list = ['ccpair',]
	for col in df.columns.tolist()[1:]:
		temp_list_0, temp_list_1 = [x.split('/')[0] for x in df[col]], [x.split('/')[1] for x in df[col]]
		col_name_0 = col.replace('label_', '') + '_bid'
		col_name_1 = col.replace('label_', '') + '_ask'
		df[col_name_0], df[col_name_1] = temp_list_0, temp_list_1
		col_list.append(col_name_0)
		col_list.append(col_name_1)
	df_dict['cny_fx_swap'] = df[col_list].copy()

	for key, df in df_dict.items():
		for col in df.columns.tolist():
			df[col] = df[col].replace('---', '')
			df[col] = df[col].replace('&nbsp;', '')
		time_stps = [dt.datetime.now() for x in range(len(df))]
		datadate = [dt.datetime.now().strftime('%Y%m%d') for x in range(len(df))]
		df['datadate'] = datadate
		df['time_stp'] = time_stps
		df_dict[key] = df

	return df_dict

def upload_data(df_dict):

	for key, df in df_dict.items():
		try:
			file_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\daily_update_%s.xls' % key
			df.to_excel(file_path)
			df.to_sql(
				key,
				engine,
				schema='cfets',
				if_exists = 'append',
				index = False
			)
		except:
			traceback.print_exc()
			print('data transfer failed...')
			os.system('pause')

	send_mail_via_com('hello Will', 
				'daily_update', 
				'sunweiyao@sinopac.com', 
				select_file = 'cfets')

def main():
	try:
		df_dict = get_data_dict()
	except:
		traceback.print_exc()
		print('downloading data failed...')
		os.system('pause')
	try:
		upload_data(df_dict)
	except:
		traceback.print_exc()
		print('update sql failed......')
		os.system('pause')
	print('CFETS Data Update Success!')
	os.system('pause')

if __name__ == '__main__':
	main()