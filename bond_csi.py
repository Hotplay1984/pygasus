import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
from send_email import *
import os
import datetime as dt
import requests, zipfile, io
import traceback

engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)

current_date = dt.datetime.now().strftime('%Y%m%d')
time_stp = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def download_file(current_date):
	local_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\bond_csi\\'
	url = 'http://115.29.204.48/zqgz/%sbond_valuation.zip' % current_date
	local_filename = local_path + url.split('/')[-1]
	try:
		r = requests.get(url, stream=True)
		z = zipfile.ZipFile(io.BytesIO(r.content))
		z.extractall(local_filename)
	except:
		traceback.print_exc()
		os.system('pause')

def data_df(current_date):
	header = ['datadate', 'shh_code', 'shz_code', 'interbank_code', 'calculation_price', 'ytm', 'modified_duration',
			 'convexity', 'clean_price', 'accrued_interest', 'reserve', 'time_stp']
	stored_data = [[] for x in range(len(header))]
	local_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\bond_csi\\%sbond_valuation.zip\\%sbond_valuation.txt' % (current_date, current_date)
	with open(local_path, 'r') as f:
		raws = f.readlines()
	data_lines = raws[12:]

	for data_line in data_lines:
		datas = [x.strip(' ') for x in data_line.split('|')[:-1]] + [time_stp]
		for i in range(len(datas)):
			if datas[i]:
				stored_data[i].append(datas[i])
			else:
				stored_data[i].append('')

	df = pd.DataFrame()
	for i in range(len(stored_data)):
		if i in [1,2,3]:
			df[header[i]] = ["'" + str(x) for x in stored_data[i]]
		else:
			df[header[i]] = stored_data[i]
	
	return df

download_file(current_date)
df_data = data_df(current_date)


# download_file('20180129')
# df_data = data_df('20180129')
try:
	file_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\daily_update_bond_csi.xls'
	df_data.to_excel(file_path)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_bond_csi.xls')
except:
	print('data transfer failed...')
	traceback.print_exc()
	os.system('pause')

df_data.to_sql(
			'bond_csi',
			engine,
			schema='bond',
			if_exists = 'append',
			index = False
		)

print('Bond_CSI complete')
os.system('pause')