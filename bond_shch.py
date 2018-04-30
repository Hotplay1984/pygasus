import requests
import json
import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
from send_email import *
import os
import datetime as dt

url = 'http://www.shclearing.com/shchapp/web/valuationclient/findvaluationdata'
engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)

session = requests.Session()
start_list = [x*10 for x in range(0, 10000)]
curr = dt.datetime.now()
today = curr.strftime('%Y-%m-%d')
df = pd.DataFrame()

print('Downloading bond valuation data from SHCH. Date: %s' % today)

for start in start_list:
	data={'startTime':today,
	  'endTime':today,
	  'bondNames':'',
	  'bondCodes':'',
	  'bondTypes':'402895815c8fa1ac015d400a02735729',
	  'limit':'10',
	  'start':str(start),
	  'sortFlag':'1',
	  'sortNameFlag':'1',
	  'sortDateFlag':'1'
	 }
	res = session.get(url,params=data)
	j_res = res.json()
	if not j_res['data']['datas']:
		break
	columns = list(j_res['data']['datas'][0].keys())
	data_dict = {}
	for col in columns:
		raw_list = j_res['data']['datas']
		values = []
		for raw_data in raw_list:
			values.append(raw_data[col])
		data_dict[col] = values 
	if len(pd.DataFrame(data_dict)) == 0:
		break
	df = df.append(pd.DataFrame(data_dict)).reset_index()[columns]
	print('downloading: %s' % str(start))

df_field = pd.read_excel('C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\field_mapping.xlsx')
field_dict = {}
for i in df_field.index.tolist():
	field_dict[df_field.ix[i]['field_code']] = df_field.ix[i]['field_name']

if len(df) > 0:
	df_data = df[df_field['field_code'].tolist()].copy()
else:
	print('no data...')
	os.system('pause')

new_col = [field_dict[x] for x in df_data.columns.tolist()]
df_data.columns = new_col
id_list = [x + '_' +  y for x, y in zip(df_data['债券代码'].tolist(), df_data['估值日期'].tolist())]
df_data['id'] = id_list
stp = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df_data['time_stp'] = [stp for x in range(len(df_data))]

try:
	file_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\daily_update_bond_shch.xls'
	df_data.to_excel(file_path)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_bond_shch.xls')
except:
	print('data transfer failed...')
	os.system('pause')


df_data.to_sql(
			'bond_shch',
			engine,
			schema='bond',
			if_exists = 'append',
			index = False
		)

print('All done! Valuation date: %s' % today)
os.system('pause')


