import tushare as ts
import pandas as pd
import psycopg2 as sql
import datetime as dt
from sqlalchemy import create_engine
import os
import traceback

from proxy_setting import *

sql_engine = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
engine = create_engine(sql_engine)
conn = sql.connect('dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432')
cur = conn.cursor()



def get_code_list():
	sql_get_code = '''
		select code from basic_info.stock_info
	'''
	cur.execute(sql_get_code)
	res = cur.fetchall()
	code_list = [x[0] for x in res]

	return code_list

def sql_recent_date(code):
	sql_single_stock = '''
		select * from stock_daily."@@code"
		order by date desc
	'''
	sql_single_stock = sql_single_stock.replace('@@code', code)
	# print(sql_single_stock)
	# cur.execute(sql_single_stock)
	# res = cur.fetchall()
	# columns = ['data_id', 'date', 'open', 'close',
	# 		  'high', 'low', 'volume', 'code','time_stp']
	# df = pd.DataFrame(res, columns = columns)
	# sql_dates = df['date'].tolist()
	df = pd.read_sql_query(sql_single_stock, engine)
	sql_dates = df['date'].tolist()
	return sql_dates

def update_sql_stock_info():
	print('retrieving stock info from ts...')
	stock_info = ts.get_stock_basics()
	print('stock info retrieved, uploading...')
	time = pd.Timestamp(dt.datetime.now())
	stock_info['time_stp'] = [time for x in range(len(stock_info))]
	stock_info.to_sql('stock_info',
					 engine,
					 schema = 'basic_info',
					 if_exists = 'replace')

	return stock_info

def get_update_date(code):
	df_index = ts.get_k_data(code, index = False)
	date_list = df_index['date']
	date_to_update = []
	sql_dates = sql_recent_date(code)
	for date in date_list:
		if date not in sql_dates:
			date_to_update.append(date)
	start_date, end_date = '', ''
	if len(date_to_update) != 0:
		start_date = date_to_update[0]
		end_date = date_to_update[-1]
	return start_date, end_date

def upd_stock_daily(code):
	start, end = get_update_date(code)
	df_upd = ts.get_k_data(code, index = False, start = start, end = end)
	time_stp = pd.Timestamp(dt.datetime.now())
	time_stps = [time_stp for x in range(len(df_upd))]
	df_upd['time_stp'] = time_stps
	df_upd.to_sql(code,
				 engine,
				 schema = 'stock_daily',
				 if_exists = 'append')

def get_sql_table_list():
	sql_get_table_names = '''
	SELECT table_name
	FROM information_schema.tables
	WHERE table_schema='stock_daily'
	'''
	cur.execute(sql_get_table_names)
	res = cur.fetchall()
	table_list = [x[0] for x in res]

	return table_list

def batch_delete(date):
	table_list = get_sql_table_list()
	sql_del = '''
		delete from stock_daily."@@code"
		where date = '@@date'
	'''
	total = len(table_list)
	process = 0
	for table in table_list:
		sql_del = sql_del.replace('@@code', table)
		sql_del = sql_del.replace('@@date', date)
		print(sql_del)
		print('{:.4%}'.format(process/total))
		cur.execute(sql_del)
		conn.commit()
		sql_del = sql_del.replace(table, '@@code')
		sql_del = sql_del.replace(date, '@@date')
		process += 1

def stock_daily_update():
	try:
		update_sql_stock_info()
	except:
		print('stock_basic_info update failed')
		time_stp = pd.Timestamp(dt.datetime.now())
		info = 'stock_basic_info updae failed'
		info_fail_log = pd.DataFrame([[time_stp, info]], columns = ['time_stp', 'info'])
		info_fail_log.to_csv('baisc_info_fail_log.csv')
	code_list = get_code_list()
	total_num = len(code_list)
	run_num = 0
	table_list = get_sql_table_list()
	log_dict = {}
	for code in code_list:
		log_dict[code] = []
		if code in table_list:
			date_result = 'ERR'
			try:
				start, end = get_update_date(code)
				if start != '':
					print('code: %s, update from %s to %s' % (code, start, end))
					date_result = 'OK'
					log_dict[code].append('date updated.')
				else:
					print('code: %s is currently updated.' % code)
					date_result = 'UPDATED'
					log_dict[code].append('updated')
					log_dict[code].append('')
			except:
				print('%s failed to update dates.' % code)
				log_dict[code].append('date update failed.')
				log_dict[code].append('')
			if date_result == 'OK':
				try:
					upd_stock_daily(code)
					print('code: %s daily data retrieved.' % code)
					log_dict[code].append('market data retrieved.')
				except:
					print('%s failed to update daily data.' % code)
					log_dict[code].append('market data retrieving failed.')
		else:
			result = 'ERR'
			try:
				df = ts.get_k_data(code)
				result = 'OK'
				log_dict[code].append('new stock market data retrieved.')
			except:
				print('new stock: %s failed to retrieve data.' % code)
				log_dict[code].append('new stock market data retrieving failed')
				log_dict[code].append('')
			if result == 'OK':
				try:
					df.to_sql(code,
							  engine,
							  schema='stock_daily',
							  if_exists='append')
					print('new stock: %s added to sql.' % code)
					log_dict[code].append('new stock updated to sql.')
				except:
					print('new stock failed to write to sql')
					log_dict[code].append('new stock writing failed.')
		run_num += 1
		process = '{:.4%}'.format(run_num / total_num)
		print('completed: %s' % process)

	df_log = pd.DataFrame()
	log_codes = log_dict.keys()
	status_0, status_1 = [], []
	for code in log_codes:
		status_0.append(log_dict[code][0])
		status_1.append(log_dict[code][1])
	df_log['code'] = log_codes
	df_log['status_0'] = status_0
	df_log['status_1'] = status_1
	time_stp = pd.Timestamp(dt.datetime.now())
	df_log['time_stp'] = [time_stp for x in range(len(df_log))]

	df_log.to_sql(
		'stock_daily_log',
		engine,
		schema='stock_daily',
		if_exists='append'
	)

	t = dt.datetime.now().strftime('%Y%m%d')
	log_name = 'stock_log_' + t + '.csv'
	df_log.to_csv(log_name)


if __name__ == '__main__':
	proxy_setting = Proxy_Setting()
	proxy = proxy_setting.check_proxy()
	proxy_setting.close_proxy()
	stock_daily_update()
	if proxy is True:
		proxy_setting.start_proxy()
	os.system('pause')