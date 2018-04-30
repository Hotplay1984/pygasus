import tushare as ts
import pandas as pd
import datetime as dt
import psycopg2 as sql
import datetime as dt
from sqlalchemy import create_engine
import os
from send_email import *

sql_engine = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
engine = create_engine(sql_engine)
conn = sql.connect('dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432')
cur = conn.cursor()

def get_sql_dates():
    sql_str =  '''
        select datadate from money_market.shibor
        order by datadate desc
    '''
    cur.execute(sql_str)
    res = cur.fetchall()
    date_list = [x[0] for x in res]
    return date_list

def update_shibor_daily():

	df = ts.shibor_data()
	columns = df.columns.tolist()
	df['datadate'] = [x.strftime('%Y%m%d') for x in df['date']]
	df['time_stp'] = [dt.datetime.now() for x in df['datadate']]
	df = df[['datadate'] + columns[1:] + ['time_stp']]

	date_list = get_sql_dates()
	update_list = []
	for date in df['datadate']:
		if date not in date_list:
			update_list.append(date)
	df = df[df['datadate'].isin(update_list)]

	df.to_sql(
		'shibor',
		engine,
		schema = 'money_market',
		if_exists = 'append'
	)
	df.to_excel('C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\daily_update_shibor.xls')

	send_mail_via_com('hello Will', 
				'daily_update', 
				'sunweiyao@sinopac.com', 
				select_file = 'daily_update_shibor.xls')

	print('Finished, following dates updated: ' + str(update_list))
	return

if __name__ == '__main__':
	
	update_shibor_daily()
	os.system('pause')