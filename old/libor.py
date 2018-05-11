import psycopg2 as sql
from sqlalchemy import create_engine
import datetime as dt
from datetime import timedelta
import pandas as pd
import pandas_datareader.data as web
from dateutil.relativedelta import relativedelta
from send_email import *
import traceback, os

engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)

def get_libor(year=10, begdate='', enddate='',
			 data=['ONT', '1WK', '1MT', '3MT', '6MT', '12M'], currency=['USD']
			 ):
	if (enddate == ''):
		enddate = dt.datetime.now() - relativedelta(days=1)
		begdate = enddate - relativedelta(years=year)
	else:
		enddate = dt.datetime.strptime(str(enddate), '%Y%m%d')
		begdate = dt.datetime.strptime(str(begdate), '%Y%m%d')
	for curr in currency:
		print('Getting %s LIBOR ' % curr)
		series_name = []
		for item in data:
			series_name.append("%s%sD156N" % (curr, item))
		df = web.DataReader(series_name, 'fred', begdate, enddate)
		df.columns = data

		# fill to most recent day
		newlist = []
		last_line = df.iloc[-1].tolist()
		begin = df.index[-1].to_pydatetime().date()
		end = dt.datetime.now() - relativedelta(days=1)
		end = end.date()
		i = 1
		while True:
			new_timestamp = begin + timedelta(days=i)
			newlist.append([new_timestamp] + last_line)
			if(new_timestamp == end):
				break
			i += 1
		df1 = pd.DataFrame(newlist)
		df1 = df1.set_index(0)
		df1.columns = df.columns
		df = df.append(df1)
		df.index = pd.DatetimeIndex(df.index).normalize()
		df = df.dropna()
		df.reset_index(inplace=True)
		df.columns = ['datadate', 'ON', '1W', '1M', '3M', '6M', '1Y']
		df['datadate'] = [x.strftime('%Y%m%d') for x in df['datadate']]
		df['time_stp'] = [dt.datetime.now() for x in range(len(df))]
	print('getLIBOR successful')
	return df

def date_check(new_df):
	sql_str = '''
	select distinct datadate from money_market.libor
	order by datadate desc 
	'''
	df_dates = pd.read_sql_query(sql_str, engine)
	sql_dates = df_dates['datadate'].tolist()
	values = []
	for ix in new_df.index:
		date = new_df.at[ix, 'datadate']
		if date not in sql_dates:
			values.append(new_df.iloc[ix].tolist())
	return pd.DataFrame(values, columns=new_df.columns.tolist())

def send_data(df):
	local_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\'
	file_name = local_path + 'daily_update_libor.xls'
	df.to_excel(file_name)
	df.to_sql(
		'libor',
		engine,
		schema='money_market',
		if_exists = 'append',
		index = False
	)
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_libor.xls')

if __name__ == '__main__':
	try:
		df = date_check(get_libor(year=1))
		send_data(df)
	except:
		traceback.print_exc()
	os.system('pause')
