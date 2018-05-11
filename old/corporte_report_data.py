import tushare as ts
import pandas as pd
import psycopg2 as sql
import datetime as dt
from sqlalchemy import create_engine

sql_engine = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
engine = create_engine(sql_engine)
conn = sql.connect('dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432')
cur = conn.cursor()

def get_corp_report_data(year_list, quarter_list):
	''' 用于下载所有上市公司季报数据，包括业绩报告（主表）、盈利能力、
		营运能力、成长能力、偿债能力、现金流量等6方面指标。数据每季度
		更新一次。
		Parameters:
		===========
		year_list: int, 列表。所要下载的年度, 如:[2017, 2018,]；
		quarter_list: int, 列表。所要下载的季度，如[1, 2, 3].

		Return:
		=======
		无返回对象。直接将数据上传SQL.
	'''
	time_stp = pd.Timestamp(dt.datetime.now())
	for year in year_list:
		for quarter in quarter_list:
			report_period = str(year) + '0' + str(quarter)
			print('\n%s\n' % report_period)
			df_main = ts.get_report_data(year, quarter)
			df_main['report_period'] = [report_period for x in range(len(df_main))]
			df_main['time_stp'] = [time_stp for x in range(len(df_main))]
			df_main = df_main.astype('str')
			df_main.to_sql('report_main',
				engine,
				schema='corporate',
				if_exists='append')

			df_profit = ts.get_profit_data(year, quarter)
			df_profit['report_period'] = [report_period for x in range(len(df_profit))]
			df_profit['time_stp'] = [time_stp for x in range(len(df_profit))]
			df_profit = df_profit.astype('str')
			df_profit.to_sql('report_profit',
				 engine,
				schema='corporate',
				if_exists='append')

			df_operation = ts.get_operation_data(year, quarter)
			df_operation['report_period'] = [report_period for x in range(len(df_operation))]
			df_operation['time_stp'] = [time_stp for x in range(len(df_operation))]
			df_operation = df_operation.astype('str')
			df_operation.to_sql('report_operation',
				engine,
				schema='corporate',
				if_exists='append')

			df_growth = ts.get_growth_data(year, quarter)
			df_growth['report_period'] = [report_period for x in range(len(df_growth))]
			df_growth['time_stp'] = [time_stp for x in range(len(df_growth))]
			df_growth = df_growth.astype('str')
			df_growth.to_sql('report_growth',
				engine,
				schema='corporate',
				if_exists='append')

			df_debtpaying = ts.get_debtpaying_data(year, quarter)
			df_debtpaying['report_period'] = [report_period for x in range(len(df_debtpaying))]
			df_debtpaying['time_stp'] = [time_stp for x in range(len(df_debtpaying))]
			df_debtpaying = df_debtpaying.astype('str')
			df_debtpaying.to_sql('report_debt',
				engine,
				schema='corporate',
				if_exists='append')

			df_cashflow = ts.get_cashflow_data(year, quarter)
			df_cashflow['report_period'] = [report_period for x in range(len(df_cashflow))]
			df_cashflow['time_stp'] = [time_stp for x in range(len(df_cashflow))]
			df_cashflow = df_cashflow.astype('str')
			df_cashflow.to_sql('report_cashflow',
							engine,
							schema='corporate',
							if_exists='append')
	return

get_corp_report_data([2017,], [2,3])