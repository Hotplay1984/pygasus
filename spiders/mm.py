import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(parent_path)

import pandas as pd
import json
from bs4 import BeautifulSoup
import datetime as dt
from pandas.tseries.offsets import Day
import traceback
from settings import operator

op = operator.Operator()
data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data'))
date_dt = dt.datetime.now()
date = date_dt.strftime('%Y%m%d')


def get_mm_rt(product):
	if product == 'dibo':
		columns = [ 'productCode','weightedRate', 'latestRate', 'avgPrd', 'date', ]
		url =  'http://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/iblr-md.json'
	elif product == 'dr':
		columns = [ 'productCode',  'weightedRate', 'latestRate', 'avgPrd',  'date', ]
		url = 'http://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-md.json'
	elif product == 'or':
		columns = ['productCode', 'weightedRate', 'latestRate',  'date', ]
		url = 'http://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/orr-md.json'
	raw = op.get_res(url)
	res = json.loads(raw)['records']
	update_time = json.loads(raw)['data']['showDateCN']
	values = []
	for row in res:
		values.append([row[c] for c in columns])
	df = pd.DataFrame(values, columns=columns)
	df['update_time'] = [update_time for x in range(len(df))]
	return df


def get_fr_fixing(bng_date=date, end_date=date):
	url_0 = 'http://www.chinamoney.com.cn/ags/ms/cm-u-bk-currency/FrrHis?lang=CN&startDate=@@bng_date&endDate=@@end_date'
	bng_date = dt.datetime.strptime(bng_date, '%Y%m%d').strftime('%Y-%m-%d')
	end_date = dt.datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
	url = url_0.replace('@@bng_date', bng_date)
	url = url.replace('@@end_date', end_date)
	raw_list = json.loads(op.get_res(url))['records']
	values_list = [x['frValueMap'] for x in raw_list]
	columns = ['date', 'FR001', 'FR007', 'FR014', 
	'FDR001', 'FDR007', 'FDR014']
	values = []
	for row in values_list:
		values.append([row[c] for c in columns])
	return pd.DataFrame(values, columns=columns)


def get_shibor(bng_date=date, end_date=date):
	url = 'http://www.chinamoney.com.cn/ags/ms/cm-u-bk-shibor/ShiborHis?lang=cn&startDate=@@bng_date&endDate=@@end_date'
	bng_date = dt.datetime.strptime(bng_date, '%Y%m%d').strftime('%Y-%m-%d')
	end_date = dt.datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
	url = url.replace('@@bng_date', bng_date)
	url = url.replace('@@end_date', end_date)
	labels = ['showDateCN', 'ON', '1W', '2W', '1M', '3M', '6M', '9M', '1Y']
	columns = ['date', 'ON', '1W', '2W', '1M', '3M', '6M', '9M', '1Y']
	res = json.loads(op.get_res(url))['records']
	values = []
	for row in res:
		values.append([row[label] for label in labels])
	return pd.DataFrame(values, columns=columns)

