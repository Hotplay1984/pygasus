import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(parent_path)

import pandas as pd
import datetime as dt
from pandas.tseries.offsets import Day
import zipfile, io
import traceback
from settings import operator

op = operator.Operator()
data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data'))
date_dt = dt.datetime.now()-Day(1)
date = date_dt.strftime('%Y%m%d')


def get_bond_csi(datadate=date):
	url = 'http://115.29.204.48/zqgz/%sbond_valuation.zip' % str(datadate)
	res = op.get_res(url,text=False)
	z = zipfile.ZipFile(io.BytesIO(res))
	for f_info in z.infolist():
		if 'txt' in f_info.filename:
			data_file = z.open(f_info)
	raw_data = [t.decode('utf-8') for t in data_file.readlines()[12:]]
	data_str = ''.join(raw_data)
	data_list = [t.replace(' ', '') for t in data_str.split('\r\n')]
	values = []
	for data_row in data_list:
		values.append(data_row.split('|')[:-1])
		
	header = ['datadate', 'shh_code', 'shz_code', 'interbank_code', 
			  'calculation_price', 'ytm', 'modified_duration',
			  'convexity', 'clean_price', 'accrued_interest', 'reserve']
	return pd.DataFrame(values, columns=header).dropna()


def get_bond_cfets(datadate=date):
	url_0 = 'http://www.chinamoney.com.cn/fe-c/finalValuation.do?showDate=@@showDate&bondType=@@bond_type'
	columns = ['value_date', 'bond_type', 'bond_name', 'bond_code', 
	'full_price', 'clean_price', 'yeild', 'coupon_type']

	showDate = dt.datetime.strptime(datadate, '%Y%m%d').strftime('%Y-%m-%d')
	bond_types = ['100001', '100002', '999901', '100003', '100027', 
	'100006', '100010', '100004', '100029', '100041', '100011']

	df_dict = {}
	for bond_type in bond_types[:]:
		url = url_0.replace('@@showDate', showDate)
		url = url.replace('@@bond_type', bond_type)
		print(bond_type)
		res = get_res(url)
		bs = BeautifulSoup(res, 'lxml')

		tds = bs.findAll('tr')[0].findAll('tr')[1:]
		value_lists = [[] for row in range(len(tds))]
		for row_n in range(len(tds)):
			for val_tag in tds[row_n].findAll('td')[:-1]:
				val = val_tag.text.replace('\r', '')
				val = val.replace('\n', '')
				val = val.replace('\t', '')
				val = val.replace('\xa0', '')
				value_lists[row_n].append(val)
		df = pd.DataFrame(value_lists, columns = columns)
		df_dict[bond_type] = df

	df = pd.DataFrame([[None for x in range(len(columns))]], columns = columns)
	for df_temp in df_dict.values():
		df = pd.concat([df, df_temp])
	df = df.dropna().reset_index()[columns]
	df['datadate'] = [dt.datetime.now().strftime('%Y%m%d') for x in range(len(df))]
	return df


def get_bond_index_csi(bng_date=date, end_date=date):
	url_0 = "http://www.csindex.com.cn/zh-CN/bond-valuation/stock-market-index?date1=@@bng_date&date2=@@end_date"
	bng_date = dt.datetime.strptime(str(bng_date), '%Y%m%d').strftime('%Y-%m-%d')
	end_date = dt.datetime.strptime(str(end_date), '%Y%m%d').strftime('%Y-%m-%d')
	url = url_0.replace('@@bng_date', bng_date)
	url = url.replace('@@end_date', end_date)
	return pd.read_excel(url)


def get_bond_trade():
	url = 'http://www.chinamoney.com.cn/ags/ms/cm-u-md-bond/CbtPri?lang=cn&flag=1&bondName='
	labels = ['abdAssetEncdShrtDesc', 'bondcode','bpNum', 'dmiLatestContraRate', 
			  'dmiWghtdContraRate', 'dmiTtlTradedAmnt', 'showDate']
	res = json.loads(get_res(url))['records']
	values = []
	for row in res:
		values.append([row[label] for label in labels])
	return pd.DataFrame(values, columns = labels)
