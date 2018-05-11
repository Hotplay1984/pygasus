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

curve_ids_csi = {
	"1": "国债", "2": "政策金融债（进出口和农发行）", "4": "铁道债", 
	"6": "中短票据超AAA", "7": "中短票据AAA", "8": "中短票据AA+", "9": "中短票据AA", 
	"10": "中短票据AA-", "13": "产业债AA+", "14": "产业债AA", "15": "产业债AA-", 
	"16": "城投债AA+", "17": "城投债AA", "18": "城投债AA-", "20": "公司债AAA", 
	"21": "公司债AA+", "22": "公司债AA", "23": "公司债AA-", "24": "国开债", 
	"25": "同业存单(国有银行)", "26": "同业存单(股份制银行)", "27": "同业存单(农商行)", 
	"28": "中短票据A+", "29": "中短票据A", "30": "企业债AAA", "31": "企业债AA+", 
	"32": "企业债AA", "33": "企业债AA-", "34": "企业债A+", "35": "企业债A", 
	"36": "公司债A+", "37": "公司债A"}
curve_ids_cfets = {
	'100001': '国债', '100002': '央行票据', '100003': '政策性金融债(进出口行、农发行)',
	'100029': '超短期融资券(AAA+)', '210001': '地方政府债(AAA)', '240001': '政府支持机构债券', '240002': '政府支持机构债券(减税)',
	'260001': '短期融资券(AAA)', '260002': '短期融资券(AAA+)', '260003': '短期融资券(AAA-)',
	'260004': '短期融资券(AA+)', '260005': '短期融资券(AA)', '260006': '短期融资券(AA-)',
	'260007': '短期融资券(A+)', '260008': '短期融资券(A)', '260009': '短期融资券(A-)',
	'270001': '中期票据(AAA)', '270002': '中期票据(AA+)', '270003': '中期票据(AA)',
	'270004': '中期票据(AA-)', '270005': '中期票据(AAA+)', '290002': '超短期融资券(AAA)',
	'330001': '企业债(AAA+)', '330002': '企业债(AAA)', '330003': '企业债(AA+)',
	'330004': '企业债(AA)', '330005': '企业债(AA-)', '330006': '企业债(AAA2)',
	'330007': '企业债(AA+2)', '330008': '企业债(AA2)', '330009': '企业债(AA-2)',
	'340001': '政策性金融债(1Y_Depo)点差', '350001': '政策性金融债(Shibor_3M)点差',
	'360001': '企业债(1Y_Depo，AAA)点差', '360002': '企业债(Shibor_3M，AAA)点差',
	'360003': '企业债(Shibor_1Y，AA+)点差', '370001': '中期票据(1Y_Depo，AAA)点差', '370002': '中期票据(1Y_Depo，AA+)点差',
	'370003': '中期票据(1Y_Depo，AA)点差', '370004': '中期票据(1Y_Depo，AA-)点差', '380001': '同业存单(AAA+)',
	'380002': '同业存单(AAA)', '380003': '同业存单(AA+)', '380004': '同业存单(AA)',
	'999901': '政策性金融债(国开行)'}


def get_csi_url(curve_id=1, bng_date=date, end_date=date):
	bng_date = dt.datetime.strptime(str(bng_date), '%Y%m%d').strftime('%Y-%m-%d')
	end_date = dt.datetime.strptime(str(end_date), '%Y%m%d').strftime('%Y-%m-%d')
	url_str = ["http://www.csindex.com.cn/zh-CN/bond-valuation/bond-yield-curve?",
		  "type=","&line_id=","&line_date=","&line_type=","&start_date=",
		  "&end_date=", "&download="]
	url_type = '1'
	url_line_date = '1'
	url_line_type = '1'
	url_start_date = bng_date
	url_end_date = end_date
	url_download = '1'
	url_line_id = str(curve_id)
	url_option = [url_type, url_line_id, url_line_date, url_line_type, 
				 url_start_date, url_end_date, url_download]
	url = url_str[0]
	for option, option_str in zip(url_option, url_str[1:]):
		url += option_str+option
	return url


def get_csi_curve(curve_id='all', bng_date=date, end_date=date):
	if curve_id == 'all':
		for curve_id in list(curve_ids_csi.keys()):
			print('Downloading %s' % curve_ids_csi[curve_id])
			df = pd.read_excel(get_csi_url(x))
			df['curve_id'] = [str(x) for x in range(len(df))]
			df_dict[str(x)] = df
		return df_dict
	else:
		return pd.read_excel(get_csi_url(str(curve_id)))


def get_cfets_curve_codes():
	print('downloading curve codes...')
	url_curve_code = 'http://www.chinamoney.com.cn/fe-c/closedYieldCurveHistoryQueryAction.do?'
	res = get_res(url_curve_code)
	bs = BeautifulSoup(res, 'lxml')
	curve_code_dict = {}
	for val_html in bs.find('select', {'name':'bondTypeTemp'}).findAll('option'):
		curve_code_dict[val_html.attrs['value']] = val_html.text
	print('curve codes downloaded...')
	return curve_code_dict


def get_cfets_bond_curve_single(url, curve_code):
	bs = BeautifulSoup(get_res(url), 'lxml')
	trs = bs.find('table', {'class':'rmb-cnt'}).findAll('tr')[3:]
	columns = ['curve_date', 'time_bucket', 'ytm', 'spot', 'forward']
	value_lists = [[] for x in range(len(trs))]
	for row_n in range(len(trs)):
		for val_html in trs[row_n].findAll('td')[:]:
			val = val_html.text.replace('\xa0', '')
			val = val.replace('---', '0.0000')
			value_lists[row_n].append(val)
	df = pd.DataFrame(value_lists, columns = columns).dropna()
	df['curve_name'] = [curve_ids_cfets[curve_code] for x in range(len(df))]
	df['curve_code'] = [curve_code for x in range(len(df))]
	columns = ['curve_date', 'time_bucket', 'ytm', 'spot', 'forward', 'curve_name', 'curve_code']
	return df


def get_cfets_bond_curve(curve_id='all', bng_date=date, end_date=date, update=False):
	url_0 = 'http://www.chinamoney.com.cn/fe-c/closedYieldCurveHistoryQueryAction.do?startDateTool=@@startDate&endDateTool=@@endDate&showKey=1%2C2%2C3%2C&termId=0.1&bondType=@@curveCode&start=@@startDate&end=@@endDate&bondTypeTemp=@@curveCode&reference=1&reference=2&reference=3&termIdTemp=0.1'
	bng_date = dt.datetime.strptime(bng_date, '%Y%m%d').strftime('%Y-%m-%d')
	end_date = dt.datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
	
	url_0 = url_0.replace('@@startDate', bng_date)
	url_0 = url_0.replace('@@endDate', end_date)
	if update:
		curve_ids_cfets = get_cfets_curve_codes()
	if curve_id == 'all':
		df_dict = {}
		for curve_code in curve_ids_cfets:
			url = url_0.replace('@@curveCode', curve_code)
			print('Downloading curve: %s' % curve_ids_cfets[curve_code])
			df = get_cfets_bond_curve_single(url, curve_code)
			df_dict[curve_code] = df
		return df_dict
	else:
		url = url_0.replace('@@curveCode', str(curve_id))
		return get_cfets_bond_curve_single(url, curve_id)
		