import json
import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
import os
import datetime as dt
from send_email import *

engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)
# field_mapping_ = {
	# '基准利差(BP)':'benchmark_spread_bp',
	# '债券起息日':'init_start_date',
	# '实际发行量(亿)': 'actual_issue_amount_100mio',
	# '行权日期':'option_exercise_date',
	# '发行价格(元)':'issue_price',
	#  '债项/主体评级一':'bond_issuer_rating_0',
	# '债券简称':'bond_short_name',
	# '票面利率(%)':'coupon_rate',
	#  '基准利率':'benchmark_rate',
	# '债券发行日': 'issue_date',
	#  '信用评级机构一':'rating_agency_0',
	#  '面值(元)':'par_value',
	# '债券摘牌日':'delist_date',
	# '行权类型':'option_type',
	# '参考收益率(%)':'reference_yield',
	# '币种':'currency',
	# '计划发行量(亿)':'issue_amount_planned_100mio',
	#  '流通范围':'market',
	#  '信用评级机构二':'rating_agency_1',
	# '到期兑付日':'maturity_date',
	#  '债券代码':'bond_code',
	# '计息基础':'day_count',
	#  '发行人':'issuer',
	#  '债券类型':'bond_type',
	# '债券期限':'term',
	#  '息票类型':'coupon_type',
	# '付息频率':'frequency',
	# '上市交易日':'listed_date',
	# '发行收益率(%)':'issue_yield',
	# '托管机构':'trustee',
	# '债项/主体评级二':'bond_issuer_rating_1',
# }
field_mapping = {
	'bnchmkSpreadRate':'benchmark_spread_bp',
	'frstValueDate':'init_start_date',
	'issueAmnt': 'actual_issue_amount_100mio',
	'exerciseDate':'option_exercise_date',
	'issuePrice':'issue_price',
	 'creditSubjectRating_0':'bond_issuer_rating_0',
	'bondName':'bond_short_name',
	'parCouponRate':'coupon_rate',
	 'bnchmkRate':'benchmark_rate',
	'issueDate': 'issue_date',
	 'entyFullName_0':'rating_agency_0',
	 'parValue':'par_value',
	'dlstngDate':'delist_date',
	'exerciseType':'option_type',
	'refyld':'reference_yield',
	'bondCcy':'currency',
	'plndIssueAmnt':'issue_amount_planned_100mio',
	 'pltfrmIndctr':'market',
	 'entyFullName_1':'rating_agency_1',
	'mrtyDate':'maturity_date',
	 'bondCode':'bond_code',
	'intrstBss':'day_count',
	 'entyFullName':'issuer',
	 'bondType':'bond_type',
	'bondPeriod':'term',
	 'couponType':'coupon_type',
	'couponFrqncy':'frequency',
	'lstngDate':'listed_date',
	'isYldRate':'issue_yield',
	'custodian':'trustee',
	'creditSubjectRating_1':'bond_issuer_rating_1',
}
field_mapping = {v:k for k, v in field_mapping.items()}
fields = ['bond_short_name','bond_code','issuer','bond_type','issue_date','maturity_date',
		 'listed_date','delist_date','term','market','par_value','issue_price',
		 'issue_amount_planned_100mio', 'actual_issue_amount_100mio','currency',
		 'day_count','coupon_type','init_start_date','frequency','coupon_rate','issue_yield',
		 'reference_yield','benchmark_rate','benchmark_spread_bp',
		 'rating_agency_0','bond_issuer_rating_0','rating_agency_1','bond_issuer_rating_1',
		 'option_type','option_exercise_date','trustee',]

def update_sql_and_send_email():
	path = r'C:\Users\client\Desktop\python_work\work\market_data\bond_basic_info\data'
	print('Updating CFETS Bond basic infomation')
	print('Reading local files...')
	for path, folders, filenames in os.walk(path):
		dirs = [path + '\\' + x for x in filenames if '-' not in x]
	total_list = []
	for file_path in dirs:
		with open(file_path, 'r', encoding='utf8') as f:
			dict_ = json.loads(f.read())
			info_dict = {}
			for key, value in dict_.items():
				if key not in ['creditRateEntyList', 'exerciseInfoList']:
					info_dict[key] = value
				elif key == 'creditRateEntyList':
					info_dict['entyFullName_0'] = value[0]['entyFullName']
					info_dict['creditSubjectRating_0'] = value[0]['creditSubjectRating']
					info_dict['entyFullName_1'] = value[1]['entyFullName']
					info_dict['creditSubjectRating_1'] = value[1]['creditSubjectRating']
				elif key == 'exerciseInfoList':
					info_dict['exerciseDate'] = value[0]['exerciseDate']
					info_dict['exerciseType'] = value[0]['exerciseType']
			total_list.append(info_dict)

	df = pd.DataFrame()
	list_dict = {field:[] for field in fields}
	for dict_ in total_list:
		for field in fields:
			try:
				list_dict[field].append(dict_[field_mapping[field]])
			except:
				list_dict[field].append('')
	for field in fields:
		df[field] = list_dict[field]
	datadate_dt = dt.datetime.now()
	datadate = datadate_dt.strftime('%Y%m%d')
	df['datadate'] = [datadate for x in range(len(df))]
	df['time_stp'] = [datadate_dt for x in range(len(df))]
	print('Uploading to SQL...')
	df.to_sql('basic_info',
			 engine,
			 schema='bond',
			 if_exists='replace',
			 index=False)
	local_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\'
	file_name = local_path + 'daily_update_cfets_bondinfo.xls'
	print('Writing to xls file...')
	df.to_excel(file_name)
	print('Sending email...')
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_cfets_bondinfo.xls')

if __name__ == '__main__':
	update_sql_and_send_email()