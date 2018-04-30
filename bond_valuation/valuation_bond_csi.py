import pandas as pd
import datetime as dt
from operator import *
import traceback

current_date = dt.datetime.now().strftime('%Y%m%d')
time_stp = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def csi_download(current_date):
	url = 'http://115.29.204.48/zqgz/%sbond_valuation.zip' % str(current_date)
	try:
		res = get_res(url,text=False)
		zfile = zipfile.ZipFile(io.BytesIO(res.content))
		res_str = str(zfile.read(zfile.namelist()[0]))
		return res_str 
	except:
		traceback.print_exc()

def get_csi_bond_values(current_date):
	res_strs = csi_download(current_date).split('==========')[1]
	res_strs = res_strs.split('\\r\\n')[1:-1]
	values = []
	for res_str in res_strs:
		single_row = res_str.split('|')[:-1]
		values.append([v.replace(' ', '') for v in single_row])

	headers = ['datadate', 'shh_code', 'shz_code', 'interbank_code', 'calculation_price', 'ytm', 'modified_duration',
			 'convexity', 'clean_price', 'accrued_interest', 'reserve']
	df = pd.DataFrame(values, columns=headers)
	df['time_stp'] = [dt.datetime.now() for x in range(len(df))]
	return df

print(get_csi_bond_values('20180425'))