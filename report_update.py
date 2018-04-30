import os
from analysis_report import *
from research_report_pdf_download import *
# from reuters_report_beta import *
from wsjcn_report import *
from send_email import *
from proxy_setting import *
import traceback

try:
	proxy_setting = Proxy_Setting()

	proxy_setting.close_proxy()
	df_hg = daily_update(1, 5, 'hg')
	df_hy = daily_update(1, 10, 'hy')
	df_wsjcn = wsjcn_daily_upload()

	print('sending email...')
	send_mail_via_com('hello Will', 'daily_update', 'sunweiyao@sinopac.com')
	download_pdf()
	print('done.')
except:
	traceback.print_exc()
	os.system('pause')

os.system('pause')