import os
from os import walk
from time import sleep

import win32com.client
from win32com.client import Dispatch


def get_files(select_file = 'all'):
	files = []
	path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data'
	if select_file == 'all':
		for (dirpaths, dirnames, filenames) in walk(path):
			for file in filenames:
				if file in ('daily_update_hg.xls', 'daily_update_hy.xls', 'daily_update_wsjcn.xls'):
					files.append('%s\\%s' %(path, file))
	elif select_file == 'cfets':
		for (dirpaths, dirnames, filenames) in walk(path):
			for file in filenames:
				file_list = ['daily_update_repo_o.xls', 'daily_update_g7_spot.xls', 'daily_update_cny_fx_spot.xls',
							'daily_update_cny_fx_swap.xls', 'daily_update_interbank_lending.xls', 
							'daily_update_bond.xls', 'daily_update_repo_p.xls']
				if file in file_list:
					files.append('%s\\%s' %(path, file))
	elif select_file == 'repos':
		for (dirpaths, dirnames, filenames) in walk(path):
			for file in filenames:
				if file in ('daily_update_repo_p.xls', 'daily_update_repo_o.xls'):
					files.append('%s\\%s' %(path, file))
	elif select_file == 'irs_curves':
		upload_files = ['daily_update_Shibor3M.xls', 'daily_update_FR007.xls', 'daily_update_FDR007.xls', 'daily_update_ShiborON.xls',
		'daily_update_Shibor1W.xls', 'daily_update_Deposit1Y.xls']
		for (dirpaths, dirnames, filenames) in walk(path):
			for file in filenames:
				if file in upload_files:
					files.append('%s\\%s' %(path, file))
					print('%s\\%s' %(path, file))
	else:
		files.append('%s\\%s' %(path, select_file))
	return files

def send_mail_via_com(text, subject, recipient, select_file = 'all'):
	
	o = win32com.client.Dispatch("Outlook.Application")

	Msg = o.CreateItem(0)
	Msg.To = recipient

	Msg.Subject = subject
	Msg.Body = text

	files = get_files(select_file = select_file)

	for file in files:
		Msg.Attachments.Add(file)
	Msg.Send()