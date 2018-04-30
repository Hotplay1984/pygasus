import os
import psutil
import subprocess
import winreg 

class Proxy_Setting():

	def __init__(self):
		return
	def check_proxy(self):
		pids = psutil.pids()
		process_list = []
		for pid in pids:
			try:
				p = psutil.Process(pid)
				process_list.append(p.name())
			except:
				process_list.append(None)
		if 'ShadowsocksR-dotnet4.0.exe' in process_list:
			return True
		else:
			return False
			
	def reset_proxy_setting(self):
		INTERNET_SETTINGS = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
		r'Software\Microsoft\Windows\CurrentVersion\Internet Settings',
		0, winreg.KEY_ALL_ACCESS)
		# _, reg_type = winreg.QueryValueEx(INTERNET_SETTINGS, 'ProxyEnable')
		winreg.SetValueEx(INTERNET_SETTINGS, 'ProxyEnable', 0, 4, 0)

	def start_proxy(self):
		proxy = self.check_proxy()
		if proxy is False:
			subprocess.Popen('C:\\Users\\client\\Desktop\\ShadowsocksR-4.7.0-win(fix Pac)\\ShadowsocksR-dotnet4.0.exe')

	def close_proxy(self):
		proxy = self.check_proxy()
		if proxy is True:
			os.system("TASKKILL /F /IM ShadowsocksR-dotnet4.0.exe")
			self.reset_proxy_setting()