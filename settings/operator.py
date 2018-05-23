import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import psycopg2 as sql
from sqlalchemy import create_engine

if __name__ == '__main__':
	from operator_config import *
else:
	from .operator_config import *

class Operator(object):
	def __init__(self):
		self.sql_engine_str = sql_engine_str
		self.headers_setting = headers_setting
		self.proxy_setting = proxy_setting
		self.sql_setting = get_sql_setting()

	def get_res(self, url, proxy=False, stream=True, text=True, params=None):
		res_text = None
		if res_text is None:
			with requests.Session() as session:
				retries = Retry(total=10,
						backoff_factor=0.1,
						status_forcelist=[403, 404, 500, 502, 503, 504 ])
				session.mount('http://',HTTPAdapter(max_retries=retries))
				if not proxy:
					print('Use local ip, without proxy')
					res = session.get(url, 
						headers=self.headers_setting,
						timeout = 40, 
						stream=stream)
				else:
					print('Use proxy %s' % self.proxy_setting['http'])
					res = session.get(url, 
						headers=self.headers_setting,
						timeout = (40, 40), 
						proxies=self.proxy_setting)
			if text:
				return res.text
			else:
				return res.content 
		return

if __name__ == '__main__':
	op = Operator()
	print(op.get_res('http://www.baidu.com'))
