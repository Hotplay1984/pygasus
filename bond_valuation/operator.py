import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
import config_operator as config

def get_res(url, max_retry=3, proxy=False, stream=True, text=True):
	try_counter = 0
	res_text = None
	while try_counter <= max_retry:
		if res_text is None:
			with requests.Session() as session:
				retries = Retry(total=10,
						backoff_factor=0.1,
						status_forcelist=[ 404, 500, 502, 503, 504 ])
				session.mount('http://',HTTPAdapter(max_retries=retries))
				if not proxy:
					print('Use local ip')
					res = session.get(url, 
						headers=config.headers_setting,
						timeout = 40, 
						stream=stream)
				else:
					print('Use proxy %s' % config.proxy_setting['http'])
					res = session.get(url, 
						headers=config.headers_setting,
						timeout = (40, 40), 
						proxies=config.proxy_setting)
			res_text = res.text
			if text:
				if res_text is not None:
					return res_text
				else:
					print('%s, Retrying...' % str(res.status_code))
					try_counter += 1
					sleep(1)
			else:
				return res
	return 