import os
import pandas as pd
import requests

def get_key():
    key = os.getenv("MOVIE_API_KEY")
    return key

def gen_url(load_dt='20160101'):
    base_url="http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    key = get_key()
    url = f"{base_url}?key={key}&targetDt={load_dt}"
    return url

def req(load_dt='20160101'):
    url = gen_url(load_dt)
    r = requests.get(url)
    code = r.status_code
    data = r.json()
    return code, data

def req2list(load_dt='20160101'):
    _, data = req(load_dt)
    l = data['boxOfficeResult']['dailyBoxOfficeList']
    df = pd.DataFrame(l)
    return df

def list2df(load_dt='20160101'):
    l = req2list(load_dt)
    df = pd.DataFrame(l)
    return df

def save2df(load_dt='20160101'):
        """airflow 호출 지점"""
    df = list2df(load_dt)
    df['load_dt'] = load_dt
    df.to_parquet('~/tmp/team_parquet/', partition_cols=['load_dt'])
    return df

