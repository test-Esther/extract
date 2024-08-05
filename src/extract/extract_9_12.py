import os
import pandas as pd
import requests


def req(load_dt='20160901'):
    url = gen_url(load_dt)
    r = requests.get(url)
    code = r.status_code
    data = r.json()
    return code, data


def gen_url(load_dt='20160901'):
    base_url="http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    key = get_key()
    url = f"{base_url}?key={key}&targetDt={load_dt}"
    return url


def get_key():
    key = os.getenv("MOVIE_API_KEY")
    return key


def req2list(load_dt='20160901'):
    _, data = req(load_dt)
    l = data['boxOfficeResult']['dailyBoxOfficeList']
    df = pd.DataFrame(l)
    return df

def list2df(load_dt='20160901'):
    l = req2list(load_dt)
    df = pd.DataFrame(l)
    return df

def save2df(load_dt='20160901'):
    """airflow 호출 지점"""
    df = list2df(load_dt)
    df['load_dt'] = load_dt
    df.to_parquet('~/tmp/team_parquet/', partition_cols=['load_dt'])
    return df

def apply_type2df(load_dt="20160901", path="~/tmp/team_parquet"):
    df = pd.read_parquet(f'{path}/load_dt={load_dt}')
    num_cols = ['rnum', 'rank', 'rankInten', 'salesAmt', 'audiCnt', 'audiAcc', 'scrnCnt', 'showCnt', 'salesShare', 'salesInten', 'salesChange', 'audiInten', 'audiChange']

    #for c in num_cols:
        #df[c] = pd.to_numeric(df[c])

    df[num_cols] = df[num_cols].apply(pd.to_numeric)

    return df
