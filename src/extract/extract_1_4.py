import os
import pandas as pd
import requests

def get_key():
    key=os.getenv("MOVIE_API_KEY")
    return key

def gen_url(load_dt='20160101'):
    base_url="http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    key=get_key()
    url=f"{base_url}?key={key}&targetDt={load_dt}"
    for k, v in url_param.items():
        url=url=f"&{k}={v}"
    return url

def req(load_dt='20160101', url_param={}):
    url=gen_url(load_dt, url_param)
    r=requests.get(url)
    code=r.status_code
    data=r.json()
    return code, data

def req2list(load_dt, url_param={}):
    _, data=req(load_dt, url_param)
    l=data['boxOfficeResult']['dailyBoxOfficeList']
    df=pd.DataFrame(l)
    return df

def list2df(load_dt='20160101', url_param={}):
    l=req2list(load_dt, url_param)
    df=pd.DataFrame(l)
    return df

def save2df(load_dt='20160101', url_param={}):
    df=list2df(load_dt=load_dt, url_param=url_param)
    df['load_dt']=load_dt
    df.to_parquet('~/code/team_jkl/extract/extract_parquet/', partition_cols=['load_dt'])
    return df

def apply_type2df(load_dt='20160101', path="~/code/team_jkl/extract/extract_parquet"):
    df=pd.read_parquet(f'{path}/load_dt={load_dt}')
    num_cols=['rnum', 'rank', 'rankInten', 'salesAmt', 'audiCnt', 'audiAcc', 'scrnCnt', 'showCnt', 'salesShare', 'salesInten', 'salesChange', 'audiInten', 'audiChange']
    df[num_cols]=df[num_cols].apply(pd.to_numeric)
    return df
