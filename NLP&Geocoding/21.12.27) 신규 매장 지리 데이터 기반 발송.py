#!/usr/bin/env python
# coding: utf-8


#######################################################################
# Title : 오프라인 신규 매장 주소 데이터 기반 LMS 발송
# Name : 박찬혁
# Create Date : 21.12.27 10:40
# Modify Date : 22.02.04 17.17
# Memo : 오프라인 신규 매장이 오픈하는 경우,
#        주변 시군구 또는 반경 xxkm 이내에 주소가 존재하는 경우 발송
#######################################################################



import psycopg2
import pandas.io.sql as sqlio
import pandas as pd
import time
import datetime
import numpy as np
import sys
from urllib.request import urlopen
from urllib import parse
from urllib.request import Request
from urllib.error import HTTPError
import json
from tqdm import tnrange, tqdm
from haversine import haversine
from sqlalchemy import create_engine
import pymysql
import requests
import warnings
warnings.filterwarnings('ignore')

# 발송 타겟이 되는 고객들 중, 등록된 거주 주소지

hk_dbname = ''
hk_host = ''
hk_port = ''
hk_user = ''
hk_password = ''


conn = psycopg2.connect(user=hk_user, password=hk_password, host=hk_host, port=hk_port, database=hk_dbname)

rds_dbname = ''
rds_host=''
rds_port= ''
rds_user= ''
rds_password= ''

mc_clientid = ''
mc_clientsecret = ''

nv_client_id = '';    # 본인이 할당받은 ID 입력
nv_client_pw = '';    # 본인이 할당받은 Secret 입력

subdomain = ''
auth_base_url = ''


#####################수정 필요 값#######################

target_sigungu_name = ['부천시']
shop_addr = ['경기도 부천시 부천로 26(심곡동, 1층)']
shop_phone = '032-654-8954'

#########################################################



print("***************************************")
print("New Offline Shop Sending LMS By Customer's Real Address")
print("***************************************\n\n")



def csv_raw_import() :
    print("***************************************")
    print('DB 이슈가 있는 경우, 과거에 적재한 데이터를 DB에 적재합니다.')
    
    df_raw = pd.read_csv('all_cust_result.csv')

    pg_engine = pg_connect(hk_user, hk_password, hk_dbname, hk_host)
    df_raw.to_sql('', con=pg_engine, schema='', if_exists='replace', chunksize=10000, index=False, method='multi')

    print('DB 적재를 완료되었습니다.')
    print("***************************************\n\n")

    return df_raw


def old_cust_locate_load() :
    print("***************************************")
    print('기존 고객 관련 공간 데이터를 불러옵니다.')
    
    cur = conn.cursor()
    
    sql_query = """
                SELECT *
                FROM 
                """
    cur.execute(sql_query)
    conn.commit()

    df_raw = pd.DataFrame(cur.fetchall())
    df_raw.columns = [desc[0] for desc in cur.description]
    
    print('고객 관련 공간 데이터 로드를 완료하였습니다.')
    print("***************************************\n\n")
    return df_raw



def cust_info_load() :
    print("***************************************")
    print('현재 Live한 고객 데이터를 불러옵니다.')
    
    cur = conn.cursor()
    
    sql_query = """
                SELECT cid__c as cid, recv_sms__c as recv_sms, address1__c as addr, join_date__c as date
                FROM 
                WHERE name not in ('탈퇴고객')
                and ispersonaccount = True
                and recv_sms__c = 'Y'
                and (daigong__c is null or daigong__c = 'N') 
                and cid__c in (select cid
                            from fnf.cust_brand
                            where brand in ('M', 'X', 'I'))
                """
    cur.execute(sql_query)
    conn.commit()

    df_cust_raw = pd.DataFrame(cur.fetchall())
    df_cust_raw.columns = [desc[0] for desc in cur.description]
    
    print('고객 데이터 로드를 완료하였습니다.')
    print("***************************************\n\n")
    return df_cust_raw



def split_old_locate_data(old_locate_df) :
    print("***************************************")
    print('기존 주소 데이터를 Home/Delv로 분리합니다.')
    
    df_delv = old_locate_df[old_locate_df['addr_type']=='delv'].reset_index().drop(['index'], axis=1)
    df_home = old_locate_df[old_locate_df['addr_type']=='home'].reset_index().drop(['index'], axis=1)
    
    print('공간 데이터 분리를 완료하였습니다.')
    print("***************************************\n\n")
    return df_delv, df_home



def new_cust_delv_load(df_delv, df_cust_raw) :
    print("***************************************")
    print('배송 데이터 중, 위경도 데이터가 Null값인 경우, 위경도 데이터를 생성합니다.')
    
    con = psycopg2.connect(dbname= rds_dbname, host=rds_host, port=rds_port, user= rds_user, password= rds_password)

    sql_01 = """select erp_cstmr_no as cid, addrse_addr as addr, max(ord_dt) as date
                from 
                where erp_cstmr_no is not null
                and addrse_addr is not null
                and dlv_pkup_shop_id in ('M510', 'X30004', 'I50002')
                group by erp_cstmr_no, addrse_addr, brnd_id
                ;"""
    df_delv_raw = sqlio.read_sql_query(sql_01, con)
    con = None

    csv_ref_addr = df_delv_raw[~df_delv_raw['addr'].str.contains('에프앤에프')].sort_values(by='date', ascending=False)
    csv_del_dup = csv_ref_addr.drop_duplicates(['cid'], keep='first').reset_index().drop(['index'], axis=1)
    df_delv_res = csv_del_dup[csv_del_dup['cid'].isin(df_cust_raw['cid'].values.tolist())].reset_index().drop(['index'], axis=1)
    
    tmp = df_delv.set_index('addr').T.to_dict('list')
    lat_list = []
    lng_list = []
    
    for i in tnrange(0, len(df_delv_res['cid'].values.tolist()), desc = '기존 경위도 할당') : 
        try : 
            tmp_dict = tmp[df_delv_res['addr'][i]]
            if(len(tmp_dict)>0) :
                lat_list.append(tmp_dict[3])
                lng_list.append(tmp_dict[4])
        except :
            lat_list.append(None)
            lng_list.append(None)
    
    df_delv_res['latitude'] = lat_list
    df_delv_res['longtitude'] = lng_list
    
    df_delv_lat_lng_null = df_delv_res[df_delv_res['latitude'].isnull()].reset_index().drop(['index'], axis=1)
    
    lat_lng_insert = call_naver_api(df_delv_lat_lng_null, df_delv_res, 'delv')
    
    print('위경도 데이터 생성이 완료되었습니다.')
    print("***************************************\n\n")
    return lat_lng_insert



def new_cust_home_load(df_home, df_cust_raw, df_delv_fin) :
    print("***************************************")
    print('고객 주소 데이터 중, 위경도 데이터가 Null값인 경우, 위경도 데이터를 생성합니다.')
    
    df_cust_res = df_cust_raw[~df_cust_raw['addr'].isnull()].reset_index().drop('index', axis=1)
    
    tmp = df_home.set_index('addr').T.to_dict('list')
    home_lat_list = []
    home_lng_list = []

    for i in tnrange(0, len(df_cust_res['cid'].values.tolist()), desc = '기존 경위도 할당') : 
        try : 
            tmp_dict = tmp[df_cust_res['addr'][i]]
            if(len(tmp_dict)>0) :
                home_lat_list.append(tmp_dict[3])
                home_lng_list.append(tmp_dict[4])
        except :
            home_lat_list.append(None)
            home_lng_list.append(None)
            
            
    df_cust_res['latitude'] = home_lat_list
    df_cust_res['longtitude'] = home_lng_list
    df_cust_res = df_cust_res[~df_cust_res['cid'].isin(df_delv_fin['cid'])]
    
    df_home_lat_lng_null = df_cust_res[df_cust_res['latitude'].isnull()].reset_index().drop(['index'], axis=1)

    lat_lng_insert = call_naver_api(df_home_lat_lng_null, df_cust_res, 'home')
    
    print('위경도 데이터 생성이 완료되었습니다.')
    print("***************************************\n\n")
    return lat_lng_insert
    


def call_naver_api(df_lat_lng_null, df_delv_res, addr_type) :
    geo_coordi = naver_geo_api(df_lat_lng_null) #Naver API를 호출
    
    np_geo_coordi = np.array(geo_coordi)
    df_lat_lng_null['latitude'] = np_geo_coordi[:, 0]
    df_lat_lng_null['longtitude'] = np_geo_coordi[:, 1]

    df_lat_lng_not_null = df_delv_res[df_delv_res['latitude']!=None]
    df_lat_lng_not_null = df_lat_lng_not_null[~df_lat_lng_not_null['cid'].isin(df_lat_lng_null['cid'].values.tolist())]
    df_lat_lng_not_null = df_lat_lng_not_null.reset_index().drop(['index'], axis=1)
    
    df_delv_fin = pd.concat([df_lat_lng_null, df_lat_lng_not_null]).reset_index().drop(['index'], axis=1)
    
    df_fin = sigungu_insert(df_delv_fin, addr_type)

    return df_fin



def naver_geo_api(df_lat_lng_null) : 
    api_url = 'https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode?query='
    
    # 네이버 지도 API 이용해서 위경도 찾기
    geo_coordi = []     
    for i in tnrange(0, len(df_lat_lng_null['addr']), desc='위경도 추출중'):
        add_urlenc = parse.quote(df_lat_lng_null['addr'][i])  
        url = api_url + add_urlenc
        request = Request(url)
        request.add_header('X-NCP-APIGW-API-KEY-ID', nv_client_id)
        request.add_header('X-NCP-APIGW-API-KEY', nv_client_pw)
        try:
            response = urlopen(request)
        except HTTPError as e:
            print('HTTP Error!')
            latitude = None
            longitude = None
        else:
            rescode = response.getcode()
            if rescode == 200:
                response_body = response.read().decode('utf-8')
                response_body = json.loads(response_body)   # json
                try : 
                    if response_body['addresses'] == [] :
                        latitude = None
                        longitude = None
                    else:
                        latitude = response_body['addresses'][0]['y']
                        longitude = response_body['addresses'][0]['x']
                except :
                    latitude = None
                    longitude = None
            else:
                print('Response error code : %d' % rescode)
                latitude = None
                longitude = None

        geo_coordi.append([latitude, longitude])
        
    return geo_coordi



def sigungu_insert(df_delv_fin, addr_type) :
    print("***************************************")
    print('주소에서 시군구를 추출합니다.')
    
    delv_sigungu_list = []

    for i in tnrange(0, len(df_delv_fin['cid']), desc='시군구 데이터 추가') :
        tmp = df_delv_fin['addr'][i].split(" ")
        for j in tmp :
            if(j.endswith("시")) :
                delv_sigungu_list.append(j)
                break
            elif(j.endswith("군")) :
                delv_sigungu_list.append(j)
                break
            elif(j.endswith("구")) :
                delv_sigungu_list.append(j)
                break

            if(j == tmp[len(tmp)-1]) :
                delv_sigungu_list.append(None)
                break


    df_delv_fin['sigungu'] = delv_sigungu_list
    df_delv_fin['recv_sms'] = 'Y'
    df_delv_fin['addr_type'] = addr_type
    
    print('시군구 추출을 완료하였습니다.')
    print("***************************************\n\n")
    return df_delv_fin
                
                

def merge_df(df_delv_fin, df_home_fin) :
    print("***************************************")
    print('기존 데이터에 시군구를 Join합니다.')
    
    df_result = pd.concat([df_home_fin, df_delv_fin])
    df_result = df_result[~df_result['sigungu'].isnull()].reset_index().drop(['index'], axis=1)
    
    print('시군구 Join을 완료하였습니다.')
    print("***************************************\n\n")
    return df_result
        
      
        
def join_sale_data(df_result, df_cust_raw) :
    print("***************************************")
    print('기존 데이터에 판매 데이터를 Join합니다.')
    
    con = psycopg2.connect(dbname= rds_dbname, host=rds_host, port=rds_port, user= rds_user, password= rds_password)

    sql_01 = """select cid, sum(amt_act) as amt_act
                from 
                where del_yn = 'N'
                and saledate::date > dateadd(day, -365, CURRENT_DATE)::date
                and amt_act > 0
                group by cid
                ;"""
    df_sale = sqlio.read_sql_query(sql_01, con)
    con = None
    
    df_fin = pd.merge(df_result, df_sale, how='left', on='cid').sort_values(by='amt_act', ascending=False)
    df_fin = df_fin[df_fin['cid'].isin(df_cust_raw['cid'].values.tolist())].reset_index().drop(['index'], axis=1)
    
    print('판매 데이터 Join을 완료하였습니다.')
    print("***************************************\n\n")
    return df_fin



def join_distance_data(df_fin) :
    print("***************************************")
    print('현재 매장으로부터 떨어진 거리를 측정하여 Join합니다.')
    
    dic = {
        'addr': shop_addr
    }

    df_shop_addr = pd.DataFrame(dic)
    shop_lat_lng = naver_geo_api(df_shop_addr)
    
    target_yn = []
    distance_list = []
    # MLB 남원
    shop_lat_lng = (float(shop_lat_lng[0][0]), float(shop_lat_lng[0][1]))

    for i in tnrange(0, len(df_fin['cid']), desc = '타겟 대상 여부 확인중') :
        if(df_fin['latitude'][i]!=None) :
            cust_lat_lng = (float(df_fin['latitude'][i]), float(df_fin['longtitude'][i]))
            distance_list.append(int(haversine(shop_lat_lng, cust_lat_lng)))
        else :
            distance_list.append(None)

    df_fin['distance'] = distance_list
    print('거리 측정 후, Join을 완료하였습니다.')
    print("***************************************\n\n")
    return df_fin



def target_sigungu(df_fin) :
    print("***************************************")
    print('요청받은 지역의 타겟 고객을 추출하고, 해당 데이터를 csv로 저장합니다.')
    
    df = df_fin[df_fin['sigungu'].isin(target_sigungu_name)].reset_index().drop(['index'], axis=1)
    
    raw_csv_name = 'all_cust_result.csv'
    target_csv_name = 'target_cust_result_%s.csv' %target_sigungu_name[0]
    
    df_fin.to_csv(raw_csv_name, header=True, index=False, encoding='utf-8-sig')
    df.to_csv(target_csv_name, header=True, index=False, encoding='utf-8-sig')
    
    print('고객 추출 및 csv 저장이 완료되었습니다.')
    print("***************************************\n\n")
    return df



def load_target_data_to_db(df_fin) : 
    print("***************************************")
    print('위경도 부여가 완료된 전체 데이터를 Heroku DB에 적재합니다.')

    pg_engine = pg_connect(hk_user, hk_password, hk_dbname, hk_host)
    df_fin.to_sql('cust_location', con=pg_engine, schema='dt_crm', if_exists='replace', chunksize=10000, index=False, method='multi')

    print('DB 적재를 완료되었습니다.')
    print("***************************************\n\n")
    

    
def pg_connect(user, password, db, host, port=):
        url = 'postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, db)
        return create_engine(url, client_encoding='utf8', use_batch_mode=True)
        return engine
    
    

def load_target_data_to_mc(target_store_df) :
    print("***************************************")
    print('타겟 고객의 데이터를 Marketing Cloud DE에 적재합니다.')
    
    conn = psycopg2.connect(user=hk_user, password=hk_password, host=hk_host, port=hk_port, database=hk_dbname)
    cur = conn.cursor()

    sql_query = """
                SELECT cid__c as cid, phone_mobile__c as mobile, name, joinbrand__c as brand
                FROM 
                WHERE name not in ('탈퇴고객')
                and ispersonaccount = True
                and recv_sms__c = 'Y'
                and cid__c in (select cid
                            from fnf.cust_brand
                            where brand in ('M', 'X', 'I'))
                """
    cur.execute(sql_query)
    conn.commit()

    df_cust_info = pd.DataFrame(cur.fetchall())
    df_cust_info.columns = [desc[0] for desc in cur.description]

    df_send = df_cust_info[df_cust_info['cid'].isin(target_store_df['cid'].values.tolist())].reset_index().drop(['index'], axis=1)
    
    df_send = df_send.append({'cid' : '', 'name' : '', 'snd_typ' : 'Y', 'mobile' : ''}, ignore_index=True)
    df_send = df_send.append({'cid' : '', 'name' : '', 'snd_typ' : 'Y', 'mobile' : ''}, ignore_index=True)
    df_send = df_send.append({'cid' : '', 'name' : '', 'snd_typ' : 'Y', 'mobile' : ''}, ignore_index=True)
    
    df_send['sender'] = shop_phone
    df_send['mid'] = ''
    df_send['snd_typ'] = 'Y'

    insert_to_db_mlb(df_send)
    
    print('DE 적재를 완료되었습니다.')
    print("***************************************\n\n")
    return 0



def insert_to_db_mlb(df_send) :
    for i in tnrange(0, len(df_send['cid'].values.tolist()), desc='MC DE 적재중') :
        if(i%200 == 0) :
            print("token 발행")
            token = generate_access_token(mc_clientid, mc_clientsecret)
        else :
            time.sleep(0.1)
            auth_token = "Bearer " + token

            headers = {
                'Accept': 'application/json; charset=UTF-8',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                'X-Requested-With': 'XMLHttpRequest',
                'Authorization': auth_token,
                'Content-Type': 'application/json',
            }

            data = '''{"ContactKey": "%s",  "EventDefinitionKey":"",
            "Data":  { "cid":"%s",    "name":"%s",    "mobile":"%s",    "brand":"%s",    "sender":"%s",    "mid":"%s", "snd_typ" : "%s"}}''' %(df_send['cid'][i], df_send['cid'][i], str(df_send['name'][i]), df_send['mobile'][i], df_send['brand'][i], df_send['sender'][i], df_send['mid'][i], df_send['snd_typ'][i])

            response = requests.post(, headers=headers, data=data.encode('utf-8'))
            rspn_text = response.text
            
            if(rspn_text.find('eventInstanceId') == -1) :
                print(rspn_text)
            
            
            
def generate_access_token(clientid: str, clientsecret: str) -> str:
    headers = {'content-type': 'application/json'}
    payload = {
      'grant_type': 'client_credentials',
      'client_id': mc_clientid,
      'client_secret': mc_clientsecret
    }
    authentication_response = requests.post(
        url=auth_base_url, data=json.dumps(payload), headers=headers
    ).json()

    if 'access_token' not in authentication_response:
        raise Exception(
          f'Unable to validate (ClientID/ClientSecret): {repr(authentication_response)}'
        )
    access_token = authentication_response['access_token']

    return access_token
    
    
    

if __name__ == '__main__' : 
#     old_locate_df = csv_raw_import() # DB 적재 이슈 시, 과거에 추출한 전체 데이터 csv 파일을 DB에 Replace
    
    old_locate_df = old_cust_locate_load() #DB상에 적재되어있는 데이터 Load
    cust_info_df = cust_info_load() # 고객 데이터 Load
    old_delv_df, old_home_df = split_old_locate_data(old_locate_df) # DB상에 적재되어있는 데이터 분리(주소지/배송지)
    new_delv_df = new_cust_delv_load(old_delv_df, cust_info_df) # 추가 혹은 갱신된 배송지 위경도 추출 후 반영
    new_home_df = new_cust_home_load(old_home_df, cust_info_df, new_delv_df) # 추가 혹은 갱신된 주소지 위경도 추출 후 반영
    merge_df = merge_df(new_delv_df, new_home_df) # 새로 생성된 배송지+주소지 데이터 병합
    update_sale_df = join_sale_data(merge_df, cust_info_df) # 최근 1년간 판매 데이터 Join
    fin_df = join_distance_data(update_sale_df) # 지정된 주소로부터 떨어진 거리 데이터 Join
    target_store_df = target_sigungu(fin_df) # 주소 데이터에서 시군구 추출하여 Join
    
    load_target_data_to_db(fin_df) # Heroku DB 상에 전체 데이터 적재
    load_target_data_to_mc(target_store_df) # MC 상에 발송 타겟에 대한 데이터 적재(기존 DE 수동으로 Clear하여야 함)
