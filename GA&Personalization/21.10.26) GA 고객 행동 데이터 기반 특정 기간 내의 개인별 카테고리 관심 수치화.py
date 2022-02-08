#!/usr/bin/env python
# coding: utf-8


#######################################################################
# Title : GA 고객 행동 데이터 기반 특정 기간 내의 개인별 카테고리 관심 수치화
# Name : 박찬혁
# Create Date : 21.10.26 10:12
# Modify Date : 22.02.08 17:12
# Memo : 
#######################################################################

import psycopg2
import pandas.io.sql as sqlio
import pandas as pd
import time
import datetime
import numpy as np
import sys
import warnings
warnings.filterwarnings('ignore')


# 일자 정보
now = datetime.datetime.now()
now_days = now.strftime("%Y-%m-%d")
print('오늘 일자 : %s' %now_days)
bef_7days = (now - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
print('7일전 일자 : %s' %bef_7days)
bef_23days = (now - datetime.timedelta(days=23)).strftime("%Y-%m-%d")
print('23일전 일자 : %s' %bef_23days)
bef_30days = (now - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
print('30일전 일자 : %s' %bef_30days)

# redshift 접속정보
rds_dbname = ''
rds_host=''
rds_port= ''
rds_user= ''
rds_password= ''


# hekroku 접속정보
hk_user=''
hk_password=''
hk_host=''
hk_port=''
hk_database=''


# 카테고리 관련 정보
cate_sex = ['여성', '남성', '혼용']
cate_age = ['키즈', '성인']
cate_list = ['가방', '기타', '레깅스', '맨투맨/후드', '모자', '백팩', '스윔웨어', '시즌슈즈',
             '신발', '아우터', '악세서리', '트레이닝복', '티셔츠', '패딩', '팬츠', '후리스']


print("***************************************")
print("자사몰 고객 행동 및 판매 데이터 기반 카테고리 별 추천")
print("***************************************\n\n")




def sale_extract() :
    print("***************************************")
    print('Redshift의 판매 데이터를 가져옵니다.')
    print('......')
    
    con = psycopg2.connect(dbname=rds_dbname, host=rds_host, port=rds_port, user=rds_user, password=rds_password)

    sql_01 = """select cid, saledate, partkey, qty, amt_act as actualamt, season, partcode+'-'+color as productsku
                from 
                where del_yn = 'N'
                and saledate::date between '%s' and '%s'
                and brand in ('X')
                ;""" %(bef_7days, now_days)
    df_sale_raw = sqlio.read_sql_query(sql_01, con)
    con = None
    
    df_sale_tmp = pd.DataFrame(df_sale_raw, columns=['cid', 'productsku', 'qty', 'actualamt'])
    
    df_sale_pivot = pd.pivot_table(df_sale_tmp, index = ['productsku'], values = ['qty', 'actualamt'], aggfunc = ['sum'], fill_value = 0)
    idx_sale_tmp = df_sale_pivot.index.values.tolist()
    qty_sale_tmp = df_sale_pivot[df_sale_pivot.columns[1]].values.tolist()
    actamt_sale_tmp = df_sale_pivot[df_sale_pivot.columns[0]].values.tolist()
    
    df_sale_ref = pd.DataFrame({'productsku' : idx_sale_tmp, 'qty' : qty_sale_tmp, 'actualamt' : actamt_sale_tmp})
    
    print('판매 데이터 import가 완료 되었습니다.')
    print("***************************************\n")
    return df_sale_ref, df_sale_tmp['cid']



def part_extract() :
    print("***************************************")
    print('Redshift의 카테고리 데이터를 가져옵니다.')
    print('......')
    
    con = psycopg2.connect(dbname=rds_dbname, host=rds_host, port=rds_port, user=rds_user, password=rds_password)

    sql_02 = """select partcode, season, color, partcode+'-'+color as productsku, gender, adult, ctgr, item
                from
                where brand = 'X'
                ;"""
    df_part_raw = sqlio.read_sql_query(sql_02, con)
    df_part_raw = df_part_raw.drop_duplicates(['productsku'], keep ='first')
    con = None
    
    df_part_raw['gender'] = df_part_raw['gender'].map({'X' : '혼용', 'D' : '여성', 'U' : '남성'})
    df_part_raw['adult'] = df_part_raw['adult'].map({'A' : '성인', 'K' : '키즈'})
    
    print('카테고리 데이터 import가 완료 되었습니다.')
    print("***************************************\n")
    return df_part_raw



def join_dfs(df_sale, df_part) :
    print("***************************************")
    print('판매 데이터와 카테고리 데이터를 조인합니다.')
    print('......')
    
    df_join = pd.merge(df_sale, df_part, how='inner', on='productsku').sort_values('actualamt', ascending=False)
    
    print('조인이 완료 되었습니다.')
    print("***************************************\n")
    return df_join



# 조건에 만족하는 성별-성인키즈-카테고리 별 판매 수량을 파일로 추출
def export_csv(df_join) :
    print("***************************************")
    print('성별, 성인여부, 카테고리 별로 csv 파일을 추출합니다.')
    print('......')
    
    for i in cate_sex :
        for j in cate_age :
            for k in cate_list :
                if(True) :
                    k = k.replace('/', '')
                    tmp = df_join[df_join['gender'] == i][df_join['adult'] == j][df_join['ctgr'] == k][:4].reset_index().drop(['index'], axis=1)
                    if(len(tmp['partcode']) >= 4 and tmp['qty'][0] >= 20) :
                        file_name = 'cate_extract\\%s_%s_%s_%s.csv' %(i, j, k, now_days)
                        tmp.to_csv(file_name, header=True, index = False, encoding='utf-8-sig')

    print('모든 추출이 완료되었습니다.')
    print("***************************************\n")


    
def ga_customer_action_data(sale_idx_list) :
    print("***************************************")
    print('Bigquery 고객 행동 데이터를 가져와 정제합니다.')
    print('......')
    
    conn = psycopg2.connect(user=hk_user, password=hk_password, host=hk_ost, port=hk_post, database=hk_database)
    cur = conn.cursor()
    
    sql_query = """
                SELECT uid_cd1 as cid, date as saledate, productsku, productclick, productdetail, productprice as actualamt
                FROM
                WHERE input_date::date >= '%s'
                and brand = 'DISCOVERY EXPEDITION'
                """ %bef_7days
    cur.execute(sql_query)
    
    conn.commit()
    conn = None

    df_ga_raw = pd.DataFrame(cur.fetchall())
    df_ga_raw.columns = [desc[0] for desc in cur.description]
    
    df_ga_raw = df_ga_raw.astype({'productclick' : 'int', 'productdetail' : 'int', 'actualamt' : 'int'})
    
    ######### 일주일동안 몇번 로그인했는지 추가(카테고리 포괄 개념)
    ######### 해당 카테고리를 몇 번 카운트했는지가 final(추출한 후에 위에꺼 마지막에 inner로 붙이고)
    df_ga_raw['cnt'] = 1
    df_ga_raw['productclick'] = df_ga_raw['productclick'] * 1.
    df_ga_raw['productdetail'] = df_ga_raw['productdetail'] * 1
    df_ga_raw['action_weight'] = df_ga_raw['productclick'] + df_ga_raw['productdetail']
    df_ga_raw = df_ga_raw.drop(['productclick', 'productdetail'], axis = 1)
    
    tmp_cid = []
    for i in df_ga_raw['cid'] :
        if(len(i) < 10) :
            tmp_cid.append(i)
    
    # 최근 7일 이내에 구매이력이 존재하는 고객은 제외
    df_ga = df_ga_raw[~df_ga_raw['cid'].isin(sale_idx_list)][df_ga_raw['cid'].isin(tmp_cid)]
    df_join = pd.merge(df_ga, df_part_ref, how='inner', on='productsku').sort_values('cid', ascending=True).reset_index().drop(['index'], axis=1)
    
    print('데이터 정제가 완료되었습니다.')
    print("***************************************\n")
    return df_join



# 고객 데이터 추출 함수
def cust_extract(df) : 
    print("***************************************")
    print('메일 수신을 허용한 고객 데이터 추출을 시작합니다.')
    print('......')
    
    conn = psycopg2.connect(user=hk_user, password=hk_password, host=hk_ost, port=hk_post, database=hk_database)
    cur = conn.cursor()

    sql_query = """
                SELECT cid__c as cid, name, recv_sms__c as snd_typ, phone_mobile__c as mobile
                FROM 
                WHERE name not in ('탈퇴고객')
                and ispersonaccount = True
                and sleep_yn__c = 'N'
                and recv_email__c = 'Y'
                and (daigong__c is null or daigong__c = 'N') 
                and cid__c in (select cid
                            from fnf.cust_brand
                            where brand in ('M', 'I', 'X'))
                """
    cur.execute(sql_query)
    conn.commit()

    df_cust_raw = pd.DataFrame(cur.fetchall())
    df_cust_raw.columns = [desc[0] for desc in cur.description]
    
    df_cust = df[df['cid'].isin(df_cust_raw['cid'].values.tolist())].reset_index().drop(['index'], axis=1)
    
    print("고객 데이터 추출을 완료하였습니다..")
    print("***************************************\n")
    return df_cust



def cust_interest_category_extract(df_cust) :
    print("***************************************")
    print('고객이 일주일동안 가장 관심이 높았던 카테고리-키즈/성인 쌍과 수치를 추출합니다.')
    print('......')
    
    df_cust.sort_values('actualamt', ascending=False).reset_index().drop(['index'], axis=1)

    df_fin_pivot = pd.pivot_table(df_cust, index = ['cid', 'ctgr', 'adult'], values = ['action_weight', 'cnt'], aggfunc = ['sum'], fill_value = 0)

    df_interest_fin = pd.DataFrame(df_fin_pivot.to_records())
    df_interest_fin = df_interest_fin.rename(columns = {'''('sum', 'action_weight')''' : 'action_cnt', '''('sum', 'cnt')''' : 'cate_cnt'})

    df_interest_fin = df_interest_fin.sort_values(['action_cnt', 'cate_cnt'], ascending=False).reset_index().drop(['index'], axis=1)
    df_interest_fin = df_interest_fin.drop_duplicates(['cid'], keep='first')
    df_interest_fin = df_interest_fin[:int(len(df_interest_fin['cid'])*0.75)].reset_index().drop(['index'], axis=1)
    
    
    ctgr_nm = []
    ctgr_cnt = []

    for i in set(df_cust['ctgr'].values.tolist()) :
        ctgr_nm.append(i)
        ctgr_cnt.append(df_cust['ctgr'].values.tolist().count(i))

    df_ctgr_fin = pd.DataFrame({'카테고리명' : ctgr_nm, '고객 수' : ctgr_cnt}).sort_values('고객 수', ascending=False).reset_index().drop(['index'], axis=1)
    
    print("추출을 완료하였습니다..")
    print("***************************************\n")
    return df_interest_fin, df_ctgr_fin
    

    
    

if __name__ == '__main__' :
    df_sale_ref, sale_idx_list = sale_extract()
    df_part_ref = part_extract()
    df_join = join_dfs(df_sale_ref, df_part_ref)
    export_csv(df_join)
    tmp01 = ga_customer_action_data(sale_idx_list)
    df_cust = cust_extract(tmp01)
    df_interest_fin, df_ctgr_fin = cust_interest_category_extract(df_cust)
    
    print(df_interest_fin)
    print(df_ctgr_fin)
    
    sys.exit("모든 작업이 완료되었습니다.")
