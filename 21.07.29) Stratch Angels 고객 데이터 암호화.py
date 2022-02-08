#######################################################################
# Title : CAFE24 고객 데이터 크롤링 및 DB 적재 Prgm
# Name : 박찬혁
# Create Date : 21.07.29 14:58
# Modify Date : 21.08.04 15:35
# Memo : 
#######################################################################

import pandas as pd
import os
import time
import re
import glob
from datetime import date
from hashlib import blake2b


def data_refine() : 
    print("***************************************")
    print("고객 데이터를 불러오고 정제합니다.")
    
    input_file = os.listdir(os.getcwd()+'\\Input_Data\\')
    file_name = os.getcwd() + '\\Input_Data\\' + input_file[0]
    df_ref = pd.read_csv(file_name)
    df_ref = pd.DataFrame(df_ref , columns=['아이디', '나이', '생년월일', '성별'])
    
    df_ref = df_ref.fillna('undefined')
    
    df_ref['생년월일'] = list(map(str, df_ref['생년월일']))
    
    for i in range (0, len(df_ref['생년월일'].tolist())) :
        if df_ref['생년월일'][i] != 'undefined' :
            df_ref['생년월일'][i] = df_ref['생년월일'][i][:4]
        
    

    print('\n데이터 정제를 완료하였습니다.')
    print("***************************************\n")
    
    return df_ref
    
def enc_id_generate(df) :   
    print("***************************************")
    print("사용자 ID의 암호화를 시작합니다.")
    print('...')

    id_list = list(df['아이디'])

    
    enc_id_list = []
    for i in id_list :
        tmp = bytearray(i, 'utf-8')
        h = blake2b(digest_size = 10)
        h.update(tmp)
        enc_id_list.append(h.hexdigest())

    df['ga:dimension1'] = enc_id_list
    df = pd.DataFrame(df, columns=['ga:dimension1', '나이', '생년월일', '성별'])
    df.columns = ['ga:dimension1', 'ga:dimension3', 'ga:dimension4', 'ga:dimension5']
    
    
    print('암호화가 완료되었습니다.')
    print("***************************************\n")
    
    return df


def load_account_to_csv(df_enc) : 
    today_date = date.today().isoformat()
    file_name = "SA회원정보(enc_id)_%s.csv" %today_date
    df_enc.to_csv(file_name, index=False, encoding='utf-8-sig')
    
    input_file = os.listdir(os.getcwd()+'\\Input_Data\\')
    file_name = os.getcwd() + '\\Input_Data\\' + input_file[0]
    os.remove(file_name)
    
    print('고객 데이터 생성이 완료되었습니다.')
    print("***************************************\n")
    print(df_enc)


if __name__ =='__main__' :
    df = data_refine()
    df_enc = enc_id_generate(df)
    load_account_to_csv(df_enc)
    time.sleep(5)