#!/usr/bin/env python
# coding: utf-8

#######################################################################
# Title : Data Comparing & Verifying
# Name : 박찬혁
# Create Date : 20.07.17 14:32
# Modify Date : 20.07.22 11:49
# Memo : 
#######################################################################

import glob
import os
import pandas as pd
import numpy as np
from tqdm import tnrange, tqdm
from datetime import datetime
import time
from tabulate import tabulate
import sys
import warnings
warnings.filterwarnings(action='ignore')
sys.setrecursionlimit(2000)
start = time.time()

# Brand Group
keyword_group = [[0, 'MLB'], [1, 'Discovery'], [2, 'STRATCH ANGELS'], [3, "MLB KIDS"], [4, "DUVETICA"]]

# Input Value
keyword_num = 1
keyword_year = "2020"
keyword_start_month = "1"
keyword_end_month = "7"
brand = keyword_group[keyword_num][1]

# input & output Path
input_file = 'C:\\Users\\chani\\Verifying Data\\'
output_file = 'C:\\Users\\chani\\Verifying Data\\merge\\%s_raw_merge.csv' %brand

# print User definition function for 'PRETTY'
def pprint_df(dframe):
    print(tabulate(dframe, headers='keys', tablefmt='psql', showindex=False))

# Table Merging User definition fuction
def table_merge() : 
    print("Code Execution Date " + str(datetime.now()))
    print("-----------------------------------------------------\n\n")
    allFile_list = []
    if(keyword_start_month != "") :
        for i in range(int(keyword_start_month), int(keyword_end_month)+1) : 
            keyword_path = "%s_%s_%d.csv" %(brand, keyword_year, i)
            allFile_list.append(
                glob.glob(os.path.join(input_file, keyword_path))[0])
    print("추가된 파일 리스트 : ")
    for i in allFile_list : print(i)
    print("\n\n-----------------------------------------------\n")

    allData = []
    for file in allFile_list:
        df = pd.read_csv(file, engine='python')
        allData.append(df)

    csv_merge = pd.concat(allData, axis=0, ignore_index=True)

    csv_merge.drop(csv_merge.columns[[0]], axis='columns')
    csv_merge['BRAND'] = brand
    csv_merge.to_csv(output_file, index=False, encoding='utf-8-sig')
    print("1st Merging File Path : " + output_file ) # 1차 Merging 파일을 저장

    return csv_merge
    
    
# Table Refining User definition function
def table_refine() :
    df_raw = pd.read_csv(
    output_file,
    engine='python',
    encoding='utf-8-sig')

    # Null Value Handling
    df_raw['CID'] = df_raw['CID'].replace(np.nan, '0')
    df_raw['GENDER'] = df_raw['GENDER'].replace(np.nan, '알수없음')

    # First Sale Date Join & Null Value Handling
    df_tmp = pd.read_csv("C:\\Users\\chani\\Verifying Data\\type\\최초 구매일 2020_01 이후 ERP 데이터3.csv", encoding='utf-8-sig').astype(str)
    code_list = df_raw["SHOPCODE"].values.tolist()
    df_raw = pd.merge(df_raw, df_tmp, how='left', on='CID')

    df_raw.rename(columns = {'FIRSTSALEDATE' : "FIRSALEDATE"}, inplace=True)
    df_raw['FIRSALEDATE'] = df_raw['FIRSALEDATE'].replace(np.nan, '0000-00-00')
    
    sale_date_list = df_raw['SALEDATE'].values.tolist()
    fir_sale_date_list = df_raw['FIRSALEDATE'].values.tolist()

    # Data Preprocessing for New & Old
    new_old_list = []
    for i in range(len(sale_date_list)) :
        if(str(fir_sale_date_list[i][0:4]) == "0000") :
            new_old_list.append("기존" + str(sale_date_list[i][5:7]))
        elif(str(fir_sale_date_list[i][0:4]) =="2020" and sale_date_list[i][5:7] == fir_sale_date_list[i][5:7]) :
            new_old_list.append("신규" + str(sale_date_list[i][5:7]))
        elif(str(fir_sale_date_list[i][0:4]) =="2020" and sale_date_list[i][5:7] > fir_sale_date_list[i][5:7]) :
            new_old_list.append("기존" + str(sale_date_list[i][5:7]))
        else : 
            new_old_list.append("기존" + str(sale_date_list[i][5:7]))
    df_raw["NEWOLD"] = pd.Series(new_old_list, index=df_raw.index)

    # Minus insert for MLB
    if(brand == "MLB") : 
        sale_type_list = df_raw['SALETYPE'].values.tolist()
        amt_list = df_raw['LISTAMT'].values.tolist()
        qty_list = df_raw['QTY'].values.tolist()

        for i in tnrange(len(sale_type_list), desc="Minus 처리중") : 
            if(str(sale_type_list[i]) == '2' and amt_list[i] > 0) :
                df_raw["LISTAMT"][i] = amt_list[i]*(-1)
                df_raw["QTY"][i] = qty_list[i]*(-1)
    
    tmp_title = ("C:\\Users\\chani\\Verifying Data\\merge\\%s_mg_result_%s_%s_to_%s_%s.csv"
            %(brand, keyword_year, keyword_start_month, keyword_year, keyword_end_month))

    df_refine = df_raw
    df_refine.to_csv(tmp_title, index=False, encoding='utf-8-sig')
    print("최종 Merging 파일 출력 완료 : " + tmp_title)
    
    return df_refine
    
    
# Visualizing Statistics
def data_statistics() : 
    print("\n\n------------------통계적 분석------------------\n")
    # 파일 Path 정의
    tmp_title = ("C:\\Users\\chani\\Verifying Data\\merge\\%s_mg_result_%s_%s_to_%s_%s.csv"
            %(brand, keyword_year, keyword_start_month, keyword_year, keyword_end_month))
    # 해당 Path의 파일 Read
    df_sc = pd.read_csv(tmp_title, engine='python', encoding='utf-8-sig')
    
    
    # 매출액 및 판매 개수
    print("""00. 매출액 및 판매 개수""")
    df_sale = df_sc
    df_sale = df_sale[df_sale['TYPE'] == "포함"]
    df_sale = df_sale[df_sale['AGEGROUP'] != "알수없음"]
    df_sale_01 = df_sale.groupby('BRAND').LISTAMT.sum()
    df_sale_02 = df_sale.groupby('BRAND').ACTUALAMT.sum()
    df_sale_03 = df_sale.groupby('BRAND').QTY.sum()

    print("LISTAMT SUM: " + str(df_sale_01.values[0]))
    print("ACTUALAMT SUM: " + str(int(df_sale_02.values[0])))
    print("QTY SUM: " + str(int(df_sale_03.values[0])))
    print("\n")
    
    
    # 회원 성별
    df_gender = df_sc
    df_gender = df_gender[df_gender['TYPE'] == "포함"]
    df_gender = df_gender[df_gender['GENDER'] != "알수없음"]
    df_gender = df_gender[df_gender['CID'] != "0"]
    df_gender = df_gender.drop_duplicates(['CID'], keep = 'first')
    index_list = df_gender.groupby('GENDER').sum().index.tolist()
    value_list = df_gender.groupby('GENDER').GENDER.value_counts().tolist()
    value_pc_list = []
    for i in range(len(value_list)) :
        value_pc_list.append(round(value_list[i]/sum(value_list)*100, 2))

    df_result = pd.DataFrame({'Gender' : index_list, 'Count' : value_list, 'Percentage' : value_pc_list})
    print("""01. 회원 성별(%)""")
    pprint_df(df_result)
    print("\n")
    
    
    # 신규 및 기존 회원 비중(%)
    old = 0
    new = 0
    print("""02. 신규 및 기존 회원 비중(%)""")
    print("-------------------기존-------------------")   

    for i in range(int(keyword_start_month), int(keyword_end_month)+1) :
        text = "기존0%s" %str(i)
        df_tmp = df_sc
        df_tmp = df_tmp[df_tmp['NEWOLD'] != ""]
        df_tmp = df_tmp[df_tmp['TYPE'] == "포함"]
        df_tmp = df_tmp[df_tmp['CID'] != "0"]
        df_tmp = df_tmp[df_tmp['NEWOLD'] == text]
        df_tmp = df_tmp.drop_duplicates(['CID'])
        old += len(df_tmp['NEWOLD'])
        print(text + " : " + str(len(df_tmp['NEWOLD'])))
        
    print("-------------------신규-------------------")
    for i in range(int(keyword_start_month), int(keyword_end_month)+1) : 
        text = "신규0%s" %str(i)
        df_tmp = df_sc
        df_tmp = df_tmp[df_tmp['NEWOLD'] != ""]
        df_tmp = df_tmp[df_tmp['TYPE'] == "포함"]
        df_tmp = df_tmp[df_tmp['CID'] != "0"]
        df_tmp = df_tmp[df_tmp['NEWOLD'] == text]
        df_tmp = df_tmp.drop_duplicates(['CID'])
        new += len(df_tmp['NEWOLD'])
        print(text + " : " + str(len(df_tmp['NEWOLD'])))

    print("---------------통으로 Unique------------------")   
    text = "기존"
    df_tmp = df_sc
    df_tmp = df_tmp[df_tmp['NEWOLD'] != ""]
    df_tmp = df_tmp[df_tmp['TYPE'] == "포함"]
    df_tmp = df_tmp[df_tmp['CID'] != "0"]
    df_tmp = df_tmp[df_tmp['NEWOLD'] != "신규03"]
    df_tmp = df_tmp[df_tmp['NEWOLD'] != "신규04"]
    df_tmp = df_tmp[df_tmp['NEWOLD'] != "신규05"]
    df_tmp = df_tmp.drop_duplicates(['CID'])
    print(text + " : " + str(len(df_tmp['NEWOLD'])))

    print("----------------전체-------------------")
    print("기존 전체 SUM : " + str(old))
    print("신규 전체 SUM : " + str(new))
    print("\n")

    
    # 판매 채널 별 매출 비중(%)
    print("""03. 판매 채널 별 매출 비중(%)""")
    df_channel = df_sc
    df_channel = df_channel[df_channel['TYPE'] == "포함"]
    index_list = df_channel['SHOPTYPE'].unique()
    value_list = []
    for i in index_list :
        amt = 0
        df_tmp = df_channel[df_channel['SHOPTYPE'] == i]
        tmp_list = df_tmp["ACTUALAMT"].astype(float).values.tolist()
        for j in range(len(tmp_list)) :
            amt += tmp_list[j]
        value_list.append(amt)
    value_pc_list = []
    for i in range(len(value_list)) :
        value_pc_list.append(round(value_list[i]/sum(value_list)*100, 2))
    for i in range(len(index_list)) : 
        value_list[i] = str(value_list[i])[:-2]
    df_result = pd.DataFrame({'ShopType' : index_list, 'ActualAMT' : value_list, 'Percentage' : value_pc_list})
    pprint_df(df_result)
    print("\n")
    
    
    # 회원 연령대 및 매출 비중
    print("""04. 회원 연령대 및 매출 비중(%)""")
    df_agegroup = df_sc
    df_agegroup = df_agegroup[df_agegroup['TYPE'] == "포함"]
    df_agegroup = df_agegroup[df_agegroup['AGEGROUP'] != "알수없음"]
    df_agegroup = df_agegroup.drop_duplicates(['CID'], keep = 'first')
    index_list = df_agegroup.groupby('AGEGROUP').sum().index.tolist()
    value_list = df_agegroup.groupby('AGEGROUP').AGEGROUP.value_counts().tolist()
    amt_list = df_agegroup.groupby('AGEGROUP').ACTUALAMT.sum().tolist()
    amt_list = list(map(int, amt_list))
    value_pc_list = []
    for i in range(len(value_list)) :
        value_pc_list.append(round(value_list[i]/sum(value_list)*100, 2))
    amt_pc_list = []
    for i in range(len(amt_list)) :
        amt_pc_list.append(round(amt_list[i]/sum(amt_list)*100, 2))
    df_result = pd.DataFrame({
        'AgeGroup' : index_list, 'ActualAMT' : amt_list, 'CIDCount' : value_list,
        'AMTPercentage' : amt_pc_list, 'CIDPercentage' : value_pc_list})
    fir, sec = df_result.iloc[-2].copy(), df_result.iloc[-1].copy()
    df_result.iloc[-2], df_result.iloc[-1] = sec, fir
    pprint_df(df_result)
    print("\n")

    
    # 회원 연령대 별 구매 할인율(%)
    print("""05. 회원 연령대 별 구매 할인율(%)""")
    df_dc = df_sc
    # df_dc = df_dc[df_dc['TYPE'] == "포함"]
    index_list = df_dc.groupby('AGEGROUP').sum().index.tolist()
    value_list = df_dc.groupby('AGEGROUP').DISCOUNTPCT.mean().tolist()
    for i in range(0, len(value_list)) :
        value_list[i] = round(value_list[i], 2)
    df_result = pd.DataFrame({'AgeGroup' : index_list, 'Discount' : value_list})
    df_result = df_result[df_result['AgeGroup'] != "알수없음"]
    fir, sec = df_result.iloc[-2].copy(), df_result.iloc[-1].copy()
    df_result.iloc[-2], df_result.iloc[-1] = sec, fir
    pprint_df(df_result)
    print("\n")
    
    
    # 기간별 1인 객단가(AUS)
    print("""06. 기간별 1인 객단가(AUS)""")
    df_aus = df_sc
    df_aus = df_aus[df_aus['TYPE'] == "포함"]
    df_aus = df_aus[df_aus['AGEGROUP'] != "알수없음"]
    df_aus = df_aus.drop_duplicates(['CID'], keep = 'first')
    df_aus = df_aus[df_aus.CID != '0']
    df_aus = pd.pivot_table(df_aus, index = ['AGEGROUP','GENDER'], values = ['CID'],
                            aggfunc = ['count'], fill_value = 0)

    df_aus_1 = df_sc
    df_aus_1 = df_aus_1[df_aus_1['TYPE'] == "포함"]
    df_aus_1 = df_aus_1[df_aus_1['AGEGROUP'] != "알수없음"]
    df_aus_1 = pd.pivot_table(df_aus_1, index = ['AGEGROUP','GENDER'],
                              values = ['ACTUALAMT'], aggfunc = ['sum'], fill_value = 0)

    df_aus = pd.concat([df_aus, df_aus_1], axis = 1)
    tmp = df_aus.reset_index()
    age_list = tmp[tmp.columns[0]].values.tolist()
    gender_list = tmp[tmp.columns[1]].values.tolist()
    fir_val_list = tmp[tmp.columns[2]].values.tolist()
    sec_val_list = tmp[tmp.columns[3]].values.tolist()

    aus_list = []
    for i in range(0, len(fir_val_list)) :
        aus_list.append(round(sec_val_list[i]/fir_val_list[i], 2))
    aus_list = list(map(int, np.round(aus_list, 0)))

    df_result = pd.DataFrame({'AgeGroup' : age_list, 'Gender' : gender_list,
                              'CIDCount' : fir_val_list, 'ActualAMT' : sec_val_list, 'IPT' : aus_list})
    fir, sec = df_result.iloc[-1].copy(), df_result.iloc[-3].copy()
    df_result.iloc[-1], df_result.iloc[-3] = sec , fir
    fir, sec = df_result.iloc[-2].copy(), df_result.iloc[-4].copy()
    df_result.iloc[-2], df_result.iloc[-4] = sec , fir

    pprint_df(df_result[df_result["Gender"] == "남성"])
    pprint_df(df_result[df_result["Gender"] == "여성"])
    print("\n")
    
          
    # 기간별 1인/구매 개수 평균(IPT)
    print("""07. 기간별 1인/구매 개수 평균(IPT)""")
    df_ipt = df_sc
    df_ipt = df_ipt[df_ipt['TYPE'] == "포함"]
    df_ipt = df_ipt[df_ipt['AGEGROUP'] != "알수없음"]
    df_ipt = df_ipt.drop_duplicates(['CID'], keep = 'first')
    df_ipt = df_ipt[df_ipt.CID != '0']

    df_ipt = pd.pivot_table(df_ipt, index = ['AGEGROUP','GENDER'], values = ['ACTUALAMT'],
                            aggfunc = ['count'], fill_value = 0)

    df_ipt_1 = df_sc
    df_ipt_1 = df_ipt_1[df_ipt_1['TYPE'] == "포함"]
    df_ipt_1 = df_ipt_1[df_ipt_1['AGEGROUP'] != "알수없음"]
    df_ipt_1 = pd.pivot_table(df_ipt_1, index = ['AGEGROUP','GENDER'], values = ['QTY'], aggfunc = ['sum'], fill_value = 0)

    df_ipt = pd.concat([df_ipt, df_ipt_1], axis = 1)
    tmp = df_ipt.reset_index()
    age_list = tmp[tmp.columns[0]].values.tolist()
    gender_list = tmp[tmp.columns[1]].values.tolist()
    fir_val_list = tmp[tmp.columns[2]].values.tolist()
    sec_val_list = tmp[tmp.columns[3]].values.tolist()
    ipt_list = []
    for i in range(0, len(fir_val_list)) :
        ipt_list.append(round(sec_val_list[i]/fir_val_list[i], 2))

    df_result = pd.DataFrame({'AgeGroup' : age_list, 'Gender' : gender_list, 'ActualAMT' : fir_val_list,
                              'QTY' : sec_val_list, 'IPT' : ipt_list})
    fir, sec = df_result.iloc[-1].copy(), df_result.iloc[-3].copy()
    df_result.iloc[-1], df_result.iloc[-3] = sec, fir
    fir, sec = df_result.iloc[-2].copy(), df_result.iloc[-4].copy()
    df_result.iloc[-2], df_result.iloc[-4] = sec, fir

    pprint_df(df_result[df_result["Gender"] == "남성"])
    pprint_df(df_result[df_result["Gender"] == "여성"])
    print("\n")
    
    
    # 1개 아이템 구매 평균 금액(IPS)
    print("""08. 1개 아이템 구매 평균 금액(IPS)""")
    df_ips = df_sc
    df_ips = df_ips[df_ips['TYPE'] == "포함"]
    df_ips = df_ips[df_ips['AGEGROUP'] != "알수없음"]
    df_ips = df_ips[df_ips.CID != '0']

    df_ips = pd.pivot_table(df_ips, index = ['AGEGROUP','GENDER'],
                            values = ['QTY', 'ACTUALAMT'], aggfunc = ['sum'], fill_value = 0)
    df_ips

    tmp = df_ips.reset_index()
    age_list = tmp[tmp.columns[0]].values.tolist()
    gender_list = tmp[tmp.columns[1]].values.tolist()
    fir_val_list = tmp[tmp.columns[2]].values.tolist()
    sec_val_list = tmp[tmp.columns[3]].values.tolist()

    ips_list = []
    for i in range(0, len(fir_val_list)) :
        ips_list.append(round(fir_val_list[i]/sec_val_list[i], 2))
    ips_list = list(map(int, np.round(ips_list, 0)))

    df_result = pd.DataFrame({'AgeGroup' : age_list, 'Gender' : gender_list, 'ActualAMT' : fir_val_list,
                              'QTY' : sec_val_list, 'IPS' : ips_list})
    fir, sec = df_result.iloc[-1].copy(), df_result.iloc[-3].copy()
    df_result.iloc[-1], df_result.iloc[-3] = sec, fir
    fir, sec = df_result.iloc[-2].copy(), df_result.iloc[-4].copy()
    df_result.iloc[-2], df_result.iloc[-4] = sec, fir

    pprint_df(df_result[df_result["Gender"] == "남성"])
    pprint_df(df_result[df_result["Gender"] == "여성"])
    print("\n")
    
    
    print("""09. 판매 Top 제품 및 유형 비교 분석""")
    df_item = df_sc
    df_item = df_item[df_item['TYPE'] == "포함"]
    index_list = df_item.groupby('PARTCODE').sum().index.tolist()
    value_list = df_item.groupby('PARTCODE').QTY.sum().tolist()

    df_result = pd.DataFrame({'PartCode' : index_list, 'QTY' : value_list})
    df_result = df_result.sort_values('QTY', axis=0, ascending=False).head(20)
    pprint_df(df_result.reset_index(drop=True))
    print("\n")
    
    print("""Execution Time :""", round(time.time() - start, 2), "sec\n")
    print("-----------------------------------------------\n\n")
    
    df_aus_1


if __name__ == '__main__' :
    df_merge = table_merge() #파일 Merge
    df_refine = table_refine() # 전처리
    data_statistics() # 통계 Display

