#!/usr/bin/env python
# coding: utf-8

# In[1]:


#######################################################################
# Title : Social Studio Crawling By API
# Name : 박찬혁
# Create Date : 21.08.24 11:04
# Modify Date : 22.02.08 09:20
# Memo : Code refactoring 필요
#######################################################################

# pip install simplejson
import simplejson as json
import requests
from datetime import datetime, date, timedelta, time
import datetime
import pandas as pd
from nltk.stem import PorterStemmer
import multiprocessing
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
from konlpy.utils import pprint
from collections import Counter
import warnings
warnings.filterwarnings(action='ignore')


id_list = [""]
keyword_dic = {"" : "golf"
}
leap_year = { 1 : 31, 2 : 28, 3 : 31, 4 : 30, 5 : 31, 6 : 30,
             7 : 31, 8 : 31, 9 : 30, 10 : 31, 11 : 30, 12 : 31}
year_list= [2020, 2021]
split_hour = [[9, 10, 0], [11, 12, 0], [13, 14, 0], [15, 16, 0], [17, 18, 0],
              [19, 20, 0], [21, 23, 0], [0, 2, 0], [3, 4, 0], [5, 6, 0], [7, 8, 0]]

client_id = ''
client_secret = ''
username = ''
password =  ''

# tokenizing 관련 정적변수
stop_words = []


# API 호출 과정에서 1회 호출 시, 1천개 이상의 포스트를 수집할 경우 에러가 발생하여 일자 및 시간 별로 나누어 수집을 호출하였고
# 물리적인 데이터의 양이 많아 반복 호출 과정에서 죽어버리는 케이스가 확인되어 각 호출별로 로컬로 떨군 후에 merging 진행
def socialstudio_api_crawling() : 
    for b_id in id_list :
        for year in year_list :
            for month in range(1, 12) :
                data = {'grant_type': 'password', 
                        'client_id': client_id, 
                        'client_secret': client_secret, 
                        'username': username, 
                        'password': password}
                result = requests.post('https://api.socialstudio.radian6.com/oauth/token', data=data)

                load = json.loads(result.content)
                key = load["access_token"]

                for day in range(1, leap_year[month]+1) : 
                    for hour in range(0, len(split_hour)) :
                        headers = {"access_token": key}
                        if(split_hour[hour][0] > 8) : 
                            startDate = str(int(datetime.datetime(year,month,day,split_hour[hour][0],0, 0).timestamp())*1000)
                            endDate = str(int(datetime.datetime(year,month,day,split_hour[hour][1],59, 59).timestamp())*1000)
                        elif(month == 12 and day == leap_year[month] and split_hour[hour][0] == 0) :
                            startDate = str(int(datetime.datetime(year+1,month+1,1,split_hour[hour][0],0, 0).timestamp())*1000)
                            endDate = str(int(datetime.datetime(year+1,1,1,split_hour[hour][1],59, 59).timestamp())*1000)
                        elif(month == 12 and day == leap_year[month] and split_hour[hour][0] > 0 and split_hour[hour][1] < 9) :
                            startDate = str(int(datetime.datetime(year+1,month+1,1,split_hour[hour][0],0, 0).timestamp())*1000)
                            endDate = str(int(datetime.datetime(year+1,1,1,split_hour[hour][1],59, 59).timestamp())*1000)
                        elif(day == leap_year[month] and split_hour[hour][0] == 0) :
                            startDate = str(int(datetime.datetime(year,month+1,1,split_hour[hour][0],0, 0).timestamp())*1000)
                            endDate = str(int(datetime.datetime(year,month+1,1,split_hour[hour][1],59, 59).timestamp())*1000)
                        elif(day == leap_year[month] and split_hour[hour][0] > 0 and split_hour[hour][1] < 9) :
                            startDate = str(int(datetime.datetime(year,month+1,1,split_hour[hour][0],0, 0).timestamp())*1000)
                            endDate = str(int(datetime.datetime(year,month+1,1,split_hour[hour][1],59, 59).timestamp())*1000)
                        elif(split_hour[hour][0] < 8) : 
                            startDate = str(int(datetime.datetime(year,month,day+1,split_hour[hour][0],0, 0).timestamp())*1000)
                            endDate = str(int(datetime.datetime(year,month,day+1,split_hour[hour][1],59, 59).timestamp())*1000)


                        url = 'https://api.socialstudio.radian6.com/v3/posts?topics=1922623&startDate=' + startDate + '&endDate=' + endDate + '&limit=1000&keywordGroups=%s' %b_id
                        resp = requests.get(url, headers=headers)
                        topic_data = json.loads(resp.content)


                        df = pd.DataFrame.from_dict(topic_data['data'], orient='columns')
                        df_result = df
                        if(df_result.empty != True) : 
                            # 컬럼 전처리(source, medoiaProvider, author)
                            tmp_exLink = df_result['source']
                            tmp_author = df_result['author']
                            tmp_date = df_result['publishedDate']

                            for i in range(0, len(tmp_exLink)) :
                                tmp_exLink[i] = tmp_exLink[i]['externalLink']
                                tmp_author[i] = tmp_author[i]['authorFullName']
                                tmp_date[i] = tmp_date[i]


                            df_result['mediaProvider'] = tmp_exLink
                            df_result['author'] = tmp_author 
                            df_result['publishedDate'] = tmp_date

                            df_result = df[['publishedDate', 'title',
                                            'content', 'externalLink','mediaProvider' ,'author']]
                            text = "GOLF_ETL\\%s\\%s_" %(keyword_dic[b_id], keyword_dic[b_id]) + str(year) + "년_%d월_%d일_%d.csv" %(month, day, hour)
                            df_result.to_csv(text, encoding='utf-8-sig')
                            print(keyword_dic[b_id] + text[13:] + "출력 완료 \n")
        
    print("*******전체 작업 완료*******")
    

def cleansing_data() :
    input_path = '2021_01'
    input_file = 'GOLF_ETL\\golf\\%s\\' %input_path
    output_file = 'GOLF_ETL\\merge\\%s_merge.csv' %input_path

    # csv 파일 Load
    df_res = pd.read_csv(output_file, encoding='utf-8-sig')
    df_res

    # content(내용)가 null인 record Drop
    df_def_tmp = df_res.fillna('a')
    df_def = df_def_tmp[df_def_tmp['content'] != 'a']
    
    snow_stemmer = SnowballStemmer(language = 'english')
    token_list = []

    allnoun_list = []
    alladverb_list = []
    allverb_list = []

    print('nltk로 tokenize하여 단어 추출')
    for i in tnrange(len(df_def['content'].tolist()), desc='nltk로 tokenize하여 단어 추출') :
        if(len(i) > 1) : 
            repl = ''

            pattern = '(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
            text_u_re = re.sub(pattern=pattern, repl=repl, string = df_def['content'].tolist()[i])

            pattern = '(watch).[a-zA-Z0-9_.+-]+'
            text_w_re = re.sub(pattern=pattern, repl=repl, string = text_u_re)    

            pattern = 'www\.youtube\.com/\S+|#\S+'
            text_y_re = re.sub(pattern=pattern, repl=repl, string = text_w_re)
            pattern = 'youtube/\S+|#\S+'
            text_y_re_01 = re.sub(pattern=pattern, repl=repl, string = text_y_re)

            pattern = '([a-zA-Z0-9_.+-]@[a-zA-Z0-9_.+-]+\.[a-zA-Z0-9_.+-]+)'
            text_e_re = re.sub(pattern=pattern, repl=repl, string = text_y_re_01)

            bmp = re.compile("["u"\U00010000-\U0010FFFF""]+", flags=re.UNICODE)
            clean_text_mark = bmp.sub(r'', text_y_re_01)

            clean_text_tmp = re.sub('''[-=+,#/\?^$.@*\"※“”~&%•ㆍ!』\\‘’|\(\)\[\]\<\>`\'…》]''', '', clean_text_mark).lower()

            clean_text = re.sub('[^a-zA-Z0-9]',' ',clean_text_tmp).strip()

            tokens = nltk.word_tokenize(clean_text)
            tagged = nltk.pos_tag(tokens)
            
            allnoun = [word for word, pos in tagged if pos in ['NN'] and len(word) > 1]
            allverb = [word for word, pos in tagged if pos in ['VB', 'VBZ', 'VBD'] and word not in stop_words]

            stem_list = []
            for w in allverb :
                x = snow_stemmer.stem(w)
                stem_list.append(x)
            allverb_list.append(stem_list)

            alladverb = [word for word, pos in tagged if pos in ['RB', 'RBR', 'RBS', 'RP']]
            allnoun_list.append(allnoun)
            alladverb_list.append(alladverb)


    print('채널 추출')
    channel = []
    for i in df_def['mediaProvider'].tolist() :
        if('twitter' in i) :
            channel.append('twitter')
        elif('youtube' in i) :
            channel.append('youtube')
        elif('forum' in i) :
            channel.append('forum')
        elif('blog' in i) :
            channel.append('blog')
        else :
            channel.append('etc')


    print('브랜드 추출')
    brand = []
    for i in allnoun_list :
        if('taylormade' in i) :
            brand.append('taylormade')
        elif('titleist' in i) :
            brand.append('titleist')
        elif('callaway' in i) :
            brand.append('callaway')
        else :
            brand.append('etc')
    
    
    df_result = pd.DataFrame({'date' : df_res['publishedDate'], 'channel' : channel, 'brand' : brand, 'content' : df_def['content'], 'allnoun' : allnoun_list, 'allverb' : allverb_list, 'alladverb' : alladverb_list})

    for i in tnrange(len(df_result["date"]), desc = '일자 변환') :
        df_result["date"][i] = df_result["date"][i][:7]

    all_con_list = []
    for i in df_result['content'] : 
        all_con_list.append(i.replace(',', '').replace('，', ''))

    all_verb_list = []
    for i in df_result['allverb'] :
        tmp =[]
        if (len(i) > 1) :
            for j in range (0, len(i)) :
                tmp.append(i[j].replace(',', '').replace('，', ''))

            all_verb_list.append(" ".join(tmp))
        elif (len(i) == 1): 
            all_verb_list.append(i[0].replace(',', '').replace('，', ''))
        else :
            all_verb_list.append("")

    all_noun_list = []
    for i in df_result['allnoun'] :
        tmp =[]
        if (len(i) > 1) :
            for j in range (0, len(i)) :
                tmp.append(i[j].replace(',', '').replace('，', ''))

            all_noun_list.append(" ".join(tmp))
        elif (len(i) == 1): 
            all_noun_list.append(i[0].replace(',', '').replace('，', ''))
        else :
            all_noun_list.append("")

    all_adverb_list = []
    for i in df_result['alladverb'] :
        tmp =[]
        if (len(i) > 1) :
            for j in range (0, len(i)) :
                tmp.append(i[j].replace(',', '').replace('，', ''))

            all_adverb_list.append(" ".join(tmp))
        elif (len(i) == 1): 
            all_adverb_list.append(i[0].replace(',', '').replace('，', ''))
        else :
            all_adverb_list.append("")


    df_result['content'] = all_con_list
    df_result['allnoun'] = all_noun_list
    df_result['allverb'] = all_verb_list
    df_result['alladverb'] = all_adverb_list

    return df_result



def src_tgt_extract(df_result) : 
    content_join_tm = " ".join((map(str,df_result[df_result['brand']=='taylormade']['allnoun'].values.tolist()))).replace('[', '').replace(']', '').replace(',', '').replace('\'', '').lower().split()
    word_counter = Counter(content_join_tm)
    df_kw_tm_res = pd.DataFrame.from_dict(word_counter, orient='index').reset_index()
    df_kw_tm_res['brand'] = 'taylormade'


    content_join_tl = " ".join((map(str,df_result[df_result['brand']=='titleist']['allnoun'].values.tolist()))).replace('[', '').replace(']', '').replace(',', '').replace('\'', '').lower().split()
    word_counter = Counter(content_join_tl)
    df_kw_tl_res = pd.DataFrame.from_dict(word_counter, orient='index').reset_index()
    df_kw_tl_res['brand'] = 'titleist'

    content_join_ca = " ".join((map(str,df_result[df_result['brand']=='callaway']['allnoun'].values.tolist()))).replace('[', '').replace(']', '').replace(',', '').replace('\'', '').lower().split()
    word_counter = Counter(content_join_ca)
    df_kw_ca_res = pd.DataFrame.from_dict(word_counter, orient='index').reset_index()
    df_kw_ca_res['brand'] = 'callaway'

    content_join_etc = " ".join((map(str,df_result[df_result['brand']=='etc']['allnoun'].values.tolist()))).replace('[', '').replace(']', '').replace(',', '').replace('\'', '').lower().split()
    word_counter = Counter(content_join_etc)
    df_kw_etc_res = pd.DataFrame.from_dict(word_counter, orient='index').reset_index()
    df_kw_etc_res['brand'] = 'etc'

    csv_merge = pd.concat([df_kw_tm_res, df_kw_tl_res, df_kw_ca_res, df_kw_etc_res], axis=0, ignore_index=True)
    csv_merge

    output_file_res = 'GOLF_ETL\\%s_keyword_Noun.csv' %input_path
    csv_merge.to_csv(output_file_res, index=False, encoding='utf-8-sig')

    return csv_merge


    
if __name__ == '__main__' :
    socialstudio_api_crawling()
    tokenizing_df = cleansing_data()
    fin_df = src_tgt_extract(tokenizing_df)
