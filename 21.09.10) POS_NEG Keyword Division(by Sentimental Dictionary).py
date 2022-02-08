#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#######################################################################
# Title : CSV Merging & Sentence Tokenize
# Name : 박찬혁
# Create Date : 21.09.10 15:52
# Modify Date : 22.02.08 13:16
# Memo : 
#######################################################################
import pandas as pd
import glob
import os
import nltk
from nltk.tokenize import word_tokenize
import re
from nltk.stem.snowball import SnowballStemmer
from tqdm import tnrange, tqdm


def load_raw_data() :
    tmp_file_name = 'nltk_keyword_extract.csv'
    df_tmp_res = pd.read_csv(tmp_file_name, encoding='utf-8-sig')

    edge_raw_list = []

    for i in df_tmp_res['all_list'] :
        tmp = []
        if (i.count(' ') > 1) :
            tmp.append(i.replace(',', '').replace('，', '').replace('[', '').replace(']', '').replace('''\'''', ''))
            edge_raw_list.append(" ".join(tmp))
        elif (i.count(' ') == 0): 
            edge_raw_list.append(i.replace(',', '').replace('，', '').replace('[', '').replace(']', '').replace('''\'''', ''))
            
            
    return edge_raw_list

            
    
def load_word_senti_score() :
    senti_file_name = 'SentiWordNet_3.0.0.csv'

    # csv 파일 Load
    df_senti_raw = pd.read_csv(senti_file_name, encoding='utf-8-sig')
    df_senti_raw_tmp = df_senti_raw.reset_index().drop(['index', 'POS'], axis=1)


    word_list = []
    pos_score_list = []
    neg_score_list = []


    for i in tnrange(0, len(df_senti_raw_tmp['SynsetTerms'].values.tolist())) :
        word_cnt = len(df_senti_raw_tmp['SynsetTerms'].values.tolist()[i].split())
        word_cp = df_senti_raw_tmp['SynsetTerms'].values.tolist()[i].split()
        for j in range(0, word_cnt) :
            pos_score_list.append(df_senti_raw_tmp['PosScore'].values.tolist()[i])
            neg_score_list.append(df_senti_raw_tmp['NegScore'].values.tolist()[i])
            word_list.append(word_cp[j].split('#')[0].replace('_', '').replace('-', ''))

    df_word_result = pd.DataFrame({'Keyword' : word_list, 'PosScore' : pos_score_list, 'NegScore' : neg_score_list})
    
    return df_word_result



def make_senti_dict(df_word_result) :
    df_keyword_pivot = pd.pivot_table(df_word_result, index = ['Keyword'], values = ['PosScore', 'NegScore'], aggfunc = ['sum'], fill_value = 0)
    tmp = df_keyword_pivot.index.values.tolist()

    keyword = []

    for i in tmp :
        keyword.append(i)

    pos_score = df_keyword_pivot[df_keyword_pivot.columns[0]].values.tolist()
    neg_score = df_keyword_pivot[df_keyword_pivot.columns[1]].values.tolist()

    pos_word_dict = dict(zip(keyword, pos_score))
    neg_word_dict = dict(zip(keyword, neg_score))
    
    return (pos_word_dict, neg_word_dict)



def extract_senti_score(pos_word_dict, neg_word_dict) :
    pos_source_list = []
    pos_target_list = []

    neg_source_list = []
    neg_target_list = []

    err_cnt = 0

    for i in tnrange(len(edge_raw_list), desc = 'Network Dataset Extract') :
        list_val = edge_raw_list[i]
        if(len(edge_raw_list[i]) > 1) : 
            for j in range(0, len(list_val.split())) :
                k = j
                while(k+1 != len(list_val.split())) :
                    k = k+1
                    try :
                        pos_source_name = list_val.split()[j]
                        pos_target_name = list_val.split()[k]

                        if(pos_word_dict[pos_source_name] > 0 and pos_word_dict[pos_target_name] > 0) :
                            pos_source_list.append(pos_source_name)
                            pos_target_list.append(pos_target_name)
                        elif(neg_word_dict[neg_source_name] < 0 and neg_word_dict[neg_target_name] < 0) :
                            neg_source_list.append(neg_source_name)
                            neg_target_list.append(neg_target_name)
                    except :
                        err_cnt += 1

    pos_label_list = list(set(pos_source_list + pos_target_list))
    neg_label_list = list(set(neg_source_list + neg_target_list))

    df_pos_result = pd.DataFrame({'Source' : pos_source_list, 'Target' : pos_target_list})
    df_neg_result = pd.DataFrame({'Source' : neg_source_list, 'Target' : neg_target_list})
    df_pos_label = pd.DataFrame({'Id' : pos_label_list, 'Label' : pos_label_list})
    df_neg_label = pd.DataFrame({'Id' : neg_label_list, 'Label' : neg_label_list})


    df_pos_result.to_csv('all_pos_edge.csv', index=False, encoding='utf-8-sig')
    df_neg_result.to_csv('all_neg_edge.csv', index=False, encoding='utf-8-sig')
    df_pos_label.to_csv('all_pos_node.csv', index=False, encoding='utf-8-sig')
    df_neg_label.to_csv('all_neg_node.csv', index=False, encoding='utf-8-sig')

    return (df_pos_result, df_neg_result)
    
    
    
def make_src_tgt(df_result, flag) :
    if(flag == 0) :
        flag_name = 'pos'
    else : 
        flag_name = 'neg'
        
    df_result['Weight'] = 1
    df_piv = pd.pivot_table(df_result, index = ['Source', 'Target'], values = ['Weight'], aggfunc = ['sum'], fill_value = 0)
    
    df_con = pd.DataFrame({'Source' : df_piv.index[0]})
      
    src_list = []
    tar_list = []

    for j in df_piv.index.values.tolist() :
        src_list.append(j[0])
        tar_list.append(j[1])

    df_con = pd.DataFrame({'Source' : src_list,
                            'Target' : tar_list,
                            'Weight' : df_piv[df_piv.columns[0]].values.tolist()})
    df_done = df_con.sort_values(["Weight"], ascending=[False]).reset_index().drop('index', axis=1)[:10001]
    
    tmp_file_path = 'edge_weight_top10k_%s_golf.csv' %flag_name
    df_done.to_csv(tmp_file_path, index=False, encoding='utf-8-sig')
    
    df_set_list = list(set(df_done['Source'].tolist() + df_done['Target'].tolist()))
    df_set = pd.DataFrame({'Id' : df_set_list, 'Label' : df_set_list})
    df_set
    tmp_file_path = 'Label_top10k_%s_golf.csv' %flag_name
    df_set.to_csv(tmp_file_path, index=False, encoding='utf-8-sig')
    
    
    


if __name__ == '__main__' :
    edge_raw_list = load_raw_data()
    senti_word_raw_df = load_word_senti_score()
    pos_word_dict, neg_word_dict = make_senti_dict(senti_word_raw_df)
    df_pos_result, df_neg_result = extract_senti_score(pos_word_dict, neg_word_dict)
    make_src_tgt(df_pos_result, 0)
    make_src_tgt(df_neg_result, 1)

