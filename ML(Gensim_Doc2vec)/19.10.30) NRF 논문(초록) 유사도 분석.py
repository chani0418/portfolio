#!/usr/bin/env python
# coding: utf-8


###################################################################################
# Title : 초록 단어로 구성된 명사 리스트 생성(Doc2Vec 사용)
# Name : 박찬혁
# Create Date : 2019.10.30 11:30
# Modify Date : 2019.11.06 10:57
# Memo : 평균 코드 실행 시간 : 140sec
###################################################################################

# pip install scikit-learn
# pip install konlpy
# pip install --upgrade pip
# pip install JPype1‑0.6.3‑cp35‑cp35m‑win_amd64.whl

from konlpy.tag import Kkma
import pandas as pd
from pandas import Series, DataFrame
from gensim.utils import simple_preprocess
import time
start = time.time()

def make_doclist() :
    for paper in paper_content :
        # 각각을 명사로 쪼개고 쪼갠 결과를 공백(' ')으로 연결하여 mydoclist 리스트 변수에 appending
        kkma_nouns = ' '.join(kkma.nouns(paper))
        mydoclist_kkma.append(kkma_nouns)
    
# 파일을 가져와 dataframe화
df = pd.read_excel('', sheet_name = 'Sheet1')

# 가공하기 쉽도록 논문ID와 초록을 리스트형태로 저장
paper_id = df['논문ID'].values.tolist()
paper_title = df['논문한글명'].values.tolist()
paper_content = df['초록(한글)'].values.tolist()
        
# 객체 생성
# Kkma - 세종 말뭉치를 이용해 생성된 사전 (꼬꼬마)
# Twitter(Okt) - 오픈소스 한글 형태소 분석기
kkma = Kkma()

# 각각의 초록에서 추출된 명사들을 담는 리스트 생성
mydoclist_kkma = []

# 함수 실행
make_doclist()
print("형태소 생성 완료. 소요 시간 :", round(time.time() - start), "sec\n")




###################################################################################
# Title : KorWiki 데이터 기반으로 Dec2Vec 모델 생성
# Name : 박찬혁
# Create Date : 2019.10.30 11:30
# Modify Date : 2019.11.06 10:57
# Memo : 학습시키는 txt 파일이 500MB를 넘는 상당히 거대한 파일이기에
#        학습 완료까지 상당히 오랜 시간이 소요되는 것을 보여줌.
###################################################################################

*********실행시 모델 생성까지 약 70분 소요. 실행 필요시 해당 라인 주석 처리*********

#-*- coding: utf-8 -*-
import gensim
from gensim.models import doc2vec
from gensim.models.doc2vec import Doc2Vec
import sys
import imp
import multiprocessing
import time
start = time.time()

imp.reload(sys)

cores = multiprocessing.cpu_count()

#doc2vec parameters
vector_size = 300
window_size = 15
word_min_count = 2
sampling_threshold = 1e-5
negative_size = 5
train_epoch = 100
dm = 1 #0 = dbow; 1 = dmpv
worker_count = cores #number of parallel processes

#실행 폴더에 wiki_pos_tokenizer_without_taginfo.txt가 있어야함 또는 경로 지정 필수
inputfile = "./wiki_pos_tokenizer_without_taginfo.txt"
modelfile = "./doc2vec.model"

word2vec_file = modelfile + ".word2vec_format"

sentences=doc2vec.TaggedLineDocument(inputfile)

#build voca 
doc_vectorizer = doc2vec.Doc2Vec(min_count=word_min_count, size=vector_size, alpha=0.025, min_alpha=0.025, seed=1234, workers=worker_count, iter=20)
doc_vectorizer.build_vocab(sentences)

# 학습 실행 코드
doc_vectorizer.train(sentences, epochs = doc_vectorizer.iter, total_examples=doc_vectorizer.corpus_count)

# To save
doc_vectorizer.save(modelfile)
doc_vectorizer.save_word2vec_format(word2vec_file, binary=False)

print("모델 학습 완료. 소요 시간 :", round(time.time() - start), "sec\n")

doc_vectorizer = gensim.models.Doc2Vec.load("./doc2vec.model")

print("모델 Load 완료.\n")




###################################################################################
# Title : 초록간의 유사도 확인을 위한 코드(Doc2Vec 사용)
# Name : 박찬혁
# Create Date : 2019.11.05 16:20
# Modify Date : 2019.11.06 11:27
# Memo : doc_vectorizer.docvecs.similarity_unseen_docs 함수는 코사인 유사성을 계산합니다.
#        하지만 학습을 진행한 후에 코사인 유사성을 계산하므로 학습을 수행하지 않은 값과
#        다를 수 있습니다.
###################################################################################

# chk_paper_id에 비교를 원하는 논문ID를 입력
chk_paper_id = 'ART002100789'
chk_paper_num = paper_id.index(chk_paper_id)
cp_paper = mydoclist_kkma[chk_paper_num].split(" ")

print('비교 대상 논문ID : ' + chk_paper_id)
print('\n')

# 출력데이터를 담을 데이터프레임 생성
df_result_kkma = DataFrame({})
df_result_kkma["논문ID"] = ''
df_result_kkma["논문한글명"] = ''
df_result_kkma["유사도"] = ''


print('******Doc2Vec 유사도 분석 결과******\n')
for i in range (0, len(paper_id)) :
    score = doc_vectorizer.docvecs.similarity_unseen_docs(doc_vectorizer,cp_paper, mydoclist_kkma[i].split(" "))
    df_val = pd.Series([paper_id[i],  paper_title[i], score],index = ['논문ID', '논문한글명', '유사도'])
    df_result_kkma = df_result_kkma.append(df_val, ignore_index=True)
df_result_kkma = df_result_kkma.sort_values(by='유사도', ascending=False)
df_result_kkma = df_result_kkma.drop(chk_paper_num, 0)
print(df_result_kkma)
df_result_kkma.to_excel('Paper_Similarity_Result_kkma_Doc2Vec.xlsx', sheet_name='Result')
