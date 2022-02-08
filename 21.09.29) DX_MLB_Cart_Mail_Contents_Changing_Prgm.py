#!/usr/bin/env python
# coding: utf-8

#######################################################################
# Title : DX & MLB Cart Mail contents Changing Prgm
# Name : 박찬혁
# Create Date : 21.09.29 17:20
# Modify Date : 21.10.25 17.46
# Memo : 
#######################################################################

import sys
import time
import os
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver 
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
import smtplib
from email import encoders
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import psycopg2
import pandas as pd
from psycopg2.errors import UniqueViolation
from sqlalchemy.exc import IntegrityError
import json
from math import floor
from datetime import date
import re


# Marketing Cloud Rest API 정보
clientid = ''
clientsecret = ''

subdomain = ''
auth_base_url = f''


options = webdriver.ChromeOptions()
hidden_browser = True
if hidden_browser:
    options.add_argument('headless')
    options.add_argument('window-size=1920x1080')
    options.add_argument("disable-gpu")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("start-maximized")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions")

capa = DesiredCapabilities.CHROME
capa["pageLoadStrategy"] = "none"
driver = webdriver.Chrome(executable_path='./chromedriver', options=options, desired_capabilities=capa)
wait = WebDriverWait(driver, 20)

today_date = datetime.today().strftime("%Y%m%d")[2:]
url_dx = "https://www.discovery-expedition.com/event/promotionList"
url_mlb = "https://www.mlb-korea.com/main/mall/view"

print("***************************************")
print("Cart Mail Contents Changing Notification")
print("***************************************\n\n")


def changing_check_dx(): 
    print("***************************************")
    print('기획전 컨텐츠의 변경 여부를 확인합니다.')
    html = requests.get(url_dx)
    bs_html = BeautifulSoup(html.content,"html.parser")

    contents_src = bs_html.find("div",{"class":"item-list04"})

    images = contents_src.findAll('img')
    img_src_list = []
    for image in images:
        img_src_list.append(image['src'])

    return img_src_list


def changing_check_mlb(): 
    print("***************************************")
    print('기획전 컨텐츠의 변경 여부를 확인합니다.')
    html = requests.get(url_mlb)
    bs_html = BeautifulSoup(html.content,"html.parser")

    contents_src = str(bs_html.find("div",{"class":"main-section quick_menu"}))
    
    d1 = re.search('https(.+?)jpg', contents_src).group(0)
    d_tmp = contents_src.replace(d1, '')
    d2  = re.search('https(.+?)jpg', d_tmp).group(0)
    
    tmp = [d1, d2]
    
    return tmp
    

def get_url_dx(img_src_list) :
    print("***************************************")
    print('랜딩 URL의 주소를 수집합니다.')
    print('...')
    fir_cts = '#contents > div.contents-type01-box03 > div.item-list04 > ul > li:nth-child(1) > div > a > img'
    sec_cts = '#contents > div.contents-type01-box03 > div.item-list04 > ul > li:nth-child(2) > div > a > img'
    chk_url_list = [fir_cts, sec_cts]
    randing_url_list = []
    
    for i in chk_url_list : 
        driver.get(url_dx)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, i)))
        driver.find_element_by_css_selector(i).click()
        time.sleep(5)
        randing_url_list.append(driver.current_url)
        
    print('랜딩 URL의 주소를 수집을 완료하였습니다.')
    print("***************************************\n")
    return img_src_list, randing_url_list


def get_url_mlb(img_src_list) :
    print("***************************************")
    print('랜딩 URL의 주소를 수집합니다.')
    print('...')
    fir_cts = '#contents > div.main-section.quick_menu > div > a:nth-child(1) > div'
    sec_cts = '#contents > div.main-section.quick_menu > div > a:nth-child(2) > div'
    chk_url_list = [fir_cts, sec_cts]
    randing_url_list = []
    
    for i in chk_url_list : 
        driver.get(url_mlb)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, i)))
        driver.find_element_by_css_selector(i).click()
        time.sleep(5)
        randing_url_list.append(driver.current_url)
        
    print('랜딩 URL의 주소를 수집을 완료하였습니다.')
    print("***************************************\n")
    return img_src_list, randing_url_list
    

    
    
def generate_access_token(clientid: str, clientsecret: str) -> str:
    headers = {'content-type': 'application/json'}
    payload = {
      'grant_type': 'client_credentials',
      'client_id': clientid,
      'client_secret': clientsecret
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
    
    
    
def insert_to_db(token, img_all_list) :
    auth_token = "Bearer " + token
       
    headers = {
        'Accept': 'application/json; charset=UTF-8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'X-Requested-With': 'XMLHttpRequest',
        'Authorization': auth_token,
        'Content-Type': 'application/json',
    }

    data = '''{"ContactKey": "",  "EventDefinitionKey":"",
    "Data":  { "cid":"",    "brand":"DISCOVERY EXPEDITION",    "img_url_01":"%s",    "img_url_02":"%s",    "landing_url_01":"%s",    "landing_url_02":"%s", "input_date" : "%s"}}''' %(img_all_list[0][0], img_all_list[0][1], img_all_list[1][0], img_all_list[1][1], date.today().isoformat())

    response = requests.post('', headers=headers, data=data)
    print(response)
    
    

def send_mail(img_src_list, img_randing_list) :
    if('static' not in img_src_list[0]) : 
        print("***************************************")
        print('Notifiaction 메일 발송을 시작합니다.')
        print('...')

        smtp = smtplib.SMTP('smtp.live.com', 587)
        smtp.starttls()
        smtp.login('', '')


        msg = MIMEMultipart()
        msg['Subject'] = '''MC 장바구니 메일 컨텐츠 교체 관련 건'''
        mail_text = """
        교체 관련 이미지가 정상적이지 않습니다. 확인을 해주세요! 

        ○ 첫번째 기획전
        이미지 URL : %s
        랜딩 URL : %s

        ○ 두번째 기획전
        이미지 URL : %s
        랜딩 URL : %s

            """ %(img_src_list[0], img_randing_list[0], img_src_list[1], img_randing_list[1])
        part = MIMEText(mail_text)
        msg.attach(part)

        recipients = ['']
        msg['To'] = ", ".join(recipients)
        smtp.sendmail('', recipients, msg.as_string())
        smtp.quit()

        print('Notification 메일 발송을 완료하였습니다.')
        print("***************************************\n")

        sys.exit('잘못된 주소가 수집되었습니다.')
    
    
if __name__ =='__main__' :
    img_all_list_dx = get_url_dx(changing_check_dx())
    img_all_list_mlb = get_url_mlb(changing_check_mlb())
    send_mail(img_all_list_dx[0], img_all_list_dx[1])
    send_mail(img_all_list_mlb[0], img_all_list_mlb[1])
    token = generate_access_token(clientid, clientsecret)
    insert_to_db(token, img_all_list_dx)
    insert_to_db(token, img_all_list_mlb)
    driver.close()
    sys.exit('전체 프로세스가 완료 되었습니다.')