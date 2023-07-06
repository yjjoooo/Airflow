import requests
import pandas as pd
import logging
import traceback

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

''' 전역 변수 '''
API_SERVER_IP = Variable.get('API Server IP')
API_SERVER_PORT = Variable.get('API Server Port')

''' Python 함수 '''
# 유저 테이블 데이터 삭제
def delete_user_info():
    try:
        # call_api('del', 'http://{}:{}/userinfo/user/entries'.format(API_SERVER_IP, API_SERVER_PORT))
        logging.info('Delete User Info')
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Delete User Info Error')
        logging.error(traceback.format_exc())

# 유저 테이블 데이터 수집
def collect_user_info():
    try:
        # call_api('get', 'http://{}:{}/userinfo/user/entries'.format(API_SERVER_IP, API_SERVER_PORT))
        logging.info('Collect User Info')
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Collect User Info Error')
        logging.error(traceback.format_exc())

# 유저 상세 테이블 데이터 수집
def collect_user_detail():
    try:
        # call_api('get', 'http://{}:{}/userinfo/user/detail'.format(API_SERVER_IP, API_SERVER_PORT))
        logging.info('Collect User Detail')
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Collect User Detail Error')
        logging.error(traceback.format_exc())

# API 호출 함수
def call_api(method, access_url, body = None):
    try:
        if method == 'get':
            response = requests.get(url = access_url)
            logging.info('Call API "GET" \"{}\"'.format(access_url))
        elif method == 'post':
            response = requests.post(url = access_url, data = body)
            logging.info('Call API "POST" \"{}\"'.format(access_url))
        elif method == 'del':
            response = requests.delete(url = access_url)
            logging.info('Call API "DELETE" \"{}\"'.format(access_url))
        
        return response
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Call API Error')
        logging.error(traceback.format_exc())

''' DAG 설정 '''
default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2023, 6, 11),
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 5),
}

dag = DAG(
    'collect_user',
    default_args = default_args,
    # schedule_interval = '@daily'
    schedule_interval = '0 0 * * 5',
    on_failure_callback = delete_user_info,
)

''' 
Task 설정
Python Operator 활용 Python 함수 실행
'''
# 1. 유저 테이블 데이터 삭제
task_delete_user_info = PythonOperator(
    task_id = 'delete_user_info',
    python_callable = delete_user_info,
    dag = dag
)

# 2. 유저 테이블 데이터 수집
task_collect_user_info = PythonOperator(
    task_id = 'collect_user_info',
    python_callable = collect_user_info,
    dag = dag
)

# 3. 유저 상세 테이블 데이터 수집
task_collect_user_detail = PythonOperator(
    task_id = 'collect_user_detail',
    python_callable = collect_user_detail,
    dag = dag
)

''' Task 간 Flow 설정 '''
task_delete_user_info >> task_collect_user_info
task_collect_user_info >> task_collect_user_detail