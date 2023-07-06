import requests
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
# 매치 리스트 테이블 데이터 수집
def collect_match_list(**context):
    try:
        body = {
            'startTime' : '',
            'endTime' : ''
        }
        
        # call_api('post', 'http://{}:{}/match/list'.format(API_SERVER_IP, API_SERVER_PORT), body)
        logging.info('Collect Match List')
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Collect Match List Error')
        logging.error(traceback.format_exc())

# 매치 상세 테이블 데이터 수집
def collect_match_detail():
    try:
        # call_api('get', 'http://{}:{}/match/detail'.format(API_SERVER_IP, API_SERVER_PORT))
        logging.info('Collect Match Detail')
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Collect Match Detail Error')
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
    'collect_match',
    default_args = default_args,
    # schedule_interval = '@daily'
    schedule_interval = '0 0 * * 5'
)

''' 
Task 설정
Python Operator 활용 Python 함수 실행
'''
# 1. 매치 리스트 테이블 데이터 수집
task_collect_match_list = PythonOperator(
    task_id = 'collect_match_list',
    python_callable = collect_match_list,
    dag = dag
)

# 2. 매치 상세 테이블 데이터 수집
task_collect_match_detail = PythonOperator(
    task_id = 'collect_match_detail',
    python_callable = collect_match_detail,
    dag = dag
)

''' Task 간 Flow 설정 '''
task_collect_match_list >> task_collect_match_detail