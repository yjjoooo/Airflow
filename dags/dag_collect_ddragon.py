import requests
import logging
import traceback

from elasticsearch import Elasticsearch

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

''' 전역 변수 '''
API_SERVER_IP = Variable.get('API Server IP')
API_SERVER_PORT = Variable.get('API Server Port')
ES_SERVER_IP = Variable.get('ES Server IP')
ES_SERVER_PORT = Variable.get('ES Server Port')
# ES_ID = Variable.get('ES ID')
# ES_PASSWORD = Variable.get('ES Password')

''' Python 함수 '''
# 버전 업데이트
def update_version():
    try:
        call_api('get', 'http://{}:{}/ddragon/version'.format(API_SERVER_IP, API_SERVER_PORT))
        logging.info('Update Version')
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Get Version Error')
        logging.error(traceback.format_exc())
        
# 현재 버전 조회
def get_version(**context):
    try:
        response = call_api('get', 'http://{}:{}/ddragon/version/current'.format(API_SERVER_IP, API_SERVER_PORT))
        version = response.json()['data']['version']
        
        logging.info('Get Version \"{}\"'.format(version))
        context['ti'].xcom_push(key = 'Current Version', value = version)
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Get Version Error')
        logging.error(traceback.format_exc())

# 데이터 수집 (챔피언/아이템/룬/스펠)
def collect_data(component, **context):
    try:
        # 인덱스 검색
        current_version = context['ti'].xcom_pull(key = 'Current Version')
        index_name = '{}-{}'.format(component, current_version)

        es = Elasticsearch(['{}:{}'.format(ES_SERVER_IP, ES_SERVER_PORT)])
        index_text = es.cat.indices()

        # 검색 O
        if index_name in index_text:
            # 수집 중단
            logging.info('Index \"{}\" Already Exists'.format(index_name))
        # 검색 X
        else:            
            # 데이터 수집
            logging.info('Collect Data \"{}\"'.format(index_name))
            call_api('get', 'http://{}:{}/ddragon/{}'.format(API_SERVER_IP, API_SERVER_PORT, component))
            
            return '{} 수집 완료'.format(index_name)
    except Warning as w:
        logging.warning(w)
    except:
        logging.error('############ Collect Data \"{}\" Error'.format(index_name))
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
    'start_date' : datetime(2023, 6, 9),
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 5),
}

dag = DAG(
    'collect_ddragon',
    default_args = default_args,
    # schedule_interval = '@daily'
    schedule_interval = '0 2 * * *'
)

''' 
Task 설정
Python Operator 활용 Python 함수 실행
'''
# 1. 버전 업데이트
task_update_version = PythonOperator(
    task_id = 'update_version',
    python_callable = update_version,
    dag = dag
)

# 2. 현재 버전 수집
task_get_version = PythonOperator(
    task_id = 'get_version',
    python_callable = get_version,
    provide_context = True,
    dag = dag
)

# 3-1. 챔피언 데이터 수집
task_collect_champions = PythonOperator(
    task_id = 'collect_champions',
    python_callable = collect_data,
    op_args=['champions'],
    provide_context = True,
    dag = dag
)

# 3-2. 아이템 데이터 수집
task_collect_items = PythonOperator(
    task_id = 'collect_items',
    python_callable = collect_data,
    op_args=['items'],
    provide_context = True,
    dag = dag
)

# 3-3. 룬 데이터 수집
task_collect_runes = PythonOperator(
    task_id = 'collect_runes',
    python_callable = collect_data,
    op_args=['runes'],
    provide_context = True,
    dag = dag
)

# 3-4. 스펠 데이터 수집
task_collect_spells = PythonOperator(
    task_id = 'collect_spells',
    python_callable = collect_data,
    op_args=['spells'],
    provide_context = True,
    dag = dag
)

''' Task 간 Flow 설정 '''
task_update_version >> task_get_version
task_get_version >> task_collect_champions
task_get_version >> task_collect_items
task_get_version >> task_collect_runes
task_get_version >> task_collect_spells