윈도우 미지원 --> Linux or WSL 활용
 - Python : 3.8.10
 - Airflow : 2.3.2
 - Database : PostgreSQL 12.14

# Apache Airflow 설치 매뉴얼

#### 설치 폴더 생성
```
mkdir airflow
```

#### Airflow 홈 디렉토리 설정
```
export AIRFLOW_HOME={AIRFLOW_HOME}
```

#### Airflow, Python 버전 설정
```
AIRFLOW_VERSION=2.3.2
PYTHON_VERSION=3.8
```

#### Airflow 설치 파일 경로 설정
```
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

#### Airflow 설치
```
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

#### Airflow 설치 확인
```
airflow version
```

#### Airflow DB 초기화
```
airflow db init
```

#### Airflow 관리자 계정 생성
```
airflow users create \
  --username admin \
  --firstname {firstname} \
  --lastname {lastname} \
  --role Admin \
  --email {email}
```

#### Airflow 실행
```
airflow webserver -D --port {실행 포트}
```

#### Airflow 스케줄러 시작
```
airflow scheduler -D
```

# Database 설정(PostgreSQL)
```
CREATE DATABASE {db};
CREATE USER {user} WITH PASSWORD {password};
GRANT ALL PRIVILEGES ON DATABASE {db} TO {user};
```

# airflow.cfg 설정
 - dags_folder 수정
 - base_log_folder 수정
 - load_examples = False
 - sql_alchemy_conn = postgresql+psycopg2://{user}:{password}@{host}/{db}
