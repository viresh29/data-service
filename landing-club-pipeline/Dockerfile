FROM quay.io/astronomer/ap-airflow:1.10.12-buster-onbuild

COPY . .

RUN pip3 install -r requirements.txt

ENV AIRFLOW__ELASTICSEARCH__JSON_FORMAT True
ENV AIRFLOW__ELASTICSEARCH__JSON_FIELDS asctime, filename, lineno, levelname, message, extra
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLE False
