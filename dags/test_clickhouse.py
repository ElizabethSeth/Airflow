from pendulum import datetime, duration
import itertools
from airflow import XComArg
from airflow.decorators import dag, task
from lib.clickhouse_operator_extended import ClickHouseOperatorExtended
import logging
CLICKHOUSE_CONN_ID = 'clickhouse'
default_args={
    "owner": "Smalch",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": duration(seconds=5),
    # 'email_on_failure': True,
    # 'email_on_success': True,
    # 'email': EMAIL_DEVELOP,
}
@dag(
    tags=["test","stocks"],
    render_template_as_native_obj=True,
    max_active_runs=1,
    #schedule='50 2 * * *',
    schedule=None,
    start_date=datetime(2023,12,1, tz='Europe/Moscow'),
    catchup=False,
    default_args=default_args,
    template_searchpath='dags/include',        
    description='testing connection',
    doc_md=__doc__,
)
def testing_clickhouse():
    ch_list_count_rows_start_of_month = ClickHouseOperatorExtended(
        task_id='ch_list_count_rows_start_of_month',
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        sql='test.sql'
    )
    ch_list_count_rows_start_of_month
testing_clickhouse()
