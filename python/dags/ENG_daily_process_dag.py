# Cronguru: https://crontab.guru/between-certain-hours
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
# Email when fail: https://stackoverflow.com/questions/51726248/airflow-dag-customized-email-on-any-of-the-task-failure
#from airflow.operators.email_operator import EmailOperator
#from airflow.utils.trigger_rule import TriggerRule

from energy import _get_hourly_demand, _calculate_hourly_demand_forecast, _get_monthly_data

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'energy',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['colmo786@gmail.com'],
    'email_on_failure': False, # to send an e-mail after task fail, you should config SMTP in airflow.cfg and set this to True
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# define the python functions
def get_monthly_data(**kwargs):
    database = Variable.get("ENERGY_DB")
    host = Variable.get("ENERGY_DB_HOST")
    user = Variable.get("ENERGY_DB_USER")
    password = Variable.get("ENERGY_DB_PASS")
    port = Variable.get("ENERGY_DB_PORT")
    # execution_date will be deprecated, use 'data_interval_start' or 'logical_date' instead.
    process_ok = _get_monthly_data(date=kwargs['logical_date'], database=database, host=host
                    , user=user, password=password, port=port, verbose=True)
    print('INFO get_monthly_data execution: ', kwargs['logical_date'])
    if not process_ok:
        raise ValueError()
    return

# define the DAG
dag = DAG(
    'ENG_daily_process_v2',
    default_args=default_args,
    description='Queries Cammesa API to get monthly data. Inserts or updates monthly information into Postgres DB. Calculates next 6 months forecast',
    # Having the schedule_interval as None means Airflow will never automatically schedule a run of the Dag.
    # you can schedule it manually via the trigger button in the Web UI
    #schedule_interval=None
    schedule_interval='0 20 * * *' # Every day at 20 hs
)

# define tasks
t1 = PythonOperator(
    task_id='get_monthly_data',
    python_callable= get_monthly_data,
    provide_context=True,
    dag=dag,
)

t1