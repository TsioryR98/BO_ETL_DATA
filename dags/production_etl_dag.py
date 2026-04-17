import os
import sys

import pendulum
from airflow.sdk import dag
from dotenv import load_dotenv
from scripts.extract_batonnage import extract_batonnage
from scripts.extract_call_prod import extract_call_prod
from scripts.extract_closed_case import extract_closed_case
from scripts.extract_csat import extract_csat
from scripts.extract_eval import extract_eval
from scripts.extract_mail_by_agent import extract_mail_by_agent
from scripts.extract_nomination import extract_nomination
from scripts.extract_production import extract_production
from scripts.merge_prod_data import merge_prod_data

load_dotenv()
PATH = os.getenv("PYTHONPATH")
BASE_DIR = os.getenv("BASE_DIR")

sys.path.insert(0, PATH)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    dag_id="production_etl_dag",
    description="ETL for testing data for production BO",
    tags=["production", "etl"],
)
def production_etl_dag():
    """
    Only one simple dags for the processed data and save as to_parquet file
    No dependency between the extraction tasks , parallelization possible
    :returns CSV final production data for Vizualisation
    """

    path_prod = extract_production(BASE_DIR)
    path_bat = extract_batonnage(BASE_DIR)
    path_mail = extract_mail_by_agent(BASE_DIR)
    path_csat = extract_csat(BASE_DIR)
    path_eval = extract_eval(BASE_DIR)
    path_case = extract_closed_case(BASE_DIR)
    path_call = extract_call_prod(BASE_DIR)

    nomination = extract_nomination(BASE_DIR)
    extraction = [
        path_prod,
        path_bat,
        path_mail,
        path_csat,
        path_eval,
        path_case,
        path_call,
    ]
    merge_data = merge_prod_data(
        BASE_DIR=BASE_DIR,
        list_path=extraction,
        nomination_path=nomination,
    )
    nomination >> extraction >> merge_data


production_etl_dag()
