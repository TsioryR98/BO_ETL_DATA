import os

import pandas as pd
from airflow.sdk import task
from utils.logger import get_logger

logger = get_logger(__name__)


@task
def merge_prod_data(
    BASE_DIR: str, list_path: list, nomination_path: str, ds: str = None
) -> str:
    """
    Merge production data from call and mail production
    @task and no PythonOperator because we want to use the same function
    :param BASE_DIR:
    :param ds: Jinja
    :return: file for final production data
     :raises : FileNotFoundError: if the processed data files are not found in the specified path
     :raises : Exception: if there is any error during the merging process
    """

    if not os.path.exists(list_path[0]):
        raise FileNotFoundError(f"File not found in {list_path[0]}")

    df_prod = pd.read_parquet(list_path[0])

    for path in list_path[1:]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found in {path}")
        try:
            logger.info(f"Reading data from {path}")
            df_next = pd.read_parquet(path)
            df_prod = pd.merge(df_prod, df_next, on=["Date", "Agent"], how="outer")
        except Exception as e:
            logger.error(f"Error reading data from {path}: {str(e)}")
            raise e

    df_nomination = pd.read_parquet(nomination_path)[["log", "EQUIPE"]]
    df_prod = df_prod.merge(
        df_nomination, left_on="Agent", right_on="log", how="inner"
    )  # without EQUIPE
    df_prod = df_prod.drop(columns=["log"])

    processed_dir = os.path.join(BASE_DIR, "processed")
    os.makedirs(processed_dir, exist_ok=True)
    output_path = os.path.join(processed_dir, f"final_production_{ds}.csv")
    df_prod.to_csv(output_path, index=False)

    logger.info(
        f"Saved to {output_path} — {df_prod.shape[0]} rows, {df_prod.shape[1]} cols"
    )
    return output_path
