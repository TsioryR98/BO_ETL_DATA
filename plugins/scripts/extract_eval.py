import os

import pandas as pd
from airflow.sdk import task
from utils.logger import get_logger
from utils.normalizer import normalize_date, normalize_str

logger = get_logger(__name__)


@task
def extract_eval(BASE_DIR: str, ds: str) -> str:
    """
    Extract evaluation data from the raw excel file, process it and save the processed data as parquet file.
    :param BASE_DIR:
    :param ds: Jija template for date
    :return: str : file path of the processed data
    :raises : FileNotFoundError: if the raw excel file is not found
    """

    filename = "EVAL SRP.xlsx"
    file_path = os.path.join(BASE_DIR, "raw", filename)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {filename} not found in {file_path}")
    try:
        logger.info(f"Extracting data from {filename}")
        df_eval_raw = pd.read_excel(file_path)
    except Exception as e:
        logger.error(f"Error extracting data from {filename}: {str(e)}")
        raise e

    df_eval_raw["Type d'appel"] = df_eval_raw["Type d'appel"].astype(str).str.strip()
    df_eval_raw["Date de l'évaluation"] = normalize_date(
        df_eval_raw["Date de l'évaluation"]
    )  # without time
    df_eval_raw["Agent"] = normalize_str(df_eval_raw["Agent"])
    df_eval_raw = df_eval_raw.rename(columns={"Date de l'évaluation": "Date"})
    df_eval_phone = (
        df_eval_raw.loc[df_eval_raw["Type d'appel"] == "Call entrant 2021"]
        .copy()
        .groupby(["Agent", "Date"])
        .agg(
            Retour_CALL=("Code évaluation", "count"),
            EVAL_CALL=("Note", "sum"),
        )
        .reset_index()
    )
    df_eval_mail = (
        df_eval_raw.loc[df_eval_raw["Type d'appel"] == "Mail 2021"]
        .copy()
        .groupby(["Agent", "Date"])
        .agg(
            Retour_MAIL=("Code évaluation", "count"),
            EVAL_MAIL=("Note", "sum"),
        )
        .reset_index()
    )
    df_eval_global = df_eval_mail.merge(
        df_eval_phone, on=["Date", "Agent"], how="outer"
    )
    eval_count = ["Retour_MAIL", "Retour_CALL"]
    df_eval_global[eval_count] = df_eval_global[eval_count].astype("Int64")
    processed_dir = os.path.join(BASE_DIR, "tmp")
    os.makedirs(processed_dir, exist_ok=True)
    output_filename = f"processed_eval_{ds}.parquet"
    output_path = os.path.join(processed_dir, output_filename)
    df_eval_global.to_parquet(output_path, index=False)

    logger.info(f"Processed data saved to {output_path} - {len(df_eval_global)} lines")
    return output_path
