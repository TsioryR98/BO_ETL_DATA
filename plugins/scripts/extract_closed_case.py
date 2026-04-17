import os

import pandas as pd
from airflow.sdk import task
from utils.logger import get_logger
from utils.normalizer import normalize_date, normalize_str

logger = get_logger(__name__)


@task
def extract_closed_case(BASE_DIR: str, ds: str) -> str:
    """
    Extract closed case data from the raw excel file
    :param BASE_DIR:
    :param ds: Jinja template for data
    :return: str file path of the processed data
    :raises : FileNotFoundError: if the raw excel file is not found in the specified path
    :raises : Exception: if there is any error during the extraction process
    """

    filename = "CLOSED CASE.xlsx"
    file_path = os.path.join(BASE_DIR, "raw", filename)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {filename} not found in {file_path}")
    try:
        logger.info(f"Extracting data from {filename}")
        df_cCase_raw = pd.read_excel(file_path)
    except Exception as e:
        raise Exception(f"Error extracting data from {filename}: {str(e)}")

    df_cCase_raw["Date de fermeture"] = normalize_date(
        df_cCase_raw["Date de fermeture"]
    )
    df_cCase_raw["Statut"] = df_cCase_raw["Statut"].astype(str).str.strip()
    df_cCase_raw["Date de fermeture"] = df_cCase_raw["Date de fermeture"]
    df_cCase_raw["Date de fermeture"] = normalize_date(
        df_cCase_raw["Date de fermeture"]
    )  # without time

    df_pivot = df_cCase_raw.pivot_table(
        index=["Propriétaire de la requête", "Date de fermeture"],
        columns="Statut",
        aggfunc="size",
        fill_value=0,
    )
    column_mapping = {
        "Fermée en attente retour client": "FEAR",
    }
    df_pivot = df_pivot.rename(columns=column_mapping)

    df_total = df_pivot.assign(Total=lambda x: x.sum(axis=1)).reset_index()
    columns = {
        "Date de fermeture": "Date",
        "Propriétaire de la requête": "Agent",
    }
    df_total = df_total.rename(columns=columns)
    df_total = df_total[["Date", "Agent", "Fermée", "FEAR", "Doublon", "Spam", "Total"]]
    df_total["Agent"] = normalize_str(df_total["Agent"])
    processed_dir = os.path.join(BASE_DIR, "tmp")
    os.makedirs(processed_dir, exist_ok=True)
    output_filename = f"processed_closed_case_{ds}.parquet"
    output_path = os.path.join(processed_dir, output_filename)
    df_total.to_parquet(output_path, index=False)

    logger.info(
        f"Processed data saved to {output_path} - {df_total.shape[0]} rows and - {df_total.shape[1]} columns"
    )
    return output_path
