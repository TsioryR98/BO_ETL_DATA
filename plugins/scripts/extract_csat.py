import os

import pandas as pd
from airflow.sdk import task
from utils.logger import get_logger
from utils.normalizer import normalize_date, normalize_str

logger = get_logger(__name__)


@task
def extract_csat(BASE_DIR: str, ds: str) -> str:
    """
    Extract CSAT data from the raw excel file
    :param BASE_DIR:
    :param ds: Jinja template for date
    :return: str : File path of the processed data
     :raises : FileNotFoundError: if the raw excel file is not found in the specified path
     :raises : Exception: if there is any error during the extraction processs
    """

    filename = "CSAT SRP.xlsx"
    file_path = os.path.join(BASE_DIR, "raw", filename)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {filename} not found in {file_path}")
    try:
        logger.info(f"Extracting data from {filename}")
        df_csat_raw = pd.read_excel(file_path)
    except Exception as e:
        raise Exception(f"Error extracting data from {filename}: {str(e)}")

    df_csat_raw["Origin"] = (
        df_csat_raw["Origin"].str.encode("latin1").str.decode("utf-8")
    )
    df_csat_raw["# Date"] = normalize_date(df_csat_raw["# Date"])
    df_csat_raw["# Date"] = df_csat_raw["# Date"].dt.normalize()  # without time
    df_phone = (
        df_csat_raw.loc[df_csat_raw["Origin"] == "Téléphone"]
        .copy()
        .groupby(["Owner.LastName", "# Date"])
        .agg(
            Amabilité_Telephone=("Q3a_Amabilite", "sum"),
            Resolution_Telephone=("Q1_Resolution", "sum"),
        )
        .reset_index()
    )

    df_mail = (
        df_csat_raw.loc[df_csat_raw["Origin"] == "E-mail"]
        .copy()
        .groupby(["Owner.LastName", "# Date"])
        .agg(
            Amabilité_Mail=("Q3a_Amabilite", "sum"),
            Resolution_Mail=("Q1_Resolution", "sum"),
        )
        .reset_index()
    )
    df_csat_global = df_phone.merge(
        df_mail, on=["# Date", "Owner.LastName"], how="outer"
    )

    column_mapping = {"Owner.LastName": "Agent", "# Date": "Date"}
    df_csat_global = df_csat_global.rename(columns=column_mapping)
    df_csat_global["Agent"] = normalize_str(df_csat_global["Agent"])

    processed_dir = os.path.join(BASE_DIR, "tmp")
    os.makedirs(processed_dir, exist_ok=True)
    output_filename = f"processed_csat_{ds}.parquet"
    output_path = os.path.join(processed_dir, output_filename)
    df_csat_global.to_parquet(output_path, index=False)

    logger.info(
        f"Processed data saved to {output_path} - {df_csat_global.shape[0]} rows and - {df_csat_global.shape[1]} columns"
    )
    return output_path
