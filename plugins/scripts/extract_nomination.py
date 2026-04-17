import os

import pandas as pd
from airflow.sdk import task
from utils.logger import get_logger

logger = get_logger(__name__)


@task
def extract_nomination(BASE_DIR: str, ds: str) -> str:
    """
    Extract nomination for each agent
    :param BASE_DIR:
    :return: str file path of the processed data
     :raises : FileNotFoundError: if the raw excel file is not found in the specified path
     :raises : Exception: if there is any error during the extraction process
    """
    filename = "NOMINATION.xlsx"
    file_path = os.path.join(BASE_DIR, "raw", filename)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {filename} not found in {file_path}")
    try:
        logger.info(f"Extracting data from {filename}")
        df = pd.read_excel(file_path)
    except Exception as e:
        logger.error(f"Error extracting data from {filename}: {str(e)}")
        raise e

    column_mapping = {
        "EQUIPE": "EQUIPE",
        "Log SF": "log",
        "Fiche de présence": "Agent",
    }
    df = df.rename(columns=column_mapping)
    df = df[list(column_mapping.values())]  # mapping col only
    df = df[["EQUIPE", "log", "Agent"]]
    df_nomination = df.astype(str).apply(lambda x: x.str.lower().str.strip())

    processed_dir = os.path.join(BASE_DIR, "tmp")
    os.makedirs(processed_dir, exist_ok=True)
    output_filename = f"processed_nomination_{ds}.parquet"
    output_path = os.path.join(processed_dir, output_filename)
    df_nomination.to_parquet(output_path, index=False)

    logger.info(
        f"Processed data saved to {output_path} - {df_nomination.shape[0]} rows and - {df_nomination.shape[1]} columns"
    )
    return output_path
