import os

import pandas as pd
from airflow.sdk import task
from utils.logger import get_logger
from utils.normalizer import normalize_date, normalize_str

logger = get_logger(__name__)


@task
def extract_production(BASE_DIR: str, ds: str) -> str:
    """
    Extract production data from the raw excel files, process them and save the processed data as parquet file.
    :param BASE_DIR: str  base directory
    :param ds: Jinja template
    :return: str  file path of the processed data
    :raises : FileNotFoundError: if any of the raw excel files is not found in the specified path
    """
    filename = "FDP JANV.xlsx"
    nomination_filename = f"processed_nomination_{ds}.parquet"
    nomination_file_path = os.path.join(BASE_DIR, "tmp", nomination_filename)
    file_path = os.path.join(BASE_DIR, "raw", filename)

    if not os.path.exists(nomination_file_path):
        raise FileNotFoundError(
            f"File {nomination_filename} not found in {nomination_file_path}"
        )
    try:
        logger.info(f"Extracting data from {nomination_filename}")
        df_nomination = pd.read_parquet(nomination_file_path)
    except Exception as e:
        logger.error(f"Error extracting data from {filename}: {str(e)}")
        raise e
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {filename} not found in {file_path}")
    try:
        logger.info(f"Extracting data from {filename}")
        df_prod = pd.read_excel(file_path, skiprows=1)
    except Exception as e:
        logger.error(f"Error extracting data from {filename}: {str(e)}")
        raise e

    column_mapping = {"DATE": "Date"}
    df_prod = df_prod.rename(columns=column_mapping)

    df_prod["PSEUDOS"] = normalize_str(df_prod["PSEUDOS"])
    df_nomination["Agent"] = normalize_str(df_nomination["Agent"])

    unused_col = df_prod[
        (df_prod["Date"].isna() | (df_prod["Date"] == 0))
        & (df_prod["H PROD SC FR"].isna() | (df_prod["H PROD SC FR"] == 0))
        & (df_prod["H PROD SC BEFR"].isna() | (df_prod["H PROD SC BEFR"] == 0))
        & (df_prod["RS/BO/PDD"].isna() | (df_prod["RS/BO/PDD"] == 0))
    ]
    df_prod = df_prod.drop(unused_col.index)
    prod_col = ["H PROD SC FR", "H PROD SC BEFR", "RS/BO/PDD"]
    df_prod["total_production"] = df_prod[prod_col].fillna(0).sum(axis=1)
    df_prod.loc[df_prod["total_production"] > 0, prod_col] = df_prod[prod_col].fillna(0)
    df_prod["Date"] = normalize_date(df_prod["Date"])

    df_final_prod = df_nomination.merge(
        df_prod, left_on="Agent", right_on="PSEUDOS", how="inner"
    )
    df_final_prod = df_final_prod.drop(columns=["PSEUDOS", "Agent"]).rename(
        columns={"log": "Agent"}
    )

    df_final_prod = df_final_prod[
        [
            "Date",
            "Agent",
            "H PROD SC FR",
            "H PROD SC BEFR",
            "RS/BO/PDD",
        ]
    ]
    processed_dir = os.path.join(BASE_DIR, "tmp")
    os.makedirs(processed_dir, exist_ok=True)
    output_filename = f"processed_fdp_{ds}.parquet"
    output_path = os.path.join(processed_dir, output_filename)
    df_final_prod.to_parquet(output_path, index=False)

    logger.info(f"Processed data saved to {output_path} - {len(df_final_prod)} lines")
    return output_path
