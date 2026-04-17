import os

import pandas as pd
from airflow.sdk import task
from utils.logger import get_logger
from utils.normalizer import normalize_date, normalize_str

logger = get_logger(__name__)


@task
def extract_batonnage(BASE_DIR: str, ds: str) -> str:
    """
    Extract production data from the raw excel files, process them and save the processed data as parquet file.
    :param BASE_DIR: str  base directory where the raw excel files are located and where the processed data will be saved
    :param ds: Jinja template
    :return: str  file path of the processed data
    :raises : FileNotFoundError: if any of the raw excel files is not found in the specified path
    """
    extract_files = [
        ("BA", "BA JANVIER.xlsx"),
        ("BO", "BATONNAGE BO.xlsx"),
        ("PDD", "BATONNAGE PDD.xlsx"),
        ("RS", "BATONNAGE RS.xlsx"),
    ]

    dataframes = {}

    for key, filename in extract_files:
        file_path = os.path.join(BASE_DIR, "raw", filename)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {filename} not found in {file_path}")
        try:
            logger.info(f"Extracting data from {filename}")
            df = pd.read_excel(file_path)
            df.columns = df.columns.str.replace(r"\xa0", " ", regex=True).str.strip()

            dataframes[key] = df
        except Exception as e:
            logger.error(f"Error extracting data from {filename}: {str(e)}")
            raise e

    df_BA = dataframes["BA"]
    df_BO = dataframes["BO"]
    df_PDD = dataframes["PDD"]
    df_RS = dataframes["RS"]

    # ------------------- BA
    df_BA_grouped = (
        df_BA.groupby(["Demande de BA : Date de création", "Demande de BA : Créé par"])
        .agg(**{"BAT BA": ("Motif", "count")})
        .reset_index()
    )
    df_BA_grouped["Demande de BA : Créé par"] = (
        df_BA_grouped["Demande de BA : Créé par"].astype(str).str.lower().str.strip()
    )
    column_mapping = {
        "Demande de BA : Date de création": "Date",
        "Demande de BA : Créé par": "Agent",
    }
    df_BA_final = df_BA_grouped.rename(columns=column_mapping)

    # ------------------- BO
    column_mapping = {"AGENT": "Agent", "DATE": "Date", "TOTAL": "BAT BO"}
    df_BO = df_BO.rename(columns=column_mapping)
    df_BO["Agent"] = normalize_str(df_BO["Agent"])
    df_BO["Date"] = normalize_date(df_BO["Date"])
    unused_col = df_BO[
        (df_BO["SF"].isna() | (df_BO["SF"] == 0))
        & (df_BO["OE Justifiées"].isna() | (df_BO["OE Justifiées"] == 0))
        & (df_BO["OE clôturées"].isna() | (df_BO["OE clôturées"] == 0))
        & (df_BO["OE injustifiées"].isna() | (df_BO["OE injustifiées"] == 0))
        & (df_BO["Relance"].isna() | (df_BO["Relance"] == 0))
        & (df_BO["ETIQUETTE"].isna() | (df_BO["ETIQUETTE"] == 0))
        & (df_BO["COUPON"].isna() | (df_BO["COUPON"] == 0))
    ]
    df_BO = df_BO.drop(unused_col.index)
    df_BO_grouped = df_BO[["Date", "Agent", "BAT BO"]]

    # ------------------- PDD
    df_PDD["Agent"] = normalize_str(df_PDD["Agent"])
    df_PDD["Date"] = normalize_date(df_PDD["Date"])
    df_PDD_grouped = (
        df_PDD.groupby(["Date", "Agent"])
        .agg(**{"BAT PDD": ("Numéro de commande", "count")})
        .reset_index()
    )
    df_PDD_grouped = df_PDD_grouped[["Date", "Agent", "BAT PDD"]]

    # ------------------- RS
    df_RS["Agent"] = normalize_str(df_RS["Agent"])
    df_RS["Date de traitement"] = normalize_date(df_RS["Date de traitement"])
    df_RS_grouped = (
        df_RS.groupby(["Date de traitement", "Agent"])
        .agg(**{"BAT RS": ("N° requête", "count")})
        .reset_index()
    )
    column_mapping = {"Date de traitement": "Date"}
    df_RS_grouped = df_RS_grouped.rename(columns=column_mapping)
    df_RS_grouped = df_RS_grouped[["Date", "Agent", "BAT RS"]]

    df_batonnage = (
        df_BA_final.merge(df_BO_grouped, on=["Date", "Agent"], how="outer")
        .merge(df_PDD_grouped, on=["Date", "Agent"], how="outer")
        .merge(df_RS_grouped, on=["Date", "Agent"], how="outer")
    )

    processed_dir = os.path.join(BASE_DIR, "tmp")
    os.makedirs(processed_dir, exist_ok=True)
    output_filename = f"processed_batonnage_{ds}.parquet"  # ignored {ds} jinja
    output_path = os.path.join(processed_dir, output_filename)
    df_batonnage.to_parquet(output_path, index=False)

    logger.info(f"Processed data saved to {output_path}  {len(df_batonnage)} lines")
    return output_path
