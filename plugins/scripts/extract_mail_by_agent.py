import os

import pandas as pd
from airflow.sdk import task
from utils.logger import get_logger
from utils.normalizer import normalize_date, normalize_str

logger = get_logger(__name__)

SOURCES = [
    {
        "file": "MAIL BEFR.xlsx",
        "date_col": "Date du message",
        "agent_col": "Créé par: Nom complet",
        "count_col": "Numéro de la requête",
        "metric": "Mails BEFR",
    },
    {
        "file": "MAIL FR.xlsx",
        "date_col": "Date du message",
        "agent_col": "Créé par: Nom complet",
        "count_col": "Numéro de la requête",
        "metric": "Mails FR",
    },
    {
        "file": "MAILS ARCHIVES.xlsx",
        "date_col": "Date de fermeture",
        "agent_col": "Propriétaire de la requête",
        "count_col": "Statut",
        "metric": "Mails Archivés",
    },
]


def process_sources(path: str, conf: dict) -> pd.DataFrame:
    """
    Centralized function to process the source files,
    :param path:
    :param conf: all the necessary information to process the source file, including date column, agent column, count column and metric name
    :return: DataFrame with three columns: Date, Agent and the metric column specified in conf
    """
    if not os.path.exists(path):
        logger.error(f"File {conf['file']} not found in {path}")
        raise FileNotFoundError(f"File {conf['file']} not found in {path}")
    try:
        logger.info(f"Extracting data from {conf['file']}")
        df = pd.read_excel(path)
    except Exception as e:
        logger.error(f"Error extracting data from {conf['file']}: {str(e)}")
        raise e

    df[conf["date_col"]] = normalize_date(df[conf["date_col"]])
    df = (
        df.groupby([conf["date_col"], conf["agent_col"]])
        .agg(**{conf["metric"]: (conf["count_col"], "count")})
        .reset_index()
    )
    column_mapping = {
        conf["date_col"]: "Date",
        conf["agent_col"]: "Agent",
    }

    df = df.rename(columns=column_mapping)
    df["Agent"] = normalize_str(df["Agent"])
    return df


@task
def extract_mail_by_agent(BASE_DIR: str, ds: str) -> str:
    """
    Extract mail data using the process_sources function
    :param BASE_DIR:
    :param ds: Jinja template for date string
    :return: str file path of the processed data
    """
    dfs = [
        process_sources(os.path.join(BASE_DIR, "raw", cfg["file"]), cfg)
        for cfg in SOURCES
    ]

    df_mails = dfs[0]
    for df in dfs[1:]:
        df_mails = df_mails.merge(df, on=["Date", "Agent"], how="outer")
    df_mails = df_mails.fillna(0)

    processed_dir = os.path.join(BASE_DIR, "tmp")
    os.makedirs(processed_dir, exist_ok=True)
    output_filename = f"processed_mails_{ds}.parquet"
    output_path = os.path.join(processed_dir, output_filename)
    df_mails.to_parquet(output_path, index=False)

    logger.info(f"Processed data saved to {output_path} - {len(df_mails)} lines ")
    return output_path
