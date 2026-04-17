import pandas as pd


def normalize_str(series: pd.Series) -> pd.Series:
    """
    Normalize agent names by converting to lowercase and stripping leading/trailing whitespace.
    """
    return series.astype(str).str.lower().str.strip()


def normalize_date(series: pd.Series) -> pd.Series:
    """
    Normalize date by converting to datetime format and handling errors.
    """
    return pd.to_datetime(series, errors="coerce").dt.normalize()
