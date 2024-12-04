import requests
from pathlib import Path
from loguru import logger
import pandas as pd
from etl.schemas import DataSchemaRAW, DataSchema

DATASET_URL = (
    "https://www.tableau.com/sites/default/files/2021-05/Sample%20-%20Superstore.xls"
)
RAW_DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "raw" / "Superstore.xls"
PROCESSED_DATA_PATH = (
    Path(__file__).resolve().parents[2] / "data" / "processed" / "Superstore.csv"
)
FORCE_GENERATE = True


def download_file(
    url: str, output_path: Path, force_download: bool = False, chunk_size: int = 8192
):
    """
    Download a file from the specified URL if it doesn't exist or force download is enabled.
    """
    if not output_path.exists() or force_download:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Downloading file from {url} to {output_path}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(output_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                file.write(chunk)
        logger.info(f"File successfully downloaded to {output_path}")
    else:
        logger.info(f"File already exists at {output_path}")
        return output_path


def load_file(file_path: Path) -> pd.DataFrame:
    try:
        return pd.read_excel(
            file_path,
            parse_dates=["Order Date", "Ship Date"],
            date_parser=lambda x: pd.to_datetime(x, format="%Y-%m-%d", errors="coerce"),
        )
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    schema_mapping = {
        getattr(DataSchemaRAW, attr): getattr(DataSchema, attr)
        for attr in DataSchemaRAW.__dict__.keys()
        if not attr.startswith("_")
    }
    logger.info(f"Renaming columns according to the schema mapping: {schema_mapping}")
    return df.rename(columns=schema_mapping)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning the dataframe by removing NA's and dropping duplicates")
    df = df.dropna(how="any")
    return df.drop_duplicates()


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df = rename_columns(df)
    df = clean_data(df)

    return df


def export_df_to_csv(df: pd.DataFrame, output_path: Path) -> None:
    Path(output_path).parent.resolve().mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")


def run_etl_pipeline(force_generate: bool = False):
    if not PROCESSED_DATA_PATH.exists() or force_generate:
        logger.info("Starting ETL process...")
        logger.info(f"Downloading the input file from {DATASET_URL} ...")
        data_file = download_file(DATASET_URL, RAW_DATA_PATH)
        raw_df = load_file(data_file)
        df = transform_data(raw_df)
        export_df_to_csv(df, PROCESSED_DATA_PATH)
    else:
        logger.info(
            f"Using the processed file from the cached data folder: {PROCESSED_DATA_PATH}"
        )
    return PROCESSED_DATA_PATH


if __name__ == "__main__":
    run_etl_pipeline(FORCE_GENERATE)
