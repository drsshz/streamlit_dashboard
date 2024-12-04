import pandas as pd
from pathlib import Path
from etl.schemas import DataSchema
from etl.etl_pipeline import run_etl_pipeline


class SuperstoreModel:
    def __init__(self, df: pd.DataFrame | None = None):
        self._data: pd.DataFrame = df

    def load_data(self, data_file: Path):
        try:
            if data_file.name.endswith((".csv")):
                self._data = pd.read_csv(
                    data_file,
                    delimiter=",",
                    usecols=[
                        DataSchema.ROW_ID,
                        DataSchema.ORDER_ID,
                        DataSchema.ORDER_DATE,
                        DataSchema.SHIP_MODE,
                        DataSchema.SHIP_DATE,
                        DataSchema.CUSTOMER_ID,
                        DataSchema.CUSTOMER_NAME,
                        DataSchema.SEGMENT,
                        DataSchema.COUNTRY_REGION,
                        DataSchema.CITY,
                        DataSchema.STATE,
                        DataSchema.POSTAL_CODE,
                        DataSchema.REGION,
                        DataSchema.PRODUCT_ID,
                        DataSchema.CATEGORY,
                        DataSchema.SUB_CATEGORY,
                        DataSchema.PRODUCT_NAME,
                        DataSchema.SALES,
                        DataSchema.QUANTITY,
                        DataSchema.DISCOUNT,
                        DataSchema.PROFIT,
                    ],
                    dtype={
                        DataSchema.ROW_ID: int,
                        DataSchema.ORDER_ID: str,
                        DataSchema.SHIP_MODE: str,
                        DataSchema.CUSTOMER_ID: str,
                        DataSchema.CUSTOMER_NAME: str,
                        DataSchema.SEGMENT: str,
                        DataSchema.COUNTRY_REGION: str,
                        DataSchema.CITY: str,
                        DataSchema.STATE: str,
                        DataSchema.POSTAL_CODE: str,
                        DataSchema.REGION: str,
                        DataSchema.PRODUCT_ID: str,
                        DataSchema.CATEGORY: str,
                        DataSchema.SUB_CATEGORY: str,
                        DataSchema.PRODUCT_NAME: str,
                        DataSchema.SALES: float,
                        DataSchema.QUANTITY: int,
                        DataSchema.DISCOUNT: float,
                        DataSchema.PROFIT: float,
                    },
                    on_bad_lines="skip",
                    parse_dates=[DataSchema.ORDER_DATE, DataSchema.SHIP_DATE],
                    date_format="%Y-%m-%d",
                )
            else:
                raise ValueError("Unsupported file format")
        except Exception as e:
            raise RuntimeError(f"Error loading data: {e}")

    def filter_data(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        region: list[str],
        state: list[str],
        city: list[str],
    ):
        self._data = self._data[
            (self._data[DataSchema.ORDER_DATE] >= start_date)
            & (self._data[DataSchema.ORDER_DATE] <= end_date)
            & (self._data[DataSchema.REGION].isin(region))
            & (self._data[DataSchema.STATE].isin(state))
            & (self._data[DataSchema.CITY].isin(city))
        ]

    def filter_cities_based_on_states(self, states: list[str]):
        return list(
            self._data.loc[
                self._data[DataSchema.STATE].isin(states), DataSchema.CITY
            ].unique()
        )

    def filter_states_based_on_regions(self, regions: list[str]):
        return list(
            self._data.loc[
                self._data[DataSchema.REGION].isin(regions), DataSchema.STATE
            ].unique()
        )

    @property
    def min_order_date(self) -> pd.Timestamp:
        return self._data[DataSchema.ORDER_DATE].min()

    @property
    def max_order_date(self) -> pd.Timestamp:
        return self._data[DataSchema.ORDER_DATE].max()

    @property
    def regions(self) -> list[str]:
        return self._data[DataSchema.REGION].to_list()

    @property
    def unique_regions(self) -> list[str]:
        return list(self._data[DataSchema.REGION].unique())

    @property
    def states(self) -> list[str]:
        return self._data[DataSchema.STATE].to_list()

    @property
    def unique_states(self) -> list[str]:
        return list(self._data[DataSchema.STATE].unique())

    @property
    def cities(self) -> list[str]:
        return self._data[DataSchema.CITY].to_list()

    @property
    def unique_cities(self) -> list[str]:
        return list(self._data[DataSchema.CITY].unique())

    @property
    def category_data(self) -> pd.DataFrame:
        return self._data.groupby(by=DataSchema.CATEGORY, as_index=False)[
            DataSchema.SALES
        ].sum()

    @property
    def region_data(self) -> pd.DataFrame:
        return self._data.groupby(by=DataSchema.REGION, as_index=False)[
            DataSchema.SALES
        ].sum()

    @property
    def time_series_data(self) -> pd.DataFrame:
        self._data[DataSchema.MONTH_YEAR] = (
            self._data[DataSchema.ORDER_DATE].dt.to_period("M").dt.strftime("%Y: %b")
        )
        return self._data.groupby(by=DataSchema.MONTH_YEAR, as_index=False)[
            DataSchema.SALES
        ].sum()

    @property
    def df(self) -> pd.DataFrame:
        return self._data


if __name__ == "__main__":
    model = SuperstoreModel()
    model.load_data(data_file=run_etl_pipeline())
    pass
