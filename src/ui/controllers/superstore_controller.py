from models.superstore_model import SuperstoreModel
from views.superstore_view import SuperstoreView
import streamlit as st
from pathlib import Path
from etl.schemas import DataSchema


class SuperstoreController:
    def __init__(self):
        self.model = SuperstoreModel()
        self.view = SuperstoreView()

    def run(self, default_file: Path):
        self.view.render_title()
        uploaded_file = self.view.render_file_uploader()

        try:
            self.model.load_data(uploaded_file or default_file)
        except RuntimeError as e:
            st.error(str(e))
            return

        start_date = self.model.min_order_date
        end_date = self.model.max_order_date

        date1, date2 = self.view.render_date_filters(start_date, end_date)

        self.view.render_multiselect_header("Choose your filter: ")
        selected_regions = self.view.render_multiselect(
            "Regions", self.model.unique_regions, select_all=True
        )

        filtered_states = self.model.filter_states_based_on_regions(selected_regions)
        selected_states = self.view.render_multiselect(
            "States", filtered_states, select_all=True
        )

        filtered_cities = self.model.filter_cities_based_on_states(selected_states)
        selected_cities = self.view.render_multiselect(
            "Cities", filtered_cities, select_all=True
        )

        self.model.filter_data(
            date1, date2, selected_regions, selected_states, selected_cities
        )

        category_data = self.model.category_data

        category_chart = self.view.render_bar_plot(
            category_data, DataSchema.CATEGORY, DataSchema.SALES
        )

        region_data = self.model.region_data

        region_chart = self.view.render_pie_plot(
            region_data, DataSchema.SALES, DataSchema.REGION, 0.5
        )

        self.view.render_charts_in_columns(
            left_fig=category_chart,
            right_fig=region_chart,
            left_title="Category-wise Sales",
            right_title="Region-wise Sales",
        )

        self.view.render_tables_in_columns(
            left_data=category_data,
            right_data=region_data,
            left_title="Category Data",
            right_title="Region Data",
            left_file="Category.csv",
            right_file="Region.csv",
        )

        time_series_data = self.model.time_series_data
        time_series_chart = self.view.render_line_plot(
            time_series_data, DataSchema.MONTH_YEAR, DataSchema.SALES
        )
        self.view.render_chart(time_series_chart, "Time Series Analysis")
        self.view.render_table(time_series_data, "TimeSeris", "TimeSeries.csv")

        self.view.render_treemap(
            self.model.df,
            title="Hierarchical view of Sales using TreeMap",
            path=[DataSchema.REGION, DataSchema.CATEGORY, DataSchema.SUB_CATEGORY],
            values=DataSchema.SALES,
            hover_data=[DataSchema.SALES],
            color=DataSchema.SUB_CATEGORY,
        )

        self.view.render_pie_plot_in_columns(
            left_data=self.model.df,
            right_data=self.model.df,
            left_title="Segment wise Sales",
            right_title="Category wise Sales",
            left_values=DataSchema.SALES,
            right_values=DataSchema.SALES,
            left_names=DataSchema.SEGMENT,
            right_names=DataSchema.CATEGORY,
        )

        self.view.render_scatter_plot(
            data=self.model.df,
            title="Relationship between Sales and Profits using Scatter Plot.",
            x_col=DataSchema.SALES,
            y_col=DataSchema.PROFIT,
            size_col=DataSchema.QUANTITY,
        )
