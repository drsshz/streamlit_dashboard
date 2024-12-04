import streamlit as st
import plotly.express as px
import pandas as pd


class SuperstoreView:
    @staticmethod
    def render_title():
        st.set_page_config(
            page_title="Superstore!!!", page_icon=":bar_chart:", layout="wide"
        )
        st.title(":bar_chart: Sample SuperStore EDA")

    @staticmethod
    def render_file_uploader():
        return st.file_uploader(":file_folder: Upload a file", type=["csv"])

    @staticmethod
    def render_date_filters(
        start_date: pd.Timestamp, end_date: pd.Timestamp
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        left, right = st.columns(2)
        with left:
            date1 = st.date_input("Start Date", start_date)
        with right:
            date2 = st.date_input("End Date", end_date)
        return pd.Timestamp(date1), pd.Timestamp(date2)

    @staticmethod
    def render_multiselect_header(multi_select_header: str) -> None:
        st.sidebar.header(multi_select_header)

    @staticmethod
    def render_multiselect(label: str, values: list[str], select_all: bool = False):
        container = st.sidebar.container()
        all = st.sidebar.checkbox(
            f"Select all {label}", value=select_all, key=f"checkbox_{label}"
        )
        selected_regions = container.multiselect(
            f"{label}:",
            options=values,
            default=values if all else [],
            key=f"multiselect_{label}",
        )
        return selected_regions

    @staticmethod
    def render_bar_plot(df: pd.DataFrame, x_col: str, y_col: str):
        return px.bar(
            df,
            x=x_col,
            y=y_col,
            text=["${:,.0f}".format(x) for x in df[y_col]],
            template="seaborn",
        )

    @staticmethod
    def render_pie_plot(
        df: pd.DataFrame, values_col: str, names_col: str, hole_val: float
    ):
        return px.pie(df, values=values_col, names=names_col, hole=hole_val)

    @staticmethod
    def render_line_plot(df: pd.DataFrame, x_col: str, y_col: str):
        return px.line(df, x=x_col, y=y_col, template="gridon")

    @staticmethod
    def render_charts_in_columns(left_fig, right_fig, left_title, right_title):
        left, right = st.columns(2)
        with left:
            st.subheader(left_title)
            st.plotly_chart(left_fig, use_container_width=True)
        with right:
            st.subheader(right_title)
            st.plotly_chart(right_fig, use_container_width=True)

    @staticmethod
    def render_tables_in_columns(
        left_data: pd.DataFrame,
        right_data: pd.DataFrame,
        left_title: str,
        right_title: str,
        left_file: str,
        right_file: str,
    ):
        left, right = st.columns(2)
        with left:
            with st.expander(f"View Data - {left_title}"):
                st.write(left_data.style.background_gradient(cmap="Blues"))
                csv = left_data.to_csv(index=False).encode("utf-8")
                st.download_button("Download Data", data=csv, file_name=left_file)
        with right:
            with st.expander(f"View Data - {right_title}"):
                st.write(right_data.style.background_gradient(cmap="Oranges"))
                csv = right_data.to_csv(index=False).encode("utf-8")
                st.download_button("Download Data", data=csv, file_name=right_file)

    @staticmethod
    def render_chart(fig, title: str):
        st.subheader(title)
        st.plotly_chart(fig, use_container_width=True)

    @staticmethod
    def render_table(
        data: pd.DataFrame, title: str, file_name: str, cmap: str = "Blues"
    ):
        with st.expander(f"View Data - {title}"):
            st.write(data.style.background_gradient(cmap=cmap))
            csv = data.to_csv(index=False).encode("utf-8")
            st.download_button("Download Data", data=csv, file_name=file_name)

    @staticmethod
    def render_treemap(
        data: pd.DataFrame,
        title: str,
        path: list[str],
        values: str,
        hover_data: list[str],
        color: str,
    ):
        st.subheader(title)
        fig3 = px.treemap(
            data, path=path, values=values, hover_data=hover_data, color=color
        )
        fig3.update_layout(width=800, height=650)
        st.plotly_chart(fig3, use_container_width=True)

    @staticmethod
    def render_pie_plot_in_columns(
        left_data: pd.DataFrame,
        right_data: pd.DataFrame,
        left_title: str,
        right_title: str,
        left_values: str,
        right_values: str,
        left_names: str,
        right_names: str,
    ):
        chart1, chart2 = st.columns((2))
        with chart1:
            st.subheader(left_title)
            fig = px.pie(
                left_data, values=left_values, names=left_names, template="plotly_dark"
            )
            fig.update_traces(text=left_data[left_names], textposition="inside")
            st.plotly_chart(fig, use_container_width=True)

        with chart2:
            st.subheader(right_title)
            fig = px.pie(
                right_data, values=right_values, names=right_names, template="gridon"
            )
            fig.update_traces(text=right_data[right_names], textposition="inside")
            st.plotly_chart(fig, use_container_width=True)

    @staticmethod
    def render_scatter_plot(
        data: pd.DataFrame, title: str, x_col: str, y_col: str, size_col: str
    ):
        data1 = px.scatter(data, x=x_col, y=y_col, size=size_col)
        data1["layout"].update(
            title=title,
            titlefont=dict(size=20),
            xaxis=dict(title=x_col, titlefont=dict(size=19)),
            yaxis=dict(title=y_col, titlefont=dict(size=19)),
        )
        st.plotly_chart(data1, use_container_width=True)
