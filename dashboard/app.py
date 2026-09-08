import os

import pandas as pd
import streamlit as st

from sqlalchemy import create_engine


# ---------------------------------------------------------
# PAGE CONFIGURATION
# ---------------------------------------------------------

st.set_page_config(
    page_title="ACLabStream",
    page_icon="🧪",
    layout="wide",
)

st.title("🧪 ACLabStream")
st.caption("Automated laboratory data pipeline")


# ---------------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------------

@st.cache_resource
def get_engine():

    connection_string = (
        f"postgresql+psycopg://"
        f"{os.environ['DB_USER']}:"
        f"{os.environ['DB_PASSWORD']}@"
        f"{os.environ['DB_HOST']}:"
        f"{os.environ['DB_PORT']}/"
        f"{os.environ['DB_NAME']}"
    )

    return create_engine(connection_string)


engine = get_engine()


@st.cache_data(ttl=30)
def query(sql):

    with engine.connect() as connection:
        return pd.read_sql(sql, connection)


if st.button("Refresh data"):
    st.cache_data.clear()



@st.fragment(run_every="10s")

def render_dashboard():

    # ---------------------------------------------------------
    # PIPELINE HEALTH
    # ---------------------------------------------------------

    st.header("Pipeline Health")

    health = query("""
        select
            (select count(*) from public.raw_lab_json)
                as raw_rx_batches,

            (select count(*) from public.raw_lab_csv)
                as raw_labtrak_rows,

            (select count(*) from analytics.deduped_lab_samples)
                as silver_samples,

            (select count(*) from analytics.batch_summary)
                as gold_batches
    """)

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "RX raw batches",
        int(health.loc[0, "raw_rx_batches"])
    )

    col2.metric(
        "LabTrak raw rows",
        int(health.loc[0, "raw_labtrak_rows"])
    )

    col3.metric(
        "Silver samples",
        int(health.loc[0, "silver_samples"])
    )

    col4.metric(
        "Gold batches",
        int(health.loc[0, "gold_batches"])
    )


    # ---------------------------------------------------------
    # CATALYST PERFORMANCE
    # ---------------------------------------------------------

    st.header("Catalyst Performance")

    catalysts = query("""
        select
            catalyst,
            batch_count,
            sample_count,
            avg_yield_pct,
            avg_purity_pct
        from analytics.catalyst_performance
        order by avg_yield_pct desc
    """)

    st.bar_chart(
        catalysts,
        x="catalyst",
        y="avg_yield_pct"
    )

    st.dataframe(
        catalysts,
        use_container_width=True,
        hide_index=True
    )


    # ---------------------------------------------------------
    # TEMPERATURE PERFORMANCE
    # ---------------------------------------------------------

    st.header("Temperature Performance")

    temperature = query("""
        select
            catalyst,
            temperature_range,
            avg_temperature_c,
            batch_count,
            avg_yield_pct,
            avg_purity_pct
        from analytics.temperature_performance
        order by catalyst, avg_temperature_c
    """)

    st.scatter_chart(
        temperature,
        x="avg_temperature_c",
        y="avg_yield_pct",
        color="catalyst",
        size="batch_count"
    )


    # ---------------------------------------------------------
    # WORST PERFORMING BATCHES
    # ---------------------------------------------------------

    st.header("Batch Outliers")

    outliers = query("""
        with overall as (

            select
                avg(avg_yield_pct) as overall_avg_yield

            from analytics.batch_summary

            where avg_yield_pct is not null
        )

        select
            b.instrument,
            b.batch_id,
            b.started_at,
            b.catalyst,
            round(b.temperature_c, 2) as temperature_c,
            b.sample_count,
            round(b.avg_yield_pct, 2) as avg_yield_pct,
            round(
                b.avg_yield_pct - o.overall_avg_yield,
                2
            ) as yield_vs_overall

        from analytics.batch_summary b

        cross join overall o

        where b.avg_yield_pct < o.overall_avg_yield

        order by yield_vs_overall asc

        limit 20
    """)

    st.dataframe(
        outliers,
        use_container_width=True,
        hide_index=True
    )


    # ---------------------------------------------------------
    # DATA QUALITY
    # ---------------------------------------------------------

    st.header("Data Quality")

    quality = query("""
        select
            instrument,
            count(*) as total_samples,

            count(*) filter (
                where invalid_temperature
            ) as invalid_temperature,

            count(*) filter (
                where invalid_yield
            ) as invalid_yield,

            count(*) filter (
                where invalid_purity
            ) as invalid_purity

        from analytics.deduped_lab_samples

        group by instrument

        order by instrument
    """)

    st.dataframe(
        quality,
        use_container_width=True,
        hide_index=True
    )

render_dashboard()