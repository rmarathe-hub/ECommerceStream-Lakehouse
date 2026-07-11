"""
CommerceStream marts dashboard — reads COMMERCESTREAM_DB.MARTS only.
Uses DE_PROJECT_WH (auto-resume). Run make snowflake-suspend when finished.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

# Load repo-root .env if present (without printing secrets)
_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
if _ENV_PATH.exists():
    for line in _ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


st.set_page_config(
    page_title="CommerceStream Marts",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
      @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');
      html, body, [class*="css"] {
        font-family: 'IBM Plex Sans', sans-serif;
      }
      .block-container { padding-top: 1.5rem; max-width: 1100px; }
      h1 { font-weight: 600 !important; letter-spacing: -0.02em; }
      [data-testid="stMetricValue"] { font-family: 'IBM Plex Mono', monospace; }
      div[data-testid="stSidebar"] { background: #0f1419; }
      div[data-testid="stSidebar"] * { color: #e7ecf1 !important; }
    </style>
    """,
    unsafe_allow_html=True,
)


def _required_env(*keys: str) -> dict[str, str]:
    missing = [k for k in keys if not os.environ.get(k)]
    if missing:
        st.error(f"Missing env vars: {', '.join(missing)}. Set them in repo-root `.env`.")
        st.stop()
    return {k: os.environ[k] for k in keys}


@st.cache_resource(show_spinner=False)
def get_connection():
    import snowflake.connector

    cfg = _required_env(
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
    )
    return snowflake.connector.connect(
        account=cfg["SNOWFLAKE_ACCOUNT"],
        user=cfg["SNOWFLAKE_USER"],
        password=cfg["SNOWFLAKE_PASSWORD"],
        role=cfg["SNOWFLAKE_ROLE"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "DE_PROJECT_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "COMMERCESTREAM_DB"),
        schema="MARTS",
        client_session_keep_alive=False,
    )


@st.cache_data(ttl=300, show_spinner="Querying Snowflake marts…")
def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(sql, conn)


st.title("CommerceStream")
st.caption(
    "Curated gold marts in Snowflake — 1M cloud-lite demo. "
    "Heavy processing stayed local; warehouse is X-Small with auto-suspend."
)

with st.sidebar:
    st.markdown("### Cost guardrails")
    st.markdown(
        """
        - Warehouse: `DE_PROJECT_WH` (XSMALL)
        - Auto-suspend: 60s
        - Monitor: 3 credits/month
        - Source: `MARTS` only
        """
    )
    st.markdown("---")
    st.markdown(
        "When finished viewing, run:\n\n"
        "```bash\nmake snowflake-suspend\n```"
    )
    if st.button("Clear query cache"):
        run_query.clear()
        st.rerun()

try:
    funnel = run_query(
        """
        SELECT
          session_date,
          total_sessions,
          sessions_with_view,
          sessions_with_cart,
          sessions_with_purchase,
          view_to_cart_rate,
          cart_to_purchase_rate,
          view_to_purchase_rate,
          cart_abandonment_rate,
          abandoned_cart_sessions
        FROM COMMERCESTREAM_DB.MARTS.mart_conversion_funnel
        ORDER BY session_date
        """
    )
    products = run_query(
        """
        SELECT
          product_id,
          category_code,
          brand,
          purchase_count,
          unique_purchasers,
          total_revenue,
          view_to_purchase_rate,
          cart_to_purchase_rate
        FROM COMMERCESTREAM_DB.MARTS.mart_product_performance
        ORDER BY total_revenue DESC NULLS LAST
        LIMIT 25
        """
    )
    session_kpis = run_query(
        """
        SELECT
          COUNT(*) AS session_count,
          SUM(IFF(converted, 1, 0)) AS converted_sessions,
          SUM(session_revenue) AS total_revenue,
          AVG(session_duration_seconds) AS avg_duration_sec
        FROM COMMERCESTREAM_DB.MARTS.mart_sessions
        """
    )
except Exception as exc:  # noqa: BLE001 — surface Snowflake errors in UI
    st.error("Could not query Snowflake marts. Confirm `make dbt-build` succeeded and credentials in `.env`.")
    st.code(str(exc))
    st.stop()

kpi = session_kpis.iloc[0]
conv_rate = (
    float(kpi["CONVERTED_SESSIONS"]) / float(kpi["SESSION_COUNT"])
    if float(kpi["SESSION_COUNT"])
    else 0.0
)

st.subheader("Session overview")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Sessions", f"{int(kpi['SESSION_COUNT']):,}")
c2.metric("Converted", f"{int(kpi['CONVERTED_SESSIONS']):,}")
c3.metric("Conversion rate", f"{conv_rate:.1%}")
c4.metric("Session revenue", f"${float(kpi['TOTAL_REVENUE']):,.0f}")

st.subheader("Conversion funnel by day")
if not funnel.empty:
    funnel = funnel.copy()
    funnel["SESSION_DATE"] = pd.to_datetime(funnel["SESSION_DATE"])
    chart_df = funnel.set_index("SESSION_DATE")[
        ["VIEW_TO_CART_RATE", "CART_TO_PURCHASE_RATE", "VIEW_TO_PURCHASE_RATE"]
    ]
    chart_df.columns = ["View → cart", "Cart → purchase", "View → purchase"]
    st.line_chart(chart_df)
    st.dataframe(
        funnel.rename(
            columns={
                "SESSION_DATE": "Date",
                "TOTAL_SESSIONS": "Sessions",
                "VIEW_TO_PURCHASE_RATE": "View→purchase",
                "CART_ABANDONMENT_RATE": "Cart abandon",
                "ABANDONED_CART_SESSIONS": "Abandoned carts",
            }
        )[
            [
                "Date",
                "Sessions",
                "View→purchase",
                "Cart abandon",
                "Abandoned carts",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Top products by revenue")
if not products.empty:
    st.dataframe(
        products.rename(
            columns={
                "PRODUCT_ID": "Product",
                "CATEGORY_CODE": "Category",
                "BRAND": "Brand",
                "PURCHASE_COUNT": "Purchases",
                "UNIQUE_PURCHASERS": "Buyers",
                "TOTAL_REVENUE": "Revenue",
                "VIEW_TO_PURCHASE_RATE": "View→purchase",
            }
        )[
            [
                "Product",
                "Category",
                "Brand",
                "Purchases",
                "Buyers",
                "Revenue",
                "View→purchase",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

st.markdown("---")
st.caption(
    "Data path: local Spark gold → S3 → Snowflake STAGING → dbt MARTS. "
    "Remember: `make snowflake-suspend` when done."
)
