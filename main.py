import streamlit as st
import plotly.express as px
from db import SnowflakeConnection
from forecast import create_forecast
import pandas as pd
import datetime


@st.cache_data
def get_data(sql, params=None):
    df = conn.query_data_warehouse(sql, params)
    return df


# find end of month date
def end_of_month_date(date):
    return date + datetime.timedelta(days=31 - date.day)


# find days between two dates
def days_between(d1, d2):
    return abs((d2 - d1).days)


# current date plus 6 months
start_date = datetime.date.today()
end_date = end_of_month_date(start_date + datetime.timedelta(days=180))

forecast_periods = days_between(start_date, end_date)
connection_closed = True

st.title("Snowflake Credit Usage Forecast")

st.write(
    "Predicting Snowflake credit usage for the next 6 months using the Streamlit, [Prophet](https://facebook.github.io/prophet/) and Plotly 🔥. "
    "This app uses Snowflake's past credit usage data to forecast its future usage. "
    "A great way to plan ahead and manage data warehousing costs by viewing predicted usage and cost by day and month.")

st.subheader("Snowflake Connection Details")
st.write("Enter your Snowflake credentials and execute.")
with st.expander("Enter your Snowflake credentials 🙈"):
    st.write("Enter your Snowflake credentials. Ensure that user and role used does have read access to "
             "snowflake information_schema.tables. "
             "For the ACCOUNT parameter, use your account identifier. ")
    st.write(
        "Note that the account identifier does not include the `snowflakecomputing.com suffix`. "
        "Example: `abc1234.us-east-1`")
    # get credentials form
    col1, col2 = st.columns(2)
    with col1:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
    with col2:
        account = st.text_input("Account")
        role = st.text_input("Role")
        credit_price = st.number_input("Price per credit", value=2.00)

submit_button = st.button(label='Execute 🚀')

if submit_button:
    if not username or not password or not account or not role:
        st.error('Oops, looks like we are missing some snowflake connection details.', icon='🤭')
    else:
        conn = SnowflakeConnection(username, password, account, role)
        try:
            connection_closed = conn.connect()
        except Exception as e:
            st.error(e, icon='☹️')

    if not connection_closed:
        st.success('Connection successful!', icon='🎉')
        st.divider()
        # credit usage over time
        credit_usage_over_time = get_data(
            """select 
            start_time::date as "ds", 
            sum(credits_used::float) as "y" 
            from snowflake.account_usage.warehouse_metering_history 
            group by 1 order by 2 desc
            limit 365;""")

        # set ds to datetime
        credit_usage_over_time['ds'] = pd.to_datetime(credit_usage_over_time['ds'])
        # forecast
        forecast_df = create_forecast(credit_usage_over_time, forecast_periods)
        # set ds to datetime
        forecast_df['ds'] = pd.to_datetime(forecast_df['ds'])
        # left join credit usage over time and forecast on ds
        forecast_df = pd.merge(forecast_df, credit_usage_over_time, how='left', on='ds')
        # forecast df columns ds, y and yhat only
        forecast_df = forecast_df[['ds', 'y', 'yhat']]
        # rename yhat to forecast
        forecast_df = forecast_df.rename(columns={'yhat': 'forecast'})
        # rename y to actual
        forecast_df = forecast_df.rename(columns={'y': 'actual'})
        # set ds to index
        forecast_df = forecast_df.set_index('ds')
        # aggregate ds to weekly
        forecast_df_m = forecast_df.resample('M').sum()

        # section 1
        st.subheader("Credit Usage by Day")
        st.write(
            "Line chart showing the credit usage by day, limited to the previous 365 days and forecast for the next 6 months.")
        # plot
        fig_credit_usage_day = px.line(forecast_df, title="Credit Usage and Forecast by Day")
        st.plotly_chart(fig_credit_usage_day, use_container_width=True)

        # section 2
        st.subheader("Credit Usage by Month")
        st.write(
            "Bar chart showing the credit usage by month including aggregated forecast usage for the next 6 months.")
        # plot
        fig_credit_usage_month = px.bar(forecast_df_m, barmode="group", title="Credit Usage and Forecast by Month")
        fig_credit_usage_month.update_xaxes(
            tickangle=-45,
            tickvals=forecast_df_m.index,
            ticktext=forecast_df_m.index.strftime('%b-%Y'))
        st.plotly_chart(fig_credit_usage_month, use_container_width=True)

        # section 3
        st.subheader("Credit Cost by Month Table")
        st.write(
            "Table of monthly credit usage with forecast predictions, include calculated cost by credit price input multiplied by credit usage.")
        # add column to forecast_df_m for credit price x forecast
        forecast_df_m['Cost Actual*'] = forecast_df_m['actual'] * credit_price
        forecast_df_m['Cost Forecast'] = forecast_df_m['forecast'] * credit_price
        # format index to month
        forecast_df_m.index = forecast_df_m.index.strftime('%b-%Y')
        # rename columns
        forecast_df_m = forecast_df_m.rename(columns={'actual': 'Credits Actual', 'forecast': 'Credits Forecast'})
        st.table(forecast_df_m)
        st.text("Note: *actual cost is based on the actual credit usage and the credit price entered.")
