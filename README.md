# streamlit-snowflake-usage
Welcome to the Snowflake Credit Usage Forecast App that utilizes Streamlit, Snowflake-Connector and Facebook Prophet packages to forecast the credit usage of Snowflake. 
By analysing the historical Snowflake credit usage, this app provides a forecast of the credit usage for the next six months and estimated costs.

Packages used:
- [Streamlit](https://streamlit.io)
- [Snowflake Connector](https://docs.snowflake.com/developer-guide/python-connector/python-connector)
- [Facebook Prophet](https://facebook.github.io/prophet/)
- [Plotly](https://plotly.com)
- Pandas

## How to run the app
1. Clone the repository
2. Install the required libraries
3. Run the app using the command `streamlit run main.py`

## How to use the app
1. Enter the Snowflake account name
2. Enter the Snowflake user name
3. Enter the Snowflake password
4. Enter the Snowflake role name that has access to `snowflake.account_usage` tables
5. Execute 🚀
