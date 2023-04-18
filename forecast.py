import pandas as pd
from datetime import datetime
from prophet import Prophet


def forecast_credit_usage(df: pd.DataFrame, forecast_periods: int):
    """Forecast GMV for each region and each day
    Args:
        df: input dataframe
        forecast_periods: number of days to forecast
    Returns:
        forecast_df: dataframe of forecast
    """
    # create a dataframe to store the forecast
    forecast_df = pd.DataFrame()
    # create model object
    m = Prophet()
    # fit model to data
    m.fit(df)
    # create future dates dataframe
    future = m.make_future_dataframe(periods=forecast_periods)
    # predict future dates
    forecast = m.predict(future)
    # append to forecast dataframe
    forecast_df = forecast_df.append(forecast)
    return forecast_df[['ds', 'yhat', 'trend', 'yhat_lower', 'yhat_upper']]
