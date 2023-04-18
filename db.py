import snowflake.connector


class SnowflakeConnection:
    def __init__(self, username, password, account, role):
        self.username = username
        self.password = password
        self.account = account
        self.role = role
        self.conn = None
        self.cur = None

    def connect(self):
        # connection to snowflake
        self.conn = snowflake.connector.connect(user=self.username,
                                                password=self.password,
                                                account=self.account,
                                                role=self.role
                                                )
        self.cur = self.conn.cursor()
        # print connection status
        connection_closed = self.conn.is_closed()
        return connection_closed

    def query_data_warehouse(self, sql: str, parameters=None) -> any:
        """
        Executes snowflake sql config and returns result as data as dataframe.
        Example of parameters
        {"order_date": '2022-07-13', "customer_region": 1} can be used to reference variable in sql config %(order_date)s
         and %(customer_region)s.
        :param sql: sql config to be executed
        :param parameters: named parameters used in the sql config (defaulted as None)
        :return: dataframe
        """
        if parameters is None:
            parameters = {}
        query = sql
        self.cur.execute(query, params=parameters)
        data = self.cur.fetch_pandas_all()
        return data
