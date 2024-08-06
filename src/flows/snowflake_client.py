from sqlalchemy import create_engine
import pandas as pd
import logging

class SnowflakeClient:
    def __init__(
        self, account: str, username: str, password: str,
        database: str, warehouse: str, logger: logging.Logger
    ):
        self.logger = logger
        
        conn_str = f'snowflake://{username}:{password}@{account}/{database}?warehouse={warehouse}'
        self.logger.debug(conn_str)
        self.engine = create_engine(
            conn_str
        )
    
    def upload_df(self, df: pd.DataFrame, schema: str, table: str, if_exists: str = 'append') -> None:
        df.to_sql(
            name=table,
            con=self.engine,
            schema=schema,
            if_exists=if_exists
        )
