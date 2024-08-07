from prefect import task, get_run_logger
from prefect_snowflake.database import SnowflakeConnector
from snowflake.connector.pandas_tools import write_pandas

import pandas as pd


@task(retries=2)
def upload_df(df: pd.DataFrame, schema: str, table: str, delete: bool = False) -> None:
    with SnowflakeConnector.load('sf1').get_connection() as conn:
        if delete:
            with conn.cursor() as cur:
                cur.execute(f'delete from {schema}.{table}')
        write_pandas(
            conn,
            df,
            table,
            database='PLAID',
            schema=schema,
            quote_identifiers=False
        )
