from prefect import task, get_run_logger
from prefect_snowflake.database import SnowflakeConnector
from snowflake.connector.pandas_tools import write_pandas

import os
import pandas as pd

import plaid
from plaid.api import plaid_api


@task(retries=2)
def create_client() -> plaid_api.PlaidApi:
    logger = get_run_logger()
    logger.debug('create_client()')

    client_id = os.getenv('PLAID_CLIENT_ID')
    secret = os.getenv('PLAID_SECRET')
    version = os.getenv('PLAID_API_VERSION')
    configuration = plaid.Configuration(
        host=plaid.Environment.Production,
        api_key={
            'clientId': client_id,
            'secret': secret,
            'plaidVersion': version
        }
    )

    api_client = plaid.ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)


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
