from prefect import flow, task, get_run_logger
from prefect.variables import Variable
from prefect.artifacts import create_table_artifact, create_markdown_artifact
from prefect_snowflake.database import SnowflakeConnector
from snowflake.connector.pandas_tools import write_pandas, pd_writer

import os
import pandas as pd
from datetime import datetime

from utils import RateLimiter, cast_to_string

import plaid
from plaid.api import plaid_api
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.accounts_get_request_options import AccountsGetRequestOptions
from plaid.model.country_code import CountryCode
from plaid.model.institutions_get_request import InstitutionsGetRequest
from plaid.model.institutions_get_request_options import InstitutionsGetRequestOptions
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.transactions_sync_request_options import TransactionsSyncRequestOptions


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


@task(retries=5)
def _get_institutions(client = plaid_api.PlaidApi, debug: bool = False) -> pd.DataFrame:
    logger = get_run_logger()
    logger.debug('_get_institutions()')

    institutions = []
    offset = 0
    count = 500
    total = 1
    while len(institutions) < total:
        with RateLimiter(2):
            request = InstitutionsGetRequest(
                count=count,
                offset=offset,
                country_codes=[CountryCode('US')]
            )
            response = client.institutions_get(request)
            total = response['total']
            institutions += response.to_dict()['institutions']
            count = min(total - len(institutions), 500)

            logger.debug(f'{len(institutions)} / {total} (offset: {offset})')
            offset = len(institutions)
        if debug:
            break

    df = pd.DataFrame(institutions)

    return df


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


@flow(name='PlaidInstitutions')
def get_institutions(debug: bool = False, delete: bool = False) -> None:
    logger = get_run_logger()
    logger.debug('get_institutions()')

    client = create_client()

    df = _get_institutions(client, debug)
    df['loaded_at'] = datetime.utcnow()
    for col in df.columns:
        df[col] = df[col].apply(cast_to_string)
    df = df[['institution_id', 'name', 'products', 'country_codes', 'routing_numbers', 'oauth', 'loaded_at']]
    
    upload_df(df, 'raw', 'institutions', True)

    create_markdown_artifact(
        key='institutions',
        markdown=df.sample(10).to_markdown(),
        description='Plaid institutions sample'
    )


if __name__ == '__main__':
    get_institutions(debug=True, delete=True)
