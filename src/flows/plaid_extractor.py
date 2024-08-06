from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact, create_markdown_artifact

import os
import json
import pandas as pd
from datetime import datetime

from utils import RateLimiter
from snowflake_client import SnowflakeClient

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
def _get_institutions(client = plaid_api.PlaidApi) -> pd.DataFrame:
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
        break

    df = pd.DataFrame(institutions)

    logger.debug(json.dumps(institutions[:2]))
    logger.debug(len(institutions))

    return df


@task(retries=2)
def upload_df(df: pd.DataFrame, schema: str, table: str, if_exists: str = 'append') -> None:
    snowflake_client = SnowflakeClient(
        os.getenv('SNOWFLAKE_ACCOUNT'),
        os.getenv('SNOWFLAKE_USERNAME'),
        os.getenv('SNOWFLAKE_PASSWORD'),
        os.getenv('SNOWFLAKE_DATABASE'),
        os.getenv('SNOWFLAKE_WAREHOUSE'),
        logger=get_run_logger()
    )
    snowflake_client.upload_df(df, 'raw', 'institutions', 'append')


@flow
def get_institutions() -> pd.DataFrame:
    logger = get_run_logger()
    logger.debug('get_institutions()')

    client = create_client()

    df = _get_institutions(client)
    df['loaded_at'] = datetime.utcnow()

    upload_df(df, 'raw', 'institutions', 'append')

    create_markdown_artifact(
        key='institution_md',
        markdown=df.sample(10).to_markdown(),
        description='Plaid institutions sample'
    )

    return df


if __name__ == '__main__':
    get_institutions()
