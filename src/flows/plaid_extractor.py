from prefect import flow, task, get_run_logger

import os
import json
import pandas as pd
from utils import RateLimiter

from plaid.api import plaid_api
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.accounts_get_request_options import AccountsGetRequestOptions
from plaid.model.country_code import CountryCode
from plaid.model.institutions_get_request import InstitutionsGetRequest
from plaid.model.institutions_get_request_options import InstitutionsGetRequestOptions
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.transactions_sync_request_options import TransactionsSyncRequestOptions

@task(retries=2)
def create_client() -> plaid_api.PlaidAPI:
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


@task
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

    df = pd.DataFrame(institutions)

    logger.debug(json.dumps(institutions[:2]))
    logger.debug(len(institutions))

    return df

@flow
def get_institutions() -> pd.DataFrame:
    logger = get_run_logger()
    logger.debug('get_institutions()')

    client = create_client()
    df = _get_institutions(client)
    return df
