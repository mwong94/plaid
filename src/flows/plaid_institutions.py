from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

import pandas as pd
from datetime import datetime

from utils import RateLimiter, cast_to_string
from plaid_tasks import create_client, upload_df

from plaid.api import plaid_api
from plaid.model.country_code import CountryCode
from plaid.model.institutions_get_request import InstitutionsGetRequest
from plaid.model.institutions_get_request_options import InstitutionsGetRequestOptions


@task(retries=5)
def _get_institutions(client: plaid_api.PlaidApi, debug: bool = False) -> pd.DataFrame:
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
    for col in df.columns:
        df[col] = df[col].apply(cast_to_string)
    df = df[['institution_id', 'name', 'products', 'country_codes', 'routing_numbers', 'oauth']]

    return df


@flow
def get_institutions(debug: bool = False, delete: bool = False) -> None:
    # debug logging
    logger = get_run_logger()
    logger.debug('get_institutions()')

    # run tasks
    client = create_client()
    df = _get_institutions(client, debug, delete)
    upload_df(df, 'raw', 'institutions')

    # create artifacts for UI
    create_markdown_artifact(
        key='institutions',
        markdown=df.sample(10).to_markdown(),
        description='Plaid institutions sample'
    )


if __name__ == '__main__':
    get_institutions(debug=True, delete=True)
