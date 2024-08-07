from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from textwrap import dedent

import pandas as pd
from datetime import datetime

from plaid_tasks import create_client, upload_df, get_items

from plaid.api import plaid_api
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.accounts_get_request_options import AccountsGetRequestOptions


@task(retries=5)
def _get_accounts(client: plaid_api.PlaidApi, items: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in items.iterrows():
        access_token = row['ACCESS_TOKEN']

        ag_request = AccountsGetRequest(
            access_token=access_token
        )
        response = client.accounts_get(ag_request).to_dict()
        accounts += response['accounts']
    
        for account in accounts:
            row = {
                'account_id': account['account_id'],
                'balance_available': account['balances']['available'],
                'balance_current': account['balances']['current'],
                'balance_limit': account['balances']['limit'],
                'balance_iso_currency_code': account['balances']['iso_currency_code'],
                'balance_unofficial_currency_code': account['balances']['unofficial_currency_code'],
                'mask': account['mask'],
                'name': account['name'],
                'official_name': account['official_name'],
                'persistent_account_id': account['persistent_account_id'] if 'persistent_account_id' in account.keys() else None,
                'type': account['type'],
                'subtype': account['subtype'],
                'institution_id': row['INSTITUTION_ID']
            }
            rows.append(row)
    df = pd.DataFrame(rows)

    return df


@flow
def get_accounts(delete: bool = False) -> None:
    # debug logging
    logger = get_run_logger()
    logger.debug('get_accounts()')

    # run tasks
    client = create_client()
    items = get_items()
    df = _get_accounts(client, items)
    upload_df(df, 'raw', 'accounts', delete)


if __name__ == '__main__':
    get_accounts()
