from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect_snowflake.database import SnowflakeConnector
from textwrap import dedent

from typing import Tuple
import pandas as pd
from datetime import datetime
import json

from utils import DateTimeEncoder
from plaid_tasks import create_client, upload_df, get_items, update_item_cursors
from dbt_build import dbt_build

from plaid.api import plaid_api
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.transactions_sync_request_options import TransactionsSyncRequestOptions


def get_latest_cursor_or_none(item_id: str) -> str:
    with SnowflakeConnector.load('sf1').get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(dedent(f'''
                select cursor
                from items
                where item_id = '{item_id}'
            '''.strip()))
            df = cur.fetch_pandas_all()
    cursor = df.iloc[0].CURSOR if len(df) >= 1 else ''
    return cursor


@task
def _get_transactions(
        client: plaid_api.PlaidApi,
        items: pd.DataFrame,
        backfill: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger = get_run_logger()
    logger.debug('_get_transactions()')

    rows = []
    cursor_rows = []
    for _, row in items.iterrows():
        institution_id = row['INSTITUTION_ID']
        item_id = row['ITEM_ID']
        access_token = row['ACCESS_TOKEN']
        if backfill:
            cursor = ''
        else:
            cursor = get_latest_cursor_or_none(item_id)
        logger.debug(f'cursor: {cursor}')

        transactions = []
        has_more = True

        while has_more:
            logger.info(f'{institution_id}, {cursor}')
            request = TransactionsSyncRequest(
                access_token=access_token,
                cursor=cursor,
                options=TransactionsSyncRequestOptions(days_requested=730)
            )
            response = client.transactions_sync(request).to_dict()

            for transaction_type in ['added', 'modified', 'removed']:
                for tmp in response[transaction_type]:
                    transactions.append({
                        'transaction_type': transaction_type,
                        'jsondata': json.dumps(tmp, cls=DateTimeEncoder)
                    })

            has_more = response['has_more']
            cursor = response['next_cursor']

            logger.info(f'\ttransactions: {len(transactions)}')
        logger.info(f'adding cursor to output: {item_id}, {cursor}')

        cursor_row = {
            'item_id': row['ITEM_ID'],
            'cursor': cursor
        }
        cursor_rows.append(cursor_row)
    
    return pd.DataFrame(transactions), pd.DataFrame(cursor_rows)


@flow
def get_transactions(backfill: bool = False, delete: bool = False, dbt_build: bool = True) -> None:
    # debug logging
    logger = get_run_logger()
    logger.debug('get_transactions()')

    # run tasks
    client = create_client()
    items = get_items()
    transactions_df, cursor_df = _get_transactions(client, items, backfill=backfill)
    upload_df(transactions_df, 'raw', 'transactions_json', delete)
    update_item_cursors(cursor_df)

    # create artifacts for UI
    if len(transactions_df) > 0:
        create_markdown_artifact(
            key='transactions',
            markdown=transactions_df.head().to_markdown(),
            description='Plaid transactions sample'
        )
        create_markdown_artifact(
            key='transactions-len',
            markdown=str(len(transactions_df)),
            description='Plaid transactions DataFrame length'
        )

    if dbt_build:
        dbt_build()


if __name__ == '__main__':
    get_transactions(backfill=False, delete=False)
