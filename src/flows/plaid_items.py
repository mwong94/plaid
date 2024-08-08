from prefect import flow, get_run_logger
from prefect_snowflake.database import SnowflakeConnector
from textwrap import dedent

from utils import create_client

from plaid.model.item_remove_request import ItemRemoveRequest

@flow
def add_item(item_id: str, access_token: str, institution_id: str, institution_name: str) -> None:
    logger = get_run_logger()
    logger.debug('add_item()')
    with SnowflakeConnector.load('sf1').get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(dedent('''
                insert into items(item_id, access_token, institution_id, institution_name)
                    values
                    (%(item_id)s, %(access_token)s, %(institution_id)s, %(institution_name)s);
                '''), {
                    'item_id': item_id,
                    'access_token': access_token,
                    'institution_id': institution_id,
                    'institution_name': institution_name
                })


@flow
def remove_item(access_token: str) -> None:
    logger = get_run_logger()
    logger.debug('remove_item()')
    
    client = create_client()

    request = ItemRemoveRequest(
        access_token=access_token
    )
    response = client.item_remove(request)