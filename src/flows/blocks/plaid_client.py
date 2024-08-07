from prefect.blocks.core import Block
from pydantic import SecretStr

import plaid
from plaid.api import plaid_api


class PlaidClient(Block):
    client_id: SecretStr
    secret: SecretStr
    version: str = '2020-09-14'


    def get_client(self):
        configuration = plaid.Configuration(
            host=plaid.Environment.Production,
            api_key={
                'clientId': self.client_id,
                'secret': self.secret,
                'plaidVersion': self.version
            }
        )

        api_client = plaid.ApiClient(configuration)
        return plaid_api.PlaidApi(api_client)
