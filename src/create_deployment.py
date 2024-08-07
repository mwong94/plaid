from prefect import flow
from prefect.client.schemas.schedules import CronSchedule

SOURCE_REPO='https://github.com/mwong94/plaid.git'

if __name__ == '__main__':
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint='src/flows/plaid_institutions.py:get_institutions',
    ).deploy(
        name='plaid-institutions',
        work_pool_name='default-work-pool',
        schedule=CronSchedule(
            cron='0 0 1 * *',
            timezone='US/Pacific'
        )
    )
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint='src/flows/plaid_accounts.py:get_accounts',
    ).deploy(
        name='plaid-accounts',
        work_pool_name='default-work-pool',
        schedule=CronSchedule(
            cron='0 0 * * *',
            timezone='US/Pacific'
        )
    )
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint='src/flows/plaid_transactions.py:get_transactions',
    ).deploy(
        name='plaid-transactions',
        work_pool_name='default-work-pool',
        schedule=CronSchedule(
            cron='2 0 * * *',
            timezone='US/Pacific'
        )
    )
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint='src/flows/plaid_items.py:add_item',
    ).deploy(
        name='plaid-items',
        work_pool_name='default-work-pool'
    )
