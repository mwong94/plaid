from prefect import flow
from prefect.client.schemas.schedules import CronSchedule

SOURCE_REPO='https://github.com/mwong94/plaid.git'

if __name__ == '__main__':
    # PLAID INSTITUTIONS
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint='src/flows/plaid_institutions.py:get_institutions',
    ).deploy(
        name='institutions',
        work_pool_name='default-work-pool',
        schedule=CronSchedule(
            cron='0 0 1 * *',
            timezone='US/Pacific'
        )
    )

    # PLAID ACCOUNTS
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint='src/flows/plaid_accounts.py:get_accounts',
    ).deploy(
        name='accounts',
        work_pool_name='default-work-pool',
        schedule=CronSchedule(
            cron='0 0 * * *',
            timezone='US/Pacific'
        )
    )

    # PLAID TRANSACTIONS
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint='src/flows/plaid_transactions.py:get_transactions',
    ).deploy(
        name='transactions',
        work_pool_name='default-work-pool',
        schedule=CronSchedule(
            cron='2 0 * * *',
            timezone='US/Pacific'
        )
    )

    # DBT BUILD MODELS
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint='src/flows/dbt_build.py:dbt_build',
    ).deploy(
        name='dbt-build',
        work_pool_name='default-work-pool',
        # schedule=CronSchedule(
        #     cron='15 0 * * *',
        #     timezone='US/Pacific'
        # )
    )

    # PLAID ITEMS - no schedule, run manually through UI
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint='src/flows/plaid_items.py:add_item',
    ).deploy(
        name='add-item',
        work_pool_name='default-work-pool'
    )
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint='src/flows/plaid_items.py:remove_item',
    ).deploy(
        name='remove-item',
        work_pool_name='default-work-pool'
    )
