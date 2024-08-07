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
