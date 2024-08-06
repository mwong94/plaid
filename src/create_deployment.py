from prefect import flow
from prefect.client.schemas.schedules import CronSchedule

SOURCE_REPO='https://github.com/mwong94/plaid.git'

if __name__ == '__main__':
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint='src/flows/plaid.py:get_institutions',
    ).deploy(
        name='plaid-deployment',
        work_pool_name='default-work-pool',
        schedule=CronSchedule(
            cron='0 0 * * 0', # crontab.guru: “At 00:00 on Sunday.”
            timezone='US/Pacific'
        )
    )
