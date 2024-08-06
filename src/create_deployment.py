from prefect import flow

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO='https://github.com/mwong94/plaid.git'

if __name__ == '__main__':
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint='src/flows/my_gh_workflow.py:repo_info', # Specific flow to run
    ).deploy(
        name='my-first-deployment',
        work_pool_name='default-work-pool',
        cron='* * * * *',
    )