from prefect import flow, get_run_logger
from prefect_dbt import DbtCoreOperation, DbtCliProfile
from prefect.utilities.filesystem import relative_path_to_current_platform


@flow(log_prints=True)
def dbt_build():
    logger = get_run_logger()

    dbt_cli_profile = DbtCliProfile.load('dbt-cli-profile')

    dbt_init = DbtCoreOperation(
        commands=['dbt build'],
        dbt_cli_profile=dbt_cli_profile,
        overwrite_profiles=True,
        project_dir='./src/dbt'
    )
    dbt_init.run()


if __name__ == '__main__':
    dbt_build()
